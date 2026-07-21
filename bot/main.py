import requests
import pandas as pd
import datetime
import pytz
import os
import time
import math
import threading
from collections import defaultdict
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import xml.etree.ElementTree as ET

# ==================== 🔑 1. Telegram 設定 ====================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
SCAN_INTERVAL_MINUTES = 30

BASE_URL = "https://www.okx.com"
_tick_size_map: dict = {}   # inst_id → tickSz float，由 get_all_okx_swap_assets() 填充
_vol_cache:     dict = {}   # instId → volCcy24h（上次成功的快取，API 失敗時備援）

if not TELEGRAM_BOT_TOKEN:
    raise ValueError("❌ 未設置 TELEGRAM_BOT_TOKEN 環境變數，請在 Secrets 中添加。")
if not TELEGRAM_CHAT_ID:
    raise ValueError("❌ 未設置 TELEGRAM_CHAT_ID 環境變數，請在 Secrets 中添加。")

# ==================== 🗂️ 2. OKX 官方全量資產動態抓取 ====================
def get_all_okx_swap_assets():
    """
    回傳 (assets列表, {instId: max_leverage} 字典)。
    
    硬性過濾：24h 成交額 ≥ 50,000,000 USDT
    理由：低流動性山寨幣合約深度差、插針頻繁、實際難以成交。
    過濾後幣種數量約減少 50%，但留下來的全是可執行的主流品種。
    """
    url = f"{BASE_URL}/api/v5/public/instruments?instType=SWAP"
    try:
        res = requests.get(url, timeout=5).json()
        if res.get('code') == '0':
            assets = []
            leverage_map = {}
            for item in res['data']:
                if item['instId'].endswith('-USDT-SWAP'):
                    assets.append(item['instId'])
                    try:
                        leverage_map[item['instId']] = int(item.get('lever', 20))
                    except:
                        leverage_map[item['instId']] = 20
                    try:
                        _tick_size_map[item['instId']] = float(item['tickSz'])
                    except Exception:
                        pass

            # ── 24h 成交額硬性過濾（流動性門檻）──
            # OKX market/tickers 一次返回全部合約，效率最高（無需循環請求）。
            # 注意：volCcy24h 對 USDT-SWAP 是「基礎幣數量」而非 USDT 金額。
            # 必須乘以 last 價格才能得到真實 USDT 成交額。
            # 例：ATH volCcy24h=482M ATH × last=0.00462 = 僅 2.2M USDT（遠低於門檻）。
            _MIN_VOL_USDT = 50_000_000   # 5000 萬 USDT
            try:
                ticker_res = requests.get(
                    f"{BASE_URL}/api/v5/market/tickers?instType=SWAP", timeout=5
                ).json()
                if ticker_res.get('code') == '0':
                    _vol_map = {}
                    for t in ticker_res['data']:
                        if not t['instId'].endswith('-USDT-SWAP'):
                            continue
                        vol_base  = float(t.get('volCcy24h', 0))   # 基礎幣數量
                        last_px   = float(t.get('last', 0))         # 現價（USDT）
                        vol_usdt  = vol_base * last_px              # 換算成 USDT 成交額
                        _vol_map[t['instId']] = vol_usdt
                    _vol_cache.update(_vol_map)   # 更新全域快取，供 API 失敗時備援
                    before = len(assets)
                    assets       = [a for a in assets if _vol_map.get(a, 0) >= _MIN_VOL_USDT]
                    leverage_map = {k: v for k, v in leverage_map.items() if k in set(assets)}
                    print(f"   📊 成交額過濾（≥50M USDT）：{before} → {len(assets)} 支（剔除 {before - len(assets)} 支低流動性山寨）")
                else:
                    print(f"⚠️ 成交額 API 回傳錯誤碼 {ticker_res.get('code')}，嘗試使用快取過濾")
                    raise RuntimeError("API 非 0 碼")
            except Exception as _vol_err:
                # API 失敗 → 優先用快取；無快取時保守只留主流幣
                if _vol_cache:
                    before = len(assets)
                    assets       = [a for a in assets if _vol_cache.get(a, 0) >= _MIN_VOL_USDT]
                    leverage_map = {k: v for k, v in leverage_map.items() if k in set(assets)}
                    print(f"⚠️ 成交額 API 失敗（{_vol_err}），使用快取過濾：{before} → {len(assets)} 支")
                else:
                    # 完全無快取（第一次啟動即失敗）→ 只保留已知主流幣防止掃山寨
                    _SAFE_FALLBACK = {
                        "BTC-USDT-SWAP","ETH-USDT-SWAP","SOL-USDT-SWAP","BNB-USDT-SWAP",
                        "XRP-USDT-SWAP","DOGE-USDT-SWAP","ADA-USDT-SWAP","AVAX-USDT-SWAP",
                        "DOT-USDT-SWAP","LINK-USDT-SWAP","UNI-USDT-SWAP","MATIC-USDT-SWAP",
                        "LTC-USDT-SWAP","TRX-USDT-SWAP","ATOM-USDT-SWAP","TON-USDT-SWAP",
                        "ARB-USDT-SWAP","OP-USDT-SWAP","SUI-USDT-SWAP","APT-USDT-SWAP",
                    }
                    before = len(assets)
                    assets       = [a for a in assets if a in _SAFE_FALLBACK]
                    leverage_map = {k: v for k, v in leverage_map.items() if k in set(assets)}
                    print(f"⚠️ 成交額 API 失敗且無快取，使用靜態主流幣白名單：{before} → {len(assets)} 支")

            print(f"📡 成功獲取全網合約資產庫，共計: {len(assets)} 支幣種")
            return assets, leverage_map
    except Exception as e:
        print(f"⚠️ 動態獲取資產庫失敗: {e}")
    fallback = ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "TON-USDT-SWAP", "ARB-USDT-SWAP"]
    return fallback, {a: 100 for a in fallback}

# ==================== 📡 3. 主力動向：資金費率 + 多空持倉比 ====================
def get_market_sentiment(asset):
    """
    獲取 OKX 主力資金費率 + 多空持倉比（主力動向指標）
    - funding_rate > 0：多方付費，市場偏多；< 0：空方付費，市場偏空
    - ls_ratio > 1：多方持倉多於空方；< 1：空方較多
    兩個 API 並行發出，節省約 200ms。
    """
    _res = {'fr': 0.0, 'ls': 1.0}

    def _fetch_fr():
        try:
            fr_url = f"{BASE_URL}/api/v5/public/funding-rate?instId={asset}"
            fr_res = requests.get(fr_url, timeout=2.0).json()
            if fr_res.get('code') == '0' and fr_res.get('data'):
                _res['fr'] = float(fr_res['data'][0]['fundingRate'])
        except:
            pass

    def _fetch_ls():
        try:
            ccy = asset.split('-')[0]   # ETH-USDT-SWAP → ETH；ETH → ETH
            ls_url = (f"{BASE_URL}/api/v5/rubik/stat/contracts/"
                      f"long-short-account-ratio?ccy={ccy}&period=5m")
            ls_res = requests.get(ls_url, timeout=2.0).json()
            if ls_res.get('code') == '0' and ls_res.get('data'):
                _res['ls'] = float(ls_res['data'][0][1])
        except:
            pass

    _t1 = threading.Thread(target=_fetch_fr, daemon=True)
    _t2 = threading.Thread(target=_fetch_ls, daemon=True)
    _t1.start(); _t2.start()
    _t1.join(timeout=3.0); _t2.join(timeout=3.0)

    return _res['fr'], _res['ls']

def build_sentiment_note(direction, funding_rate, ls_ratio):
    """根據主力動向生成說明文字與評分加減"""
    fr_pct = funding_rate * 100
    bonus = 0
    if direction == "多":
        if funding_rate > 0.0001 and ls_ratio >= 1.05:
            note = f"📈 主力偏多（費率+{fr_pct:.4f}%，多空比{ls_ratio:.2f}）✅ 方向確認"
            bonus = 12
        elif funding_rate < -0.0001 and ls_ratio < 0.95:
            note = f"⚠️ 主力偏空（費率{fr_pct:.4f}%，多空比{ls_ratio:.2f}）🔻 逆向風險"
            bonus = -15
        else:
            note = f"🔄 主力中性（費率{fr_pct:.4f}%，多空比{ls_ratio:.2f}）"
            bonus = 0
    else:
        if funding_rate < -0.0001 and ls_ratio <= 0.95:
            note = f"📉 主力偏空（費率{fr_pct:.4f}%，多空比{ls_ratio:.2f}）✅ 方向確認"
            bonus = 12
        elif funding_rate > 0.0001 and ls_ratio > 1.05:
            note = f"⚠️ 主力偏多（費率+{fr_pct:.4f}%，多空比{ls_ratio:.2f}）🔻 逆向風險"
            bonus = -15
        else:
            note = f"🔄 主力中性（費率{fr_pct:.4f}%，多空比{ls_ratio:.2f}）"
            bonus = 0
    return note, bonus

# ==================== ⚙️ 4. 市場環境指標（掃描前各取一次）====================
def get_btc_trend():
    """BTC 1H MA8 vs EMA89，判斷大盤方向（保留供持倉監控等非掃描路徑使用）"""
    try:
        url = f"{BASE_URL}/api/v5/market/candles?instId=BTC-USDT-SWAP&bar=1H&limit=100"
        res = requests.get(url, timeout=4).json()
        if res.get('code') == '0' and len(res['data']) >= 90:
            df = pd.DataFrame(res['data'], columns=['ts','open','high','low','close','vol','volCcy','volCcyQuote','state'])
            df['close'] = df['close'].astype(float)
            df = df.iloc[::-1].reset_index(drop=True)
            df['MA8']  = df['close'].rolling(8).mean()
            df['EMA89']= df['close'].ewm(span=89, adjust=False).mean()
            last = df.iloc[-1]
            return "bull" if last['MA8'] > last['EMA89'] else "bear"
    except:
        pass
    return "neutral"


def _fetch_one_trend(coin: str) -> str:
    """取單一幣種 1H MA8/EMA89 趨勢，回傳 'bull' / 'bear' / 'neutral'"""
    try:
        url = f"{BASE_URL}/api/v5/market/candles?instId={coin}-USDT-SWAP&bar=1H&limit=100"
        res = requests.get(url, timeout=4).json()
        if res.get('code') == '0' and len(res['data']) >= 90:
            closes = [float(r[4]) for r in reversed(res['data'])]
            s = pd.Series(closes)
            ma8  = s.rolling(8).mean().iloc[-1]
            k    = 2 / 90
            ema  = closes[0]
            for c in closes[1:]:
                ema = c * k + ema * (1 - k)
            return "bull" if ma8 > ema else "bear"
    except:
        pass
    return "neutral"


def get_ecosystem_ref_trends() -> dict[str, str]:
    """
    並行取回所有生態鏈參考幣的 1H 趨勢，供掃描器按幣種查表使用。
    回傳範例：{"BTC": "bull", "ETH": "bear", "SOL": "bull", "BNB": "bull", "AVAX": "neutral", ...}
    """
    ref_coins = {"BTC", "ETH", "SOL", "BNB", "AVAX", "SUI", "NEAR", "TON"}
    with ThreadPoolExecutor(max_workers=len(ref_coins)) as ex:
        futures = {ex.submit(_fetch_one_trend, coin): coin for coin in ref_coins}
        results = {}
        for f in as_completed(futures):
            coin = futures[f]
            try:
                results[coin] = f.result(timeout=6)
            except Exception:
                results[coin] = "neutral"
    return results

def detect_market_regime() -> str:
    """
    偵測當前市場階段：趨勢市 or 震盪市。

    判斷依據：BTC 4H ADX（方向性強弱最直接的指標）
      ADX ≥ 22 → 趨勢市（突破回踩 / 趨勢順勢策略命中率最高）
      ADX < 22 → 震盪市（SMC/ICT 流動性獵取策略效果最好）

    回傳：'trending' 或 'ranging'
    若 API 失敗則回傳 'trending'（保守，不改變預設行為）。
    """
    try:
        url = f"{BASE_URL}/api/v5/market/candles?instId=BTC-USDT-SWAP&bar=4H&limit=30"
        res = requests.get(url, timeout=3).json()
        if res.get('code') != '0' or len(res['data']) < 20:
            return 'trending'
        rows    = list(reversed(res['data']))   # 由舊到新
        closes  = [float(r[4]) for r in rows]
        highs   = [float(r[2]) for r in rows]
        lows    = [float(r[3]) for r in rows]
        n = len(closes)
        # True Range
        trs = [max(highs[i] - lows[i],
                   abs(highs[i] - closes[i-1]),
                   abs(lows[i]  - closes[i-1]))
               for i in range(1, n)]
        atr14 = sum(trs[-14:]) / 14 + 1e-10
        # Directional Movement
        pdm = [max(highs[i] - highs[i-1], 0)    for i in range(1, n)]
        mdm = [max(lows[i-1] - lows[i],   0)    for i in range(1, n)]
        pdm_c = [pdm[i] if pdm[i] > mdm[i] else 0 for i in range(len(pdm))]
        mdm_c = [mdm[i] if mdm[i] > pdm[i] else 0 for i in range(len(mdm))]
        pdi = 100 * (sum(pdm_c[-14:]) / 14) / atr14
        mdi = 100 * (sum(mdm_c[-14:]) / 14) / atr14
        # DX → ADX 近似（取最近 5 根 DX 均值，簡化版）
        _denom = pdi + mdi + 1e-10
        adx_approx = 100 * abs(pdi - mdi) / _denom
        regime = 'trending' if adx_approx >= 22 else 'ranging'
        label  = '趨勢市 🚀' if regime == 'trending' else '震盪市 🔄'
        print(f"   🌡️ 市場階段：{label}（BTC 4H ADX≈{adx_approx:.1f}）"
              f"{'→ 突破回踩策略優先' if regime == 'trending' else '→ SMC/ICT 策略優先'}")
        return regime
    except Exception:
        return 'trending'

def get_market_avg_funding_rate():
    """前10大幣種資金費率均值，>0.05%代表市場多頭過熱"""
    top_coins = ["BTC-USDT-SWAP","ETH-USDT-SWAP","SOL-USDT-SWAP","XRP-USDT-SWAP","BNB-USDT-SWAP",
                 "DOGE-USDT-SWAP","ADA-USDT-SWAP","AVAX-USDT-SWAP","DOT-USDT-SWAP","LINK-USDT-SWAP"]
    rates = []
    for coin in top_coins:
        try:
            url = f"{BASE_URL}/api/v5/public/funding-rate?instId={coin}"
            r = requests.get(url, timeout=2).json()
            if r.get('code') == '0':
                rates.append(float(r['data'][0]['fundingRate']))
        except:
            pass
    return sum(rates) / len(rates) if rates else 0.0

def find_market_structure_levels(df, entry, direction, atr, n=2):
    """
    從 K 線的擺動高低點（Swing High / Swing Low）推導 SL 和 TP 水位。

    邏輯：
      多頭 → SL = entry 下方最近支撐位（跌破代表趨勢失效）
             TP1/2/3 = entry 上方由近到遠的壓力位
      空頭 → 對稱反向

    n : 左右各幾根 K 需更高/更低才算成立的擺動點（預設 2，即 2+1+2 確認）
    ATR 倍數作為備援：找不到足夠水位時以 ATR 補全，確保函數永遠回傳有效數值。

    回傳 (sl, tp1, tp2, tp3)
    """
    lows_arr  = df['low'].values
    highs_arr = df['high'].values
    swing_lows, swing_highs = [], []

    for i in range(n, len(df) - n):
        # 擺動低點：左右各 n 根的最低值
        if lows_arr[i] == min(lows_arr[i-n:i+n+1]):
            swing_lows.append(float(lows_arr[i]))
        # 擺動高點：左右各 n 根的最高值
        if highs_arr[i] == max(highs_arr[i-n:i+n+1]):
            swing_highs.append(float(highs_arr[i]))

    # 合併距離 < 0.4% 的相鄰水位（防止同一區域重複計算）
    def cluster(levels, tol=0.004):
        if not levels:
            return []
        result = [sorted(levels)[0]]
        for lvl in sorted(levels)[1:]:
            if (lvl - result[-1]) / result[-1] < tol:
                result[-1] = (result[-1] + lvl) / 2  # 取平均作為代表水位
            else:
                result.append(lvl)
        return result

    supports    = cluster(swing_lows)
    resistances = cluster(swing_highs)

    # SL 最低距離：取 1 倍 ATR 與 0.4% 中的較大值
    # 確保正常市場波動不會在進場後立刻觸發止損
    min_sl_dist = max(atr * 1.0, entry * 0.004)

    if direction == "多":
        # SL：entry 下方至少 min_sl_dist 的最近支撐
        below = [s for s in supports if s <= entry - min_sl_dist]
        sl    = max(below) if below else entry - max(atr * 2.0, entry * 0.004)
        # 防呆：即使找到支撐，也確保距離足夠
        sl    = min(sl, entry - min_sl_dist)

        # TP：entry 上方壓力由近到遠
        above = sorted(r for r in resistances if r > entry * 1.001)
        tp1   = above[0] if len(above) >= 1 else entry + atr * 1.5
        tp2   = above[1] if len(above) >= 2 else tp1  + atr * 1.5
        tp3   = above[2] if len(above) >= 3 else tp2  + atr * 2.0
    else:
        # SL：entry 上方至少 min_sl_dist 的最近壓力
        above = [r for r in resistances if r >= entry + min_sl_dist]
        sl    = min(above) if above else entry + max(atr * 2.0, entry * 0.004)
        # 防呆：即使找到壓力，也確保距離足夠
        sl    = max(sl, entry + min_sl_dist)

        # TP：entry 下方支撐由近到遠
        below = sorted((s for s in supports if s < entry * 0.999), reverse=True)
        tp1   = below[0] if len(below) >= 1 else entry - atr * 1.5
        tp2   = below[1] if len(below) >= 2 else tp1  - atr * 1.5
        tp3   = below[2] if len(below) >= 3 else tp2  - atr * 2.0

    return sl, tp1, tp2, tp3


def get_fibonacci_context(df, entry, direction):
    """
    找出最近一次明顯擺動（多頭：波低→波高，空頭：波高→波低），
    計算斐波那契回撤水位（0.236/0.382/0.5/0.618/0.786），
    回傳最近的關鍵 Fib 水位與距離。

    回傳：(fib_label, fib_price, near_fib, dist_pct)
        fib_label : str  e.g. "0.618" 或 None（找不到有效擺動時）
        fib_price : float
        near_fib  : bool（距入場點 ≤ 1.5%）
        dist_pct  : float
    """
    KEY_FIBS = [0.236, 0.382, 0.500, 0.618, 0.786]
    N_CONFIRM = 3   # 左右各 3 根確認擺動點
    LOOKBACK  = 80  # 只看最近 80 根 K 線

    highs = df['high'].values[-LOOKBACK:]
    lows  = df['low'].values[-LOOKBACK:]

    swing_highs, swing_lows = [], []
    for i in range(N_CONFIRM, len(highs) - N_CONFIRM):
        if highs[i] == max(highs[i - N_CONFIRM: i + N_CONFIRM + 1]):
            swing_highs.append((i, float(highs[i])))
        if lows[i]  == min(lows[i  - N_CONFIRM: i + N_CONFIRM + 1]):
            swing_lows.append((i, float(lows[i])))

    if not swing_highs or not swing_lows:
        return None, None, False, 999.0

    try:
        if direction == "多":
            # 找「波低 → 波高」的最近上行擺動，然後量回撤
            last_hi_idx, last_hi_px = swing_highs[-1]
            lows_before = [(i, p) for i, p in swing_lows if i < last_hi_idx]
            if not lows_before:
                return None, None, False, 999.0
            _, swing_lo_px = lows_before[-1]
            rng = last_hi_px - swing_lo_px
            if rng < last_hi_px * 0.005:    # 擺動幅度 < 0.5%，無意義
                return None, None, False, 999.0
            # 回撤水位：從高點往下量
            levels = {f"{r:.3f}": last_hi_px - rng * r for r in KEY_FIBS}
        else:
            # 找「波高 → 波低」的最近下行擺動，然後量反彈
            last_lo_idx, last_lo_px = swing_lows[-1]
            highs_before = [(i, p) for i, p in swing_highs if i < last_lo_idx]
            if not highs_before:
                return None, None, False, 999.0
            _, swing_hi_px = highs_before[-1]
            rng = swing_hi_px - last_lo_px
            if rng < swing_hi_px * 0.005:
                return None, None, False, 999.0
            # 反彈水位：從低點往上量
            levels = {f"{r:.3f}": last_lo_px + rng * r for r in KEY_FIBS}
    except Exception:
        return None, None, False, 999.0

    # 找最近 Fib 水位
    closest_lbl = min(levels, key=lambda k: abs(levels[k] - entry))
    closest_px  = levels[closest_lbl]
    dist_pct    = abs(closest_px - entry) / entry * 100
    near_fib    = dist_pct <= 1.5   # ≤ 1.5% 算匯流

    return closest_lbl, closest_px, near_fib, round(dist_pct, 2)


def get_fibonacci_extension(df, entry, direction, sl):
    """
    用最近擺動的 A→B 波段計算 Fibonacci 擴展線作為 TP 目標。

    多頭：A = 最近擺動低點，B = 最近擺動高點（突破點）
          擴展目標 = B + (B-A) × ratio
          1.272 擴展 ≈ 保守 TP1
          1.618 擴展 ≈ 積極 TP2

    空頭：A = 最近擺動高點，B = 最近擺動低點（跌破點）
          擴展目標 = B - (A-B) × ratio

    回傳 (tp1_fib, tp2_fib, tp3_fib, fib_range) 或 None（找不到有效擺動時）
    fib_range = 擺動幅度，用於訊號訊息顯示
    """
    N_CONFIRM = 3
    LOOKBACK  = 60
    highs = df['high'].values[-LOOKBACK:]
    lows  = df['low'].values[-LOOKBACK:]

    swing_highs, swing_lows = [], []
    for i in range(N_CONFIRM, len(highs) - N_CONFIRM):
        if highs[i] == max(highs[i - N_CONFIRM: i + N_CONFIRM + 1]):
            swing_highs.append((i, float(highs[i])))
        if lows[i] == min(lows[i - N_CONFIRM: i + N_CONFIRM + 1]):
            swing_lows.append((i, float(lows[i])))

    if not swing_highs or not swing_lows:
        return None

    try:
        if direction == "多":
            # A = 最近擺動低點，B = 最近擺動高點
            a_idx, a_px = swing_lows[-1]
            b_candidates = [(i, p) for i, p in swing_highs if i > a_idx]
            if not b_candidates:
                return None
            b_idx, b_px = b_candidates[-1]
            rng = b_px - a_px
            if rng < entry * 0.005:   # 擺動幅度 < 0.5%，無意義
                return None
            # 擴展線：從 B 向上延伸
            tp1_fib = round(b_px + rng * 0.272, 8)   # 1.272 擴展
            tp2_fib = round(b_px + rng * 0.618, 8)   # 1.618 擴展
            tp3_fib = round(b_px + rng * 1.000, 8)   # 2.0   擴展
            # 確認 TP 在 entry 上方（防止擺動點選到進場點之前）
            if tp1_fib <= entry or tp2_fib <= tp1_fib:
                return None
        else:
            # A = 最近擺動高點，B = 最近擺動低點
            a_idx, a_px = swing_highs[-1]
            b_candidates = [(i, p) for i, p in swing_lows if i > a_idx]
            if not b_candidates:
                return None
            b_idx, b_px = b_candidates[-1]
            rng = a_px - b_px
            if rng < entry * 0.005:
                return None
            # 擴展線：從 B 向下延伸
            tp1_fib = round(b_px - rng * 0.272, 8)   # 1.272 擴展
            tp2_fib = round(b_px - rng * 0.618, 8)   # 1.618 擴展
            tp3_fib = round(b_px - rng * 1.000, 8)   # 2.0   擴展
            if tp1_fib >= entry or tp2_fib >= tp1_fib:
                return None

        # 最終安全檢查：TP1 R:R 需 ≥ 1.5
        _risk = abs(entry - sl)
        _rew  = abs(tp1_fib - entry)
        if _risk > 0 and _rew < _risk * 1.5:
            # TP1 不夠，嘗試直接用 TP2（1.618）作為第一目標
            _rew2 = abs(tp2_fib - entry)
            if _rew2 >= _risk * 1.5:
                tp1_fib, tp2_fib, tp3_fib = tp2_fib, tp3_fib, tp3_fib
            else:
                return None

        return tp1_fib, tp2_fib, tp3_fib, round(rng, 8)

    except Exception:
        return None


def detect_candle_pattern(df, direction, n_bars=3):
    """
    偵測最近 n_bars 根 K 棒中是否有確認進場的 K 線型態。

    多頭型態（回踩支撐後反彈確認）：
      - 釘頭錘 (Hammer/Pin Bar)：下影線 ≥ 2× 實體，上影線 ≤ 0.5× 實體，收陽
      - 多頭吞沒 (Bullish Engulfing)：當根陽線完全包覆前根陰線（收盤 > 前開，開盤 < 前收）

    空頭型態（回踩壓力後回落確認）：
      - 流星錘 (Shooting Star)：上影線 ≥ 2× 實體，下影線 ≤ 0.5× 實體，收陰
      - 空頭吞沒 (Bearish Engulfing)：當根陰線完全包覆前根陽線

    回傳 (pattern_name: str | None, bonus: int)
      pattern_name = None 表示無型態，bonus 用於評分
    """
    if len(df) < n_bars + 2:
        return None, 0

    best_pattern = None
    best_bonus   = 0

    for i in range(1, n_bars + 1):
        curr = df.iloc[-i]
        prev = df.iloc[-i - 1]

        o, h, l, c = float(curr['open']), float(curr['high']), float(curr['low']), float(curr['close'])
        body       = abs(c - o)
        upper_wick = h - max(c, o)
        lower_wick = min(c, o) - l
        _min_body  = float(curr.get('ATR', body + 1e-10)) * 0.05   # 防零除

        po, pc = float(prev['open']), float(prev['close'])

        if direction == "多":
            # 釘頭錘：長下影線 + 短上影線 + 陽線（或小陰線）
            if (lower_wick >= max(body, _min_body) * 2.0
                    and upper_wick <= max(body, _min_body) * 0.5
                    and c >= o):
                pat, bon = "🔨 釘頭錘", 12
                if bon > best_bonus:
                    best_pattern, best_bonus = pat, bon

            # 多頭吞沒：當根陽線包覆前根陰線
            if (c > o                       # 當根收陽
                    and pc < po             # 前根收陰
                    and c > po              # 當根收盤 > 前根開盤
                    and o < pc):            # 當根開盤 < 前根收盤
                pat, bon = "🕯️ 多頭吞沒", 15
                if bon > best_bonus:
                    best_pattern, best_bonus = pat, bon

        else:  # 空頭
            # 流星錘：長上影線 + 短下影線 + 陰線
            if (upper_wick >= max(body, _min_body) * 2.0
                    and lower_wick <= max(body, _min_body) * 0.5
                    and c <= o):
                pat, bon = "🌠 流星錘", 12
                if bon > best_bonus:
                    best_pattern, best_bonus = pat, bon

            # 空頭吞沒：當根陰線包覆前根陽線
            if (c < o                       # 當根收陰
                    and pc > po             # 前根收陽
                    and c < po              # 當根收盤 < 前根開盤
                    and o > pc):            # 當根開盤 > 前根收盤
                pat, bon = "🕯️ 空頭吞沒", 15
                if bon > best_bonus:
                    best_pattern, best_bonus = pat, bon

    return best_pattern, best_bonus


def find_range_bounds(df, n=2, tol=0.005, min_touches=2):
    """
    識別橫盤區間的支撐帶和壓力帶。
    邏輯：偵測擺動高低點 → 聚類 → 計算收盤價觸及次數 → 需 min_touches 次才算確認。
    回傳 (support_lo, support_hi, resist_lo, resist_hi) 或 (None, None, None, None)
    """
    lows   = df['low'].values
    highs  = df['high'].values
    closes = df['close'].values.astype(float)

    swing_lows, swing_highs = [], []
    for i in range(n, len(df) - n):
        if lows[i] == min(lows[i - n:i + n + 1]):
            swing_lows.append(float(lows[i]))
        if highs[i] == max(highs[i - n:i + n + 1]):
            swing_highs.append(float(highs[i]))

    def cluster_and_count(levels):
        if not levels:
            return []
        grouped = []
        for lvl in sorted(levels):
            if grouped and (lvl - grouped[-1]) / grouped[-1] < tol:
                grouped[-1] = (grouped[-1] + lvl) / 2
            else:
                grouped.append(lvl)
        result = []
        for lvl in grouped:
            touches = sum(1 for c in closes if abs(c - lvl) / lvl <= 0.005)
            result.append((lvl, touches))
        return result

    sup_levels = [(lvl, cnt) for lvl, cnt in cluster_and_count(swing_lows)  if cnt >= min_touches]
    res_levels = [(lvl, cnt) for lvl, cnt in cluster_and_count(swing_highs) if cnt >= min_touches]

    price = float(closes[-1])
    sup_below = [lvl for lvl, _ in sup_levels if lvl < price * 0.999]
    res_above = [lvl for lvl, _ in res_levels if lvl > price * 1.001]

    if not sup_below or not res_above:
        return None, None, None, None

    sup = max(sup_below)
    res = min(res_above)

    return sup * 0.997, sup * 1.003, res * 0.997, res * 1.003


def compute_poc(df, n_candles=50, n_buckets=200):
    """
    計算最近 n_candles 根 K 棒的成交量分布 Point of Control（成交量最大聚集的價格）。
    使用等寬價格桶，每根 K 棒成交量按其高低點範圍均勻分配到覆蓋的桶中。
    回傳 poc_price（float）或 None（資料不足時）。
    """
    try:
        recent = df.tail(n_candles)
        if len(recent) < 10:
            return None
        lo = float(recent['low'].min())
        hi = float(recent['high'].max())
        if hi <= lo or (hi - lo) < lo * 0.0001:
            return None
        bucket_size = (hi - lo) / n_buckets
        vol_profile = np.zeros(n_buckets)
        for _, row in recent.iterrows():
            b_lo = max(0, min(int((row['low']  - lo) / bucket_size), n_buckets - 1))
            b_hi = max(0, min(int((row['high'] - lo) / bucket_size), n_buckets - 1))
            span = max(1, b_hi - b_lo + 1)
            vol_profile[b_lo:b_hi + 1] += row['vol'] / span
        poc_idx = int(np.argmax(vol_profile))
        return lo + (poc_idx + 0.5) * bucket_size
    except Exception:
        return None


def poc_check(poc, entry, atr):
    """
    比較進場點與 POC 的距離，回傳 (bonus: int, label: str)。
    bonus：正數加分、負數扣分（納入 score/win_rate 計算）。
    label：顯示在訊號訊息的說明文字。
    距離標準（以 entry 百分比）：
      ≤0.3%  → POC 匯流，強確認          +10
      ≤1.0%  → 靠近 POC，有支撐          +5
      ≤2.5%  → 參考距離，中性             0
      ≤4.0%  → 偏離 POC，訊號較弱        -8
      >4.0%  → 嚴重偏離 POC，遠離成交密集區 -15
    原設定 >2.5% 只扣 5 分懲罰太輕，對遠離機構成交密集區的進場點應大幅扣分。
    """
    if poc is None:
        return 0, ""
    dist_pct = abs(entry - poc) / max(entry, 1e-10) * 100
    poc_str  = format_price(poc)
    if dist_pct <= 0.3:
        return 10,  f"✅ POC匯流 <code>{poc_str}</code>（差{dist_pct:.2f}%）"
    elif dist_pct <= 1.0:
        return 5,   f"📊 近POC <code>{poc_str}</code>（差{dist_pct:.1f}%）"
    elif dist_pct <= 2.5:
        return 0,   f"📊 POC <code>{poc_str}</code>（差{dist_pct:.1f}%）"
    elif dist_pct <= 4.0:
        return -8,  f"⚠️ 偏離POC <code>{poc_str}</code>（差{dist_pct:.1f}%）"
    else:
        return -15, f"🚫 嚴重偏離POC <code>{poc_str}</code>（差{dist_pct:.1f}%）"


def detect_fvg(df, entry, direction, n_lookback=30):
    """
    偵測最近 n_lookback 根 K 棒中，最靠近進場點且仍有效的 FVG（公平價值缺口）。

    多頭 FVG（做多支撐）：df[i].high < df[i+2].low
      缺口區 = [df[i].high, df[i+2].low]，作為多頭回踩支撐區
    空頭 FVG（做空壓力）：df[i].low > df[i+2].high
      缺口區 = [df[i+2].high, df[i].low]，作為空頭反彈壓力區

    有效性：缺口形成後，後續 K 棒的收盤價未穿越缺口邊界（底部/頂部）。
    回傳 (fvg_lo, fvg_hi) 或 (None, None)。
    """
    try:
        n = len(df)
        if n < 5:
            return None, None
        candidates = []
        # 從最近的 pattern 往前掃（n-3 → n-n_lookback-2）
        start = max(0, n - n_lookback - 2)
        for idx in range(start, n - 2):
            c1 = df.iloc[idx]
            c3 = df.iloc[idx + 2]
            if direction == "多":
                # 多頭 FVG：c1.high < c3.low
                if c3['low'] > c1['high']:
                    fvg_lo = float(c1['high'])
                    fvg_hi = float(c3['low'])
                    # 後續 K 棒收盤未穿越缺口底部 → 仍有效
                    post = df.iloc[idx + 3:]
                    if post.empty or (post['close'] >= fvg_lo).all():
                        dist = abs(entry - (fvg_lo + fvg_hi) / 2) / max(entry, 1e-10)
                        candidates.append((fvg_lo, fvg_hi, dist))
            else:
                # 空頭 FVG：c1.low > c3.high
                if c1['low'] > c3['high']:
                    fvg_lo = float(c3['high'])
                    fvg_hi = float(c1['low'])
                    # 後續 K 棒收盤未穿越缺口頂部 → 仍有效
                    post = df.iloc[idx + 3:]
                    if post.empty or (post['close'] <= fvg_hi).all():
                        dist = abs(entry - (fvg_lo + fvg_hi) / 2) / max(entry, 1e-10)
                        candidates.append((fvg_lo, fvg_hi, dist))
        if not candidates:
            return None, None
        # 選最靠近 entry 的有效 FVG
        best = min(candidates, key=lambda x: x[2])
        return best[0], best[1]
    except Exception:
        return None, None


def fvg_check(fvg_lo, fvg_hi, entry):
    """
    比較進場點與 FVG 缺口區的關係，回傳 (bonus: int, label: str)。
    進場在缺口內（最佳回踩點）：+8，label 顯示 ✅ FVG匯流
    進場在缺口邊緣（距中心 ≤1%）：+5，label 顯示 📊 近FVG
    進場不在缺口附近：0, ""（FVG 只做加分，不做扣分）
    """
    if fvg_lo is None or fvg_hi is None:
        return 0, ""
    lo_str = format_price(fvg_lo)
    hi_str = format_price(fvg_hi)
    zone   = f"[<code>{lo_str}</code>–<code>{hi_str}</code>]"
    if fvg_lo <= entry <= fvg_hi:
        return 8, f"✅ FVG匯流 {zone}  進場在缺口內"
    dist_pct = abs(entry - (fvg_lo + fvg_hi) / 2) / max(entry, 1e-10) * 100
    if dist_pct <= 1.0:
        side = "下緣附近" if entry < fvg_lo else "上緣附近"
        return 5, f"📊 近FVG {zone}  {side}（差{dist_pct:.1f}%）"
    return 0, ""


def get_candle_range_since(inst_id, since_ts, bar="1H", no_margin=False):
    """取得 since_ts 以來所有 K 線的最高價和最低價。
    用於偵測監控間隔中曾觸碰的價格極值，防止漏掉 TP/SL/填單事件。
    no_margin=True：嚴格只取 since_ts 之後開始的 K 棒（用於填單判定，避免納入舊棒）。
    回傳 (range_high, range_low) 或 (None, None)。
    limit=100：覆蓋最近 100 根 K 棒（1m = 100 分鐘），確保監控間隔內的極值不被遺漏。
    """
    try:
        url = f"{BASE_URL}/api/v5/market/candles?instId={inst_id}&bar={bar}&limit=100"
        res = requests.get(url, timeout=3).json()
        if res.get('code') == '0' and res['data']:
            df = pd.DataFrame(res['data'],
                              columns=['ts','open','high','low','close','vol','volCcy','volCcyQuote','state'])
            df['ts']   = df['ts'].astype(float) / 1000   # ms → seconds
            df['high'] = df['high'].astype(float)
            df['low']  = df['low'].astype(float)
            if no_margin:
                # 嚴格過濾：只包含在 since_ts 之後「開盤」的 K 棒
                df = df[df['ts'] >= since_ts]
            else:
                # 包含 since_ts 之前 1 根 K 線（防止 TP/SL 事件在兩次監控間被漏掉）
                # margin = 各 bar 對應的一個週期長度（確保回看不超過一根 K 線）
                _bar_secs = {"15m": 900, "30m": 1800, "1H": 3600, "4H": 14400, "1D": 86400}
                margin = _bar_secs.get(bar, 3600)
                df = df[df['ts'] >= since_ts - margin]
            if not df.empty:
                return float(df['high'].max()), float(df['low'].min())
    except:
        pass
    return None, None

def check_market_deterioration(inst_id, direction, tf):
    """已成交持倉的市場局勢惡化偵測。
    檢查三個指標：BTC趨勢逆向、ADX跌破20（趨勢消失）、資金費率極端逆向。
    ≥2 個惡化訊號時回傳警告字串，否則回傳 None。
    """
    warnings = []

    # ── 1. 該幣自身趨勢是否已逆向（MA8/EMA89 1H）──
    try:
        coin = inst_id.split('-')[0]
        coin_trend = _fetch_one_trend(coin)
        if direction == "多" and coin_trend == "bear":
            warnings.append(f"{coin} 1H MA8 已跌破 EMA89，多頭趨勢反轉")
        elif direction == "空" and coin_trend == "bull":
            warnings.append(f"{coin} 1H MA8 已站上 EMA89，空頭趨勢反轉")
    except:
        pass

    # ── 2. ADX 跌破 20（持倉期間趨勢消失）──
    try:
        if "15M" in tf:
            bar = "15m"
        elif "30M" in tf:
            bar = "30m"
        elif "1D" in tf:
            bar = "1D"
        elif "4H" in tf:
            bar = "4H"
        else:
            bar = "1H"
        url = f"{BASE_URL}/api/v5/market/candles?instId={inst_id}&bar={bar}&limit=60"
        res = requests.get(url, timeout=3).json()
        if res.get('code') == '0' and len(res['data']) >= 30:
            df = pd.DataFrame(res['data'],
                              columns=['ts','open','high','low','close','vol','volCcy','volCcyQuote','state'])
            df[['high','low','close']] = df[['high','low','close']].astype(float)
            df = df.iloc[::-1].reset_index(drop=True)
            df['H-L']  = df['high'] - df['low']
            df['H-PC'] = (df['high'] - df['close'].shift()).abs()
            df['L-PC'] = (df['low']  - df['close'].shift()).abs()
            df['TR']   = df[['H-L','H-PC','L-PC']].max(axis=1)
            df['ATR14']= df['TR'].rolling(14).mean()
            plus_dm    = df['high'].diff().clip(lower=0)
            minus_dm   = (-df['low'].diff()).clip(lower=0)
            mask       = plus_dm >= minus_dm
            plus_dm_c  = plus_dm.where(mask, 0)
            minus_dm_c = minus_dm.where(~mask, 0)
            atr14      = df['ATR14'] + 1e-10
            plus_di    = 100 * plus_dm_c.rolling(14).mean()  / atr14
            minus_di   = 100 * minus_dm_c.rolling(14).mean() / atr14
            dx         = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di + 1e-10)
            adx        = dx.rolling(14).mean().iloc[-1]
            if not pd.isna(adx) and adx < 20:
                warnings.append(f"ADX 已跌至 {adx:.1f}（趨勢消失，波動方向不可靠）")
    except:
        pass

    # ── 3. 資金費率極端逆向 ──
    try:
        fr_url = f"{BASE_URL}/api/v5/public/funding-rate?instId={inst_id}"
        fr_res = requests.get(fr_url, timeout=2).json()
        if fr_res.get('code') == '0':
            fr = float(fr_res['data'][0]['fundingRate'])
            if direction == "多" and fr < -0.0003:
                warnings.append(f"資金費率極端偏空（{fr*100:.4f}%），市場強烈看空")
            elif direction == "空" and fr > 0.0003:
                warnings.append(f"資金費率極端偏多（+{fr*100:.4f}%），市場強烈看多")
    except:
        pass

    if len(warnings) >= 2:
        warn_lines = "\n".join(f"     ⚠️ {w}" for w in warnings)
        return (f"🚨 <b>市場局勢惡化警告</b>（{len(warnings)}/3 指標異常）：\n"
                f"{warn_lines}\n"
                f"     👉 <b>建議提前出場或收緊止損至成本，勿等止損觸發</b>")
    return None

def get_higher_tf_alignment(asset, direction, higher_bar="4H"):
    """
    檢查更高時框的趨勢狀態。
    回傳 (aligned: bool, htf_adx: float)：
      aligned  → HTF MA8/EMA89 方向是否與訊號方向一致
      htf_adx  → HTF ADX 值（<25 = 大週期盤整，小週期交叉幾乎全是噪音）

    若 API 失敗則回傳 (True, 30) 不懲罰，但不視作強確認。
    """
    try:
        url = f"{BASE_URL}/api/v5/market/candles?instId={asset}&bar={higher_bar}&limit=120"
        res = requests.get(url, timeout=3.0).json()
        if res.get('code') == '0' and len(res['data']) >= 50:
            df = pd.DataFrame(res['data'],
                              columns=['ts','open','high','low','close','vol','volCcy','volCcyQuote','state'])
            for col in ['open','high','low','close']:
                df[col] = df[col].astype(float)
            df = df.iloc[::-1].reset_index(drop=True)
            df['MA8']   = df['close'].rolling(8).mean()
            df['EMA89'] = df['close'].ewm(span=89, adjust=False).mean()

            # ── ADX（14 週期）──
            plus_dm  = df['high'].diff().clip(lower=0)
            minus_dm = (-df['low'].diff()).clip(lower=0)
            mask     = plus_dm >= minus_dm
            pdm      = plus_dm.where(mask, 0)
            mdm      = minus_dm.where(~mask, 0)
            h_l      = df['high'] - df['low']
            h_pc     = (df['high'] - df['close'].shift(1)).abs()
            l_pc     = (df['low']  - df['close'].shift(1)).abs()
            atr_h    = pd.concat([h_l, h_pc, l_pc], axis=1).max(axis=1).rolling(14).mean() + 1e-10
            plus_di  = 100 * pdm.rolling(14).mean() / atr_h
            minus_di = 100 * mdm.rolling(14).mean() / atr_h
            dx       = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di + 1e-10)
            df['ADX'] = dx.rolling(14).mean()

            last    = df.iloc[-1]
            aligned = (direction == "多" and last['MA8'] > last['EMA89']) or \
                      (direction == "空" and last['MA8'] < last['EMA89'])

            # ── 近中性容忍（±0.25%）──
            # MA8 與 EMA89 差距 ≤ 0.25% 時，視為轉折中性區，放行。
            # 原設定 0.5% 過於寬鬆——方向正在切換的 HTF 最容易出現假突破，
            # 縮小至 0.25% 只讓「幾乎貼合」的 MA 通過，真正逆向的 HTF 仍硬擋。
            if not aligned:
                gap_pct = abs(last['MA8'] - last['EMA89']) / (last['EMA89'] + 1e-10) * 100
                if gap_pct <= 0.25:
                    aligned = True   # 差距 ≤ 0.25%：視為中性，放行

            htf_adx = float(last['ADX']) if pd.notna(last['ADX']) else 0.0
            return aligned, htf_adx
    except:
        pass
    return True, 30.0  # API 失敗時：不懲罰，但標記為一般強度

# ==================== ⚙️ 5. 同步 K 線與勝率量化評分核心 ====================
def score_to_win_rate(score):
    """將內部評分（0-130）映射為 50%–98% 勝率顯示"""
    return min(98, max(50, int(50 + (score / 130) * 48)))

def score_to_leverage(win_rate, max_leverage, signal_type='trend'):
    """依TA分取最大槓桿的比例，結果不超過交易所上限。
    底線 40%（BTC 100x = 40x）全幣種統一適用。
    策略風險越高，上限越低：趨勢 100%，區間 60%，背離 50%。"""
    if win_rate >= 90:
        ratio = 1.00
    elif win_rate >= 85:
        ratio = 0.80
    elif win_rate >= 78:
        ratio = 0.60
    elif win_rate >= 68:
        ratio = 0.50
    else:
        ratio = 0.40   # 底線 40%，全幣種適用
    # 策略風險壓制（上限均 ≥ 40%，不低於底線）
    if signal_type == 'range':
        ratio = min(ratio, 0.60)        # 區間單：最高 60% 交易所上限
    elif signal_type == 'divergence':
        ratio = min(ratio, 0.50)        # 背離單：最高 50% 交易所上限（逆勢策略）
    lev = max(1, round(max_leverage * ratio))
    return f"{lev}x"

_TF_BAR   = {"15m": "15m", "30m": "30m", "1h": "1H", "4h": "4H", "1d": "1D"}   # OKX bar param
_TF_LABEL = {"15m": "15M", "30m": "30M", "1h": "1H", "4h": "4H", "1d": "1D"}   # display label
_TF_HTF   = {"15m": "1H",  "30m": "1H", "1h": "4H", "4h": "1D", "1d": "1W"}    # 高一級時框

# 幣種 → 生態鏈參考幣對照表
# 邏輯：代幣跟著所屬鏈走，鏈本身跟著 BTC（大盤）走，不認識的幣預設參考 BTC
_CHAIN_REF: dict[str, str] = {
    # ── 大盤基準 ──
    "BTC": "BTC",
    # ── ETH 生態 ──
    "ETH": "BTC",
    "UNI": "ETH", "AAVE": "ETH", "LINK": "ETH", "MKR": "ETH",
    "COMP": "ETH", "SNX": "ETH", "CRV": "ETH", "BAL": "ETH",
    "1INCH": "ETH", "SUSHI": "ETH", "YFI": "ETH", "LDO": "ETH",
    "RPL": "ETH", "ENS": "ETH", "ARB": "ETH", "OP": "ETH",
    "BLUR": "ETH", "PENDLE": "ETH", "EIGEN": "ETH", "ENA": "ETH",
    "GRT": "ETH", "IMX": "ETH", "MANTA": "ETH", "STRK": "ETH",
    "DYDX": "ETH", "MAGIC": "ETH", "RDNT": "ETH", "GMX": "ETH",
    "WLD": "ETH", "PYUSD": "ETH",
    # ── SOL 生態 ──
    "SOL": "BTC",
    "RAY": "SOL", "BONK": "SOL", "WIF": "SOL", "JUP": "SOL",
    "PYTH": "SOL", "RNDR": "SOL", "RENDER": "SOL", "POPCAT": "SOL",
    "JITO": "SOL", "DRIFT": "SOL", "MOODENG": "SOL", "PNUT": "SOL",
    "MEW": "SOL", "BOME": "SOL", "SLND": "SOL", "FIDA": "SOL",
    # ── BNB 生態 ──
    "BNB": "BTC",
    "CAKE": "BNB", "TWT": "BNB", "ID": "BNB", "HIGH": "BNB",
    # ── AVAX 生態 ──
    "AVAX": "BTC",
    "JOE": "AVAX", "QI": "AVAX",
    # ── SUI 生態 ──
    "SUI": "BTC",
    "NAVX": "SUI", "BUCK": "SUI",
    # ── NEAR 生態 ──
    "NEAR": "BTC",
    "REF": "NEAR",
    # ── TON 生態 ──
    "TON": "BTC",
    "NOT": "TON", "DOGS": "TON",
    # ── 其他主鏈（直接參考 BTC）──
    "XRP": "BTC", "ADA": "BTC", "DOT": "BTC", "ATOM": "BTC",
    "MATIC": "BTC", "POL": "BTC", "FTM": "BTC", "ALGO": "BTC",
    "ICP": "BTC", "FIL": "BTC", "APT": "BTC", "TRX": "BTC",
    "HBAR": "BTC", "XLM": "BTC", "VET": "BTC", "EOS": "BTC",
    "THETA": "BTC", "EGLD": "BTC", "FLOW": "BTC", "KSM": "BTC",
}

# ── 板塊分類表（Sector Map）──────────────────────────────────────────────
# 用途：板塊集體拉升時去重，同板塊最多保留 2 個最強訊號，防止倉位過度集中。
# 找不到對應板塊的幣種，以幣種名稱自身為鍵（不被過濾）。
_SECTOR_MAP: dict[str, str] = {
    # ── Layer 1 主鏈 ──
    "BTC": "L1", "ETH": "L1", "SOL": "L1", "BNB": "L1", "AVAX": "L1",
    "ADA": "L1", "DOT": "L1", "ATOM": "L1", "TRX": "L1", "NEAR": "L1",
    "ICP": "L1", "APT": "L1", "SUI": "L1", "TON": "L1", "FTM": "L1",
    "ALGO": "L1", "HBAR": "L1", "XLM": "L1", "VET": "L1", "KSM": "L1",
    "EOS": "L1", "THETA": "L1", "EGLD": "L1", "FLOW": "L1",
    # ── Layer 2 擴容 ──
    "ARB": "L2", "OP": "L2", "MATIC": "L2", "POL": "L2", "STRK": "L2",
    "MANTA": "L2", "IMX": "L2",
    # ── DeFi / DEX ──
    "UNI": "DeFi", "AAVE": "DeFi", "MKR": "DeFi", "COMP": "DeFi",
    "CRV": "DeFi", "SUSHI": "DeFi", "YFI": "DeFi", "GMX": "DeFi",
    "DYDX": "DeFi", "1INCH": "DeFi", "PENDLE": "DeFi", "SNX": "DeFi",
    "BAL": "DeFi", "RDNT": "DeFi",
    # ── AI / 去中心化計算 ──
    "GRT": "AI", "RNDR": "AI", "RENDER": "AI", "PYTH": "AI", "WLD": "AI",
    "TAO": "AI", "FET": "AI", "AGIX": "AI", "OCEAN": "AI",
    # ── 預言機 / 基礎設施 ──
    "LINK": "Infra", "ENS": "Infra", "API3": "Infra",
    # ── LSD / 流動性質押 ──
    "LDO": "LSD", "RPL": "LSD", "JITO": "LSD", "EIGEN": "LSD",
    # ── SOL 生態 DeFi ──
    "RAY": "SOL_eco", "JUP": "SOL_eco", "DRIFT": "SOL_eco",
    # ── SOL 生態 Meme ──
    "BONK": "Meme_SOL", "WIF": "Meme_SOL", "POPCAT": "Meme_SOL",
    "MOODENG": "Meme_SOL", "PNUT": "Meme_SOL", "MEW": "Meme_SOL",
    "BOME": "Meme_SOL",
    # ── TON 生態 ──
    "NOT": "Meme_TON", "DOGS": "Meme_TON",
    # ── 支付 / 跨境匯款 ──
    "XRP": "Payments", "XLM": "Payments",
    # ── 儲存 / 分散式計算 ──
    "FIL": "Storage", "AR": "Storage",
    # ── 遊戲 / 元宇宙 ──
    "MAGIC": "Gaming", "GALA": "Gaming", "AXS": "Gaming", "SAND": "Gaming",
    "MANA": "Gaming",
}

def get_higher_tf_ema89_slope(asset, current_bar):
    """取高一級時間框架的 EMA89 斜率：15M→1H，1H→4H，4H→1D。回傳正數=上行，負數=下行，None=取得失敗。"""
    htf_bar = _TF_HTF.get(current_bar.lower(), "1D") if current_bar.endswith("m") else \
              ("4H" if current_bar == "1H" else "1D")
    url = f"{BASE_URL}/api/v5/market/candles?instId={asset}&bar={htf_bar}&limit=100"
    try:
        res = requests.get(url, timeout=2.5).json()
        if res.get('code') != '0' or len(res['data']) < 30:
            return None
        closes = [float(r[4]) for r in reversed(res['data'])]
        k = 2 / (89 + 1)
        ema = closes[0]
        for c in closes[1:]:
            ema = c * k + ema * (1 - k)
        # 重跑一次取倒數第4根的 EMA89
        ema_3ago = closes[0]
        for c in closes[1:-3]:
            ema_3ago = c * k + ema_3ago * (1 - k)
        return ema - ema_3ago   # 正=上行，負=下行
    except Exception:
        return None


def fetch_candle_sync(asset, tf, max_leverage=20, ref_trends=None, market_fr=0.0):
    bar_param = _TF_BAR.get(tf, "1H")
    url = f"{BASE_URL}/api/v5/market/candles?instId={asset}&bar={bar_param}&limit=300"
    try:
        res = requests.get(url, timeout=2.5).json()
        if res.get('code') == '0' and len(res['data']) >= 90:
            df = pd.DataFrame(res['data'], columns=['ts','open','high','low','close','vol','volCcy','volCcyQuote','state'])
            for col in ['open','high','low','close','vol']:
                df[col] = df[col].astype(float)
            df = df.iloc[::-1].reset_index(drop=True)

            # ── 基礎指標 ──
            df['MA8']   = df['close'].rolling(8).mean()
            df['EMA89'] = df['close'].ewm(span=89,  adjust=False).mean()
            df['EMA200']= df['close'].ewm(span=200, adjust=False).mean()

            # ── MACD（12/26/9）—— 多因子策略核心確認指標 ──
            _ema12 = df['close'].ewm(span=12, adjust=False).mean()
            _ema26 = df['close'].ewm(span=26, adjust=False).mean()
            df['MACD']     = _ema12 - _ema26
            df['MACD_SIG'] = df['MACD'].ewm(span=9, adjust=False).mean()
            df['MACD_HIST']= df['MACD'] - df['MACD_SIG']

            delta = df['close'].diff()
            gain  = delta.where(delta > 0, 0).rolling(14).mean()
            loss  = (-delta.where(delta < 0, 0)).rolling(14).mean()
            df['RSI'] = 100 - (100 / (1 + gain / (loss + 1e-10)))

            # ── ATR（動態止損基礎）──
            df['H-L']  = df['high'] - df['low']
            df['H-PC'] = (df['high'] - df['close'].shift(1)).abs()
            df['L-PC'] = (df['low']  - df['close'].shift(1)).abs()
            df['TR']   = df[['H-L','H-PC','L-PC']].max(axis=1)
            df['ATR14']= df['TR'].rolling(14).mean()

            # ── ADX（趨勢強度，>20才有方向性）──
            plus_dm  = df['high'].diff().clip(lower=0)
            minus_dm = (-df['low'].diff()).clip(lower=0)
            mask = plus_dm >= minus_dm
            plus_dm_clean  = plus_dm.where(mask, 0)
            minus_dm_clean = minus_dm.where(~mask, 0)
            atr14 = df['ATR14'] + 1e-10
            plus_di  = 100 * plus_dm_clean.rolling(14).mean()  / atr14
            minus_di = 100 * minus_dm_clean.rolling(14).mean() / atr14
            dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di + 1e-10)
            df['ADX'] = dx.rolling(14).mean()
            # DI+ / DI- 末根值（用於方向確認閘）
            c_di_plus  = float(plus_di.iloc[-1])
            c_di_minus = float(minus_di.iloc[-1])

            c_last = df.iloc[-1]

            # ── 交叉偵測：依時框調整回望窗口 ──
            # 15m=12根(3h) | 1h=10根(10h) | 4h=5根(20h)
            cross_window = (20 if tf == "15m" else
                            14 if tf == "30m" else
                            10 if tf == "1h"  else
                             5 if tf == "4h"  else 3)
            is_cross_up   = False
            is_cross_down = False
            cross_vol     = None   # 交叉當根的成交量
            _cross_i      = None   # 交叉發生在距最新根幾根前

            for _i in range(1, min(cross_window + 1, len(df) - 1)):
                _curr = df.iloc[-_i]
                _prev = df.iloc[-(_i + 1)]
                if _prev['MA8'] <= _prev['EMA89'] and _curr['MA8'] > _curr['EMA89']:
                    is_cross_up = True
                    cross_vol   = _curr['vol']
                    _cross_i    = _i
                    break
                elif _prev['MA8'] >= _prev['EMA89'] and _curr['MA8'] < _curr['EMA89']:
                    is_cross_down = True
                    cross_vol     = _curr['vol']
                    _cross_i      = _i
                    break

            # ── 若無新交叉，偵測「突破回踩阻力位」（Strategy 1: Breakout Pullback）──
            # 核心概念：趨勢確立後，價格回踩當初突破的阻力位（現為支撐）= 最強再進場點。
            # ① 找「突破阻力位」= 趨勢確立前的最後一個擺動高點（局部峰值）
            # ② 偵測現價是否回踩至該位置 1.5% 以內，且 close 收回阻力位上方（彈起確認）
            # 找不到擺動點時 fallback 至 MA8（現行機制）
            _is_pullback = False
            _pb_level    = None   # 阻力位（突破後成為支撐的那條線）
            if not (is_cross_up or is_cross_down):
                _n_chk = min(8, len(df) - 2)
                _up_bars = sum(1 for i in range(1, _n_chk + 1)
                               if df.iloc[-i]['MA8'] > df.iloc[-i]['EMA89'])
                _dn_bars = sum(1 for i in range(1, _n_chk + 1)
                               if df.iloc[-i]['MA8'] < df.iloc[-i]['EMA89'])

                if _up_bars >= 6 and c_last['MA8'] > c_last['EMA89']:
                    # 找趨勢確立前的最後擺動高點（局部峰值 = 被突破的阻力位）
                    _lookback = min(60, len(df) - _n_chk - 3)
                    _swing_high = None
                    for _j in range(_n_chk + 2, _n_chk + _lookback):
                        if _j + 1 >= len(df):
                            break
                        _bj    = df.iloc[-_j]
                        _bprev = df.iloc[-(_j + 1)]
                        _bnext = df.iloc[-(_j - 1)] if _j > 1 else _bj
                        if float(_bj['high']) > float(_bprev['high']) and \
                           float(_bj['high']) > float(_bnext['high']):
                            _swing_high = float(_bj['high'])
                            break

                    # 偵測回踩：low 觸碰阻力位 1.5% 內，close 收回阻力位上方
                    for _i in range(1, 5):
                        _b   = df.iloc[-_i]
                        _ref = _swing_high if _swing_high else float(_b['MA8'])
                        if float(_b['low']) <= _ref * 1.015 and \
                           float(_b['close']) >= _ref * 0.990:
                            is_cross_up  = True
                            cross_vol    = float(_b['vol'])
                            _cross_i     = _i
                            _is_pullback = True
                            _pb_level    = _ref
                            break

                elif _dn_bars >= 6 and c_last['MA8'] < c_last['EMA89']:
                    # 找趨勢確立前的最後擺動低點（被跌破的支撐位 = 現在壓力位）
                    _lookback = min(60, len(df) - _n_chk - 3)
                    _swing_low = None
                    for _j in range(_n_chk + 2, _n_chk + _lookback):
                        if _j + 1 >= len(df):
                            break
                        _bj    = df.iloc[-_j]
                        _bprev = df.iloc[-(_j + 1)]
                        _bnext = df.iloc[-(_j - 1)] if _j > 1 else _bj
                        if float(_bj['low']) < float(_bprev['low']) and \
                           float(_bj['low']) < float(_bnext['low']):
                            _swing_low = float(_bj['low'])
                            break

                    for _i in range(1, 5):
                        _b   = df.iloc[-_i]
                        _ref = _swing_low if _swing_low else float(_b['MA8'])
                        if float(_b['high']) >= _ref * 0.985 and \
                           float(_b['close']) <= _ref * 1.010:
                            is_cross_down = True
                            cross_vol     = float(_b['vol'])
                            _cross_i      = _i
                            _is_pullback  = True
                            _pb_level     = _ref
                            break

            if not (is_cross_up or is_cross_down):
                return None

            direction = "多" if is_cross_up else "空"

            # 確認現在仍維持交叉方向（交叉後未立即反轉）
            if direction == "多" and c_last['MA8'] <= c_last['EMA89']:
                return None
            if direction == "空" and c_last['MA8'] >= c_last['EMA89']:
                return None

            # ── K棒確認：交叉後需有至少一根後續K棒站穩 MA8 方向 ──
            # 若交叉在最新根（_cross_i=1），df.iloc[-2] 是交叉前的棒，
            # 不能用來確認 → 跳過（當根方向已由上方 MA8>EMA89 確認）
            if _cross_i is not None and _cross_i >= 2:
                _check = df.iloc[-2]   # 交叉後的第一根已收盤棒
                if direction == "多" and _check['close'] <= _check['MA8']:
                    return None   # 收在 MA8 以下 → 未站穩，捨棄
                if direction == "空" and _check['close'] >= _check['MA8']:
                    return None   # 收在 MA8 以上 → 未站穩，捨棄

            current_price = c_last['close']
            current_rsi   = c_last['RSI']
            current_ema89 = c_last['EMA89']
            current_ma8   = c_last['MA8']
            current_adx   = c_last['ADX']
            current_atr   = c_last['ATR14']

            # ① ADX 過濾：< 25 表示趨勢不明確，MA 交叉失誤率高
            # 原門檻 20 過低——ADX 20-24 僅代表「微弱」趨勢，噪訊假突破多；
            # 提高至 25 為業界標準「趨勢確立」分界線。
            if current_adx < 25:
                return None

            # ② 成交量確認：極低量（< 40% 均量）才硬擋，其餘進入評分懲罰
            avg_vol_20 = df['vol'].iloc[-22:-2].mean()
            if cross_vol is None:
                cross_vol = df['vol'].iloc[-2]
            if cross_vol <= avg_vol_20 * 0.4:
                return None  # 極低量假突破直接過濾

            # ③ EMA89 斜率過濾：EMA89 橫盤或逆向時交叉幾乎全是假突破
            ema89_slope = c_last['EMA89'] - df['EMA89'].iloc[-4]  # 最近3根的斜率
            # 15m/30m 容忍微幅逆斜：EMA89 是 89 根均線，突破初期轉向極慢，
            # BTC 在 $63000 時 -$8 斜率 = 0.013%，不應算「逆趨勢」。
            # 容忍範圍 = 0.05×ATR（BTC ATR14~$180 → 容忍 -$9），嚴重下行仍硬擋。
            _slope_tol = current_atr * 0.05 if tf in ("15m", "30m") else 0
            if direction == "多" and ema89_slope < -_slope_tol:
                return None  # EMA89 明顯下行，多頭訊號不可信
            if direction == "空" and ema89_slope > _slope_tol:
                return None  # EMA89 明顯上行，空頭訊號不可信

            # ④ RSI 背離過濾：價格方向與 RSI 動能背離 → 假突破機率高
            price_5ago = df['close'].iloc[-6]  # 交叉棒往前5根
            rsi_5ago   = df['RSI'].iloc[-6]
            if direction == "多" and current_price > price_5ago and current_rsi < rsi_5ago:
                return None  # 頂背離：價格新高但 RSI 沒跟上，多頭動能衰竭
            if direction == "空" and current_price < price_5ago and current_rsi > rsi_5ago:
                return None  # 底背離：價格新低但 RSI 沒跟上，空頭動能衰竭

            # ⑤ DI+ vs DI- 方向壓力閘（僅 1h / 4h / 1d 啟用）
            # DI 是滯後指標，計算的是「過去 14 根」的方向壓力累計。
            # 15m/30m：MA8/EMA89 交叉發生時 DI 往往還未切換（要 3–5 根才追上），
            #          硬擋會讓所有短線早期訊號（如 BTC 剛破局時）全部被過濾掉。
            #          短線已有 EMA89 斜率 + HTF 對齊 + RSI 作防護，不需要 DI 確認。
            # 1h+：等 DI 確認的代價低，保留以強化訊號可信度。
            if tf in ("1h", "4h", "1d"):
                if direction == "多" and c_di_plus <= c_di_minus:
                    return None   # 空方向壓(DI-)仍 > 多方向壓(DI+)，多叉可信度低
                if direction == "空" and c_di_minus <= c_di_plus:
                    return None   # 多方向壓(DI+)仍 > 空方向壓(DI-)，死叉可信度低

            # ⑥ RSI 極端區間硬性過濾（防追高殺低）
            if direction == "多" and current_rsi > 76:
                return None
            if direction == "空" and current_rsi < 24:
                return None

            # ⑦ EMA200 大趨勢方向硬性過濾（機構趨勢線，多因子策略核心條件）
            # 概念：EMA200 = 最廣泛使用的長期趨勢分界線（200日均線）。
            #        做多需確認大趨勢向上（價格 > EMA200）；做空反之。
            # 容忍 ±0.5%：EMA200 剛穿越初期避免誤殺合理轉折訊號。
            c_ema200 = float(c_last['EMA200']) if pd.notna(c_last['EMA200']) else None
            if c_ema200 and c_ema200 > 0:
                if direction == "多" and current_price < c_ema200 * 0.995:
                    return None   # 大趨勢仍空頭，多單勝率大幅降低
                if direction == "空" and current_price > c_ema200 * 1.005:
                    return None   # 大趨勢仍多頭，空單勝率大幅降低

            # ── 技術強度基礎評分（ADX 趨勢強度 + RSI 進場位置，有實際 TA 依據）──
            #
            # ADX 元件（0-65）：衡量趨勢有多強，已硬過濾 <20
            if   current_adx >= 50: adx_score = 65
            elif current_adx >= 40: adx_score = 57   # 與 MIN_ADX=40 廣播門檻對齊
            elif current_adx >= 30: adx_score = 35
            else:                   adx_score = 20   # 20-30 弱趨勢
            #
            # RSI 元件（0-40）：進場時動能健不健康（避免追高殺低）
            if direction == "多":
                if   45 <= current_rsi <= 65: rsi_score = 40   # 理想：有動能且未超買
                elif 35 <= current_rsi < 45 or 65 < current_rsi <= 72: rsi_score = 25
                else:                          rsi_score = 10   # >72 超買 / <35 動能缺乏
            else:
                if   35 <= current_rsi <= 55: rsi_score = 40   # 理想：有動能且未超賣
                elif 28 <= current_rsi < 35 or 55 < current_rsi <= 65: rsi_score = 25
                else:                          rsi_score = 10   # <28 超賣 / >65 過高
            base_score = adx_score + rsi_score

            # ③ 成交量加分（軟性：量大加分，量小扣分）
            if cross_vol > avg_vol_20 * 1.5:
                vol_bonus = 8    # 放量突破
            elif cross_vol > avg_vol_20 * 0.8:
                vol_bonus = 4    # 正常量
            elif cross_vol > avg_vol_20 * 0.4:
                vol_bonus = -8   # 偏低量，扣分降低勝率顯示
            else:
                vol_bonus = 0    # 已在前面硬擋，不會到這裡

            # ── MACD 方向確認評分（Strategy 3 多因子共振核心指標）──
            # 金叉/死叉 = MACD 剛穿越 Signal 線（最強動能訊號）
            # 同向 = MACD 在 Signal 正確側（多頭在上 / 空頭在下）
            # 逆向 = MACD 方向反向（扣分，不硬擋——ADX / EMA200 等指標仍可綜合判斷）
            _macd_now  = float(c_last['MACD'])     if pd.notna(c_last['MACD'])     else 0.0
            _macd_sig  = float(c_last['MACD_SIG']) if pd.notna(c_last['MACD_SIG']) else 0.0
            _macd_prev = float(df.iloc[-2]['MACD'])     if len(df) >= 2 and pd.notna(df.iloc[-2]['MACD'])     else _macd_now
            _msig_prev = float(df.iloc[-2]['MACD_SIG']) if len(df) >= 2 and pd.notna(df.iloc[-2]['MACD_SIG']) else _macd_sig
            _macd_cross_up   = (_macd_prev <= _msig_prev) and (_macd_now > _macd_sig)
            _macd_cross_down = (_macd_prev >= _msig_prev) and (_macd_now < _macd_sig)
            _macd_agree = (direction == "多" and _macd_now > _macd_sig) or \
                          (direction == "空" and _macd_now < _macd_sig)
            if (direction == "多" and _macd_cross_up) or (direction == "空" and _macd_cross_down):
                _macd_bonus = 18   # 剛金叉/死叉：最強動能共振
            elif _macd_agree:
                _macd_bonus = 8    # MACD 同向確認
            else:
                _macd_bonus = -15  # MACD 逆向：明顯扣分

            # ── 主力動向 + HTF 對齊：並行發出，節省 ~200-400ms ──
            htf_bar   = _TF_HTF.get(tf, "1D")
            htf_label = _TF_LABEL.get(htf_bar.lower(), htf_bar)
            with ThreadPoolExecutor(max_workers=2) as _iex:
                _f_sent  = _iex.submit(get_market_sentiment, asset)
                _f_align = _iex.submit(get_higher_tf_alignment, asset, direction, htf_bar)
            try:
                funding_rate, ls_ratio = _f_sent.result(timeout=5)
            except Exception:
                funding_rate, ls_ratio = 0.0, 1.0
            try:
                aligned, htf_adx = _f_align.result(timeout=5)
            except Exception:
                aligned, htf_adx = True, 30.0  # 取得失敗時不懲罰

            # ──────────────────────────────────────────────────
            # ④ 大週期趨勢硬性門檻（順趨勢做單核心邏輯）
            #
            # 規則 A：HTF 方向必須與訊號同向
            #   若 4H（或 1D）MA8/EMA89 方向相反 → 訊號是在賭反轉點，直接丟棄。
            #   例：4H 空頭 + 1H 多叉 = 1H 死貓彈，不做。
            #
            # 規則 B：HTF ADX 必須達門檻（大週期有方向性）
            #   15m/30m → HTF=1H，BTC 從震盪突破時 1H ADX 常在 18-24，
            #              門檻 ≥ 25 會把所有初段突破訊號都攔掉；降至 20 兼顧初段與防噪。
            #   1h+    → HTF=4H/1D，等待更強確認，維持 ≥ 25。
            # ──────────────────────────────────────────────────
            # 15m/30m → HTF=1H：原門檻 20，提高至 23。
            # 1H ADX 20-22 屬於極弱趨勢，此時 15m 的交叉極易是假突破。
            # 1h+ → HTF=4H/1D：維持 25（業界標準）。
            _htf_adx_min = 23 if tf in ("15m", "30m") else 25
            if not aligned:
                return None   # 逆 HTF 趨勢，硬擋
            if htf_adx < _htf_adx_min:
                return None   # HTF 盤整期，交叉訊號可信度極低，硬擋

            # HTF 對齊確認 → 固定加分（已成為進場前提，不再是「可選加分項」）
            tf_note  = f" ✅{htf_bar}(ADX={htf_adx:.0f})"

            # ⑤ 生態鏈參考幣作為風險指標（軟性評分，不硬擋——個幣主力策略優先）
            #   查 _CHAIN_REF 找到此幣所屬的鏈（如 AAVE→ETH，RAY→SOL，未知→BTC）
            #   參考鏈逆向 = 生態系統整體偏弱，扣分；同向 = 小幅加分
            symbol   = asset.split('-')[0]
            ref_coin = _CHAIN_REF.get(symbol, "BTC")
            _rt      = (ref_trends or {}).get(ref_coin, "neutral")

            btc_bonus = 0
            btc_risk_note = ""
            if _rt == "bear" and direction == "多":
                btc_bonus     = -12
                btc_risk_note = f" ⚠️{ref_coin}偏空"
            elif _rt == "bull" and direction == "空":
                btc_bonus     = -12
                btc_risk_note = f" ⚠️{ref_coin}偏多"
            elif _rt == "bull" and direction == "多":
                btc_bonus     = +5
            elif _rt == "bear" and direction == "空":
                btc_bonus     = +5

            # ⑥ 全市場資金費率過熱（軟性扣分，保留彈性）
            fr_bonus = 0
            if market_fr > 0.0005 and direction == "多":
                fr_bonus = -10   # 多頭過熱
            elif market_fr < -0.0003 and direction == "空":
                fr_bonus = -10   # 空頭過熱

            sentiment_note, sentiment_bonus = build_sentiment_note(direction, funding_rate, ls_ratio)

            # 把 BTC 風險提示附加到時框標籤，讓訊號訊息直接可見
            tf_note = tf_note + btc_risk_note

            score = base_score + sentiment_bonus + vol_bonus + btc_bonus + fr_bonus + _macd_bonus

            win_rate = score_to_win_rate(score)
            leverage = score_to_leverage(win_rate, max_leverage)

            _pb_tag = "↩回踩" if _is_pullback else ""
            if tf == "15m":
                order_type = "超短線單"
                tf_tag = f"15M超短線{_pb_tag}{tf_note}"
            elif tf == "30m":
                order_type = "半時線單"
                tf_tag = f"30M半時{_pb_tag}{tf_note}"
            elif tf == "1h":
                order_type = "短線單"
                tf_tag = f"1H短線{_pb_tag}{tf_note}"
            elif tf == "4h":
                order_type = "長線單"
                tf_tag = f"4H長線{_pb_tag}{tf_note}"
            else:
                order_type = "日線單"
                tf_tag = f"1D日線{_pb_tag}{tf_note}"

            # ── 進場錨定點（Strategy 1 突破回踩：用阻力位；MA 交叉：用 MA8）──
            # 突破回踩策略：進場點 = 阻力位（被突破後成為支撐），SL 放在該位下方。
            # 此設定讓 SL 更緊（阻力位比 MA8 更精確），盈虧比也更好。
            if _is_pullback and _pb_level is not None:
                entry_price  = _pb_level
                anchor_label = f"突破支撐={format_price(_pb_level)}"
            else:
                entry_price  = current_ma8
                anchor_label = f"MA8={format_price(current_ma8)}"

            # ⑦ SL：從 K 線擺動結構推導（entry 後方最近支撐/壓力，跌破訊號失效）
            sl_price, _ms_tp1, _ms_tp2, _ms_tp3 = find_market_structure_levels(
                df, entry_price, direction, current_atr)

            # ── TP：優先使用 Fibonacci 擴展線（1.272 / 1.618 / 2.0）──
            # 擺動結構 TP 目標往往是「最近壓力位」（0.5-1R），盈虧比偏低。
            # Fib 擴展線從最近 A→B 擺動推算，自然落在 1.272R / 1.618R 以上，
            # 符合「突破回踩」後的真實市場延伸目標。
            _fib_ext = get_fibonacci_extension(df, entry_price, direction, sl_price)
            _tp_source = ""
            if _fib_ext is not None:
                tp1, tp2, tp3, _fib_rng = _fib_ext
                _tp_source = f"📐Fib擴展(1.272/1.618/2.0)"
            else:
                # Fib 擴展計算失敗（擺動點不足），降回擺動結構 TP
                tp1, tp2, tp3 = _ms_tp1, _ms_tp2, _ms_tp3
                _tp_source = "📊擺動結構"
                # 保留舊版 R:R 硬門檻（Fib 無法計算時的安全網）
                _risk_dist = abs(entry_price - sl_price)
                _tp1_dist  = abs(tp1 - entry_price)
                if _tp1_dist < _risk_dist * 1.5:
                    _tp2_dist = abs(tp2 - entry_price)
                    if _tp2_dist >= _risk_dist * 1.5:
                        _ext = abs(tp3 - tp2) if abs(tp3 - tp2) > 0 else _risk_dist
                        if direction == "多":
                            tp1, tp2, tp3 = tp2, tp3, tp3 + _ext
                        else:
                            tp1, tp2, tp3 = tp2, tp3, tp3 - _ext
                    else:
                        return None   # 擺動結構也無足夠 R:R，拒絕

            # ── K 線型態確認（釘頭錘 / 吞沒形態）──
            # 偵測最近 3 根 K 棒，有確認型態則加分 + 附加標籤到訊號訊息
            _candle_pat, _candle_bonus = detect_candle_pattern(df, direction, n_bars=3)
            if _candle_bonus > 0:
                score += _candle_bonus
            _candle_label = f" | {_candle_pat}" if _candle_pat else ""

            # ── 斐波那契回撤匯流：判斷進場點是否在 Fib 關鍵回撤位 ──
            _fib_lbl, _fib_px, _fib_near, _fib_dist = get_fibonacci_context(
                df, entry_price, direction)
            if _fib_near and _fib_lbl in ('0.382', '0.618'):
                _fib_bonus = 8   # 最強 Fib 水位匯流
            elif _fib_near and _fib_lbl == '0.500':
                _fib_bonus = 5   # 0.5 中位支撐
            elif _fib_near:
                _fib_bonus = 2   # 其他 Fib 水位附近
            else:
                _fib_bonus = 0
            score    += _fib_bonus
            win_rate  = score_to_win_rate(score)
            leverage  = score_to_leverage(win_rate, max_leverage)

            # ── POC 匯流確認（成交量最大聚集點，進場點越靠近 POC 品質越高）──
            _poc_price = compute_poc(df)
            _poc_bonus, _poc_label = poc_check(_poc_price, entry_price, current_atr)
            score    += _poc_bonus
            win_rate  = score_to_win_rate(score)
            leverage  = score_to_leverage(win_rate, max_leverage)

            # ── FVG 公平價值缺口匯流確認 ──
            _fvg_lo, _fvg_hi = detect_fvg(df, entry_price, direction)
            _fvg_bonus, _fvg_label = fvg_check(_fvg_lo, _fvg_hi, entry_price)
            score    += _fvg_bonus
            win_rate  = score_to_win_rate(score)
            leverage  = score_to_leverage(win_rate, max_leverage)

            # ── 動態 TP 數量：依時框與訊號強度決定幾個止盈目標 ──
            # 短週期訊號（15m/30m）動能衰減快，保守取利；長週期訊號視強度決定
            if tf in ("15m", "30m"):
                tp_count = 2 if score >= 75 else 1
            elif tf == "1h":
                if score >= 80:    tp_count = 3
                elif score >= 65:  tp_count = 2
                else:              tp_count = 1
            else:  # 4h, 1d
                if score >= 80:    tp_count = 3
                elif score >= 68:  tp_count = 2
                else:              tp_count = 1

            # ── 手續費最低保本距離：TP1 < 0.25% 時「TP1後移至開倉止損」必虧 ──
            _fee_min = entry_price * 0.0025
            if direction == "多":
                tp1 = max(tp1, round(entry_price + _fee_min, 8))
                tp2 = max(tp2, round(tp1 + _fee_min, 8))
                tp3 = max(tp3, round(tp2 + _fee_min, 8))
            else:
                tp1 = min(tp1, round(entry_price - _fee_min, 8))
                tp2 = min(tp2, round(tp1 - _fee_min, 8))
                tp3 = min(tp3, round(tp2 - _fee_min, 8))

            # ── 進場建議文字（依現價與進場點的偏離程度給出建議）──
            # is_market_entry = True 表示可直接市價進場（現價已貼近或已超過進場點）
            if direction == "多":
                gap_pct = (current_price - entry_price) / entry_price * 100
                if current_rsi > 65:
                    entry_type = f"⚠️ {tf_tag}RSI過熱({current_rsi:.1f})，宜等回踩支撐後再掛限價 {anchor_label}"
                    is_market_entry = False
                elif current_price < entry_price:
                    entry_type = (f"🔴 {tf_tag}現價 {format_price(current_price)} 低於 {anchor_label}，"
                                  f"偏離 {abs(gap_pct):.2f}%｜確認大週期趨勢仍多，可市價進場")
                    is_market_entry = True   # 現價已超過進場點，視為可立即市價成交
                elif gap_pct <= 0.5:
                    entry_type = (f"⚡ {tf_tag}現價緊貼 {anchor_label}（差 {gap_pct:.2f}%），"
                                  f"建議<b>市價進場</b>或快速掛限價 {format_price(entry_price)}")
                    is_market_entry = True
                else:
                    entry_type = f"📌 {tf_tag}掛限價 {anchor_label}，止損={format_price(sl_price)}"
                    is_market_entry = False
            else:
                gap_pct = (entry_price - current_price) / entry_price * 100
                if current_rsi < 35:
                    entry_type = f"⚠️ {tf_tag}RSI超賣({current_rsi:.1f})，宜等反彈至壓力後再掛限價 {anchor_label}"
                    is_market_entry = False
                elif current_price > entry_price:
                    entry_type = (f"🔴 {tf_tag}現價 {format_price(current_price)} 高於 {anchor_label}，"
                                  f"偏離 {abs(gap_pct):.2f}%｜確認大週期趨勢仍空，可市價進場")
                    is_market_entry = True   # 現價已超過進場點，視為可立即市價成交
                elif gap_pct <= 0.5:
                    entry_type = (f"⚡ {tf_tag}現價緊貼 {anchor_label}（差 {gap_pct:.2f}%），"
                                  f"建議<b>市價進場</b>或快速掛限價 {format_price(entry_price)}")
                    is_market_entry = True
                else:
                    entry_type = f"📌 {tf_tag}掛限價 {anchor_label}，止損={format_price(sl_price)}"
                    is_market_entry = False

            # 將 POC / FVG / K 線型態 / TP 來源附加到進場建議說明尾端
            if _poc_label:
                entry_type += f"  |  {_poc_label}"
            if _fvg_label:
                entry_type += f"  |  {_fvg_label}"
            if _candle_label:
                entry_type += _candle_label
            entry_type += f"  |  {_tp_source}"

            entry_price = round_to_tick(entry_price, asset)
            sl_price    = round_to_tick(sl_price,    asset)
            tp1         = round_to_tick(tp1,         asset)
            tp2         = round_to_tick(tp2,         asset)
            tp3         = round_to_tick(tp3,         asset)
            return {
                "asset":          asset.split('-')[0],
                "dir":            direction,
                "leverage":       leverage,
                "win_rate":       win_rate,
                "tf":             tf_tag,
                "order_type":     order_type,
                "score":          score,
                "entry":          entry_price,
                "sl":             sl_price,
                "tp1":            tp1,
                "tp2":            tp2,
                "tp3":            tp3,
                "entry_type":     entry_type,
                "sentiment_note": sentiment_note,
                "ls_ratio":       ls_ratio,
                "adx":            round(current_adx, 1),
                "vol_confirmed":  cross_vol > avg_vol_20,
                "tf_note":        tf_note,
                "entry_fr":       round(funding_rate * 100, 4),
                "fib_level":        _fib_lbl,
                "fib_price":        _fib_px,
                "fib_near":         _fib_near,
                "fib_dist":         _fib_dist,
                "poc_price":        _poc_price,
                "poc_label":        _poc_label,
                "fvg_lo":           _fvg_lo,
                "fvg_hi":           _fvg_hi,
                "fvg_label":        _fvg_label,
                "candle_pattern":   _candle_pat,
                "tp_source":        _tp_source,
                "tp_count":         tp_count,
                "signal_type":      "trend",
                "atr_trail":        round(float(current_atr) * 2.5, 8),  # TP3 移動止盈距離 = ATR×2.5
                "price":            current_price,
                "is_market_entry":  is_market_entry,
            }
    except:
        pass
    return None

def fetch_trend_state(inst_id, tf):
    """
    永遠回傳當前趨勢狀態（不需要交叉事件）。
    用於自選監控：即使 MA8/EMA89 已交叉好幾天，仍能看到趨勢資訊。
    回傳 dict 或 None（API 失敗時）
    """
    bar_param = _TF_BAR.get(tf, "1H")
    url = f"{BASE_URL}/api/v5/market/candles?instId={inst_id}&bar={bar_param}&limit=100"
    try:
        res = requests.get(url, timeout=3.0).json()
        if res.get('code') != '0' or len(res['data']) < 30:
            return None
        df = pd.DataFrame(res['data'], columns=['ts','open','high','low','close','vol','volCcy','volCcyQuote','state'])
        for col in ['open','high','low','close','vol']:
            df[col] = df[col].astype(float)
        df = df.iloc[::-1].reset_index(drop=True)
        df['MA8']   = df['close'].rolling(8).mean()
        df['EMA89'] = df['close'].ewm(span=89, adjust=False).mean()
        delta = df['close'].diff()
        gain  = delta.where(delta > 0, 0).rolling(14).mean()
        loss  = (-delta.where(delta < 0, 0)).rolling(14).mean()
        df['RSI'] = 100 - (100 / (1 + gain / (loss + 1e-10)))
        df['H-L']  = df['high'] - df['low']
        df['H-PC'] = (df['high'] - df['close'].shift(1)).abs()
        df['L-PC'] = (df['low']  - df['close'].shift(1)).abs()
        df['TR']   = df[['H-L','H-PC','L-PC']].max(axis=1)
        df['ATR14']= df['TR'].rolling(14).mean()
        plus_dm  = df['high'].diff().clip(lower=0)
        minus_dm = (-df['low'].diff()).clip(lower=0)
        mask = plus_dm >= minus_dm
        plus_dm_clean  = plus_dm.where(mask, 0)
        minus_dm_clean = minus_dm.where(~mask, 0)
        atr14_s  = df['ATR14'] + 1e-10
        plus_di  = 100 * plus_dm_clean.rolling(14).mean() / atr14_s
        minus_di = 100 * minus_dm_clean.rolling(14).mean() / atr14_s
        dx  = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di + 1e-10)
        df['ADX'] = dx.rolling(14).mean()

        c = df.iloc[-1]
        avg_vol = df['vol'].iloc[-22:-2].mean()

        ma8       = c['MA8']
        ema89     = c['EMA89']
        price     = c['close']
        rsi       = c['RSI']
        adx       = c['ADX']
        atr       = c['ATR14']
        vol_pct   = round(c['vol'] / avg_vol * 100)
        ema_slope = c['EMA89'] - df['EMA89'].iloc[-4]

        direction  = "多" if ma8 > ema89 else "空"
        gap_pct    = abs(ma8 - ema89) / ema89 * 100

        # 判斷是否近期出現過交叉（8根內）
        recent_cross = False
        for i in range(-9, -1):
            prev_r = df.iloc[i]; curr_r = df.iloc[i + 1]
            if (prev_r['MA8'] <= prev_r['EMA89'] and curr_r['MA8'] > curr_r['EMA89']) or \
               (prev_r['MA8'] >= prev_r['EMA89'] and curr_r['MA8'] < curr_r['EMA89']):
                recent_cross = True
                break

        # 條件評分（同 fetch_coin_status）
        adx_ok   = adx >= 20
        vol_ok   = c['vol'] > avg_vol
        slope_ok = (direction == "多" and ema_slope > 0) or (direction == "空" and ema_slope < 0)
        price_5ago = df['close'].iloc[-6]
        rsi_5ago   = df['RSI'].iloc[-6]
        no_div = not (
            (direction == "多" and price > price_5ago and rsi < rsi_5ago) or
            (direction == "空" and price < price_5ago and rsi > rsi_5ago)
        )
        passed = sum([adx_ok, vol_ok, slope_ok, no_div])

        # ── 進場 / 止損 / 止盈：從 K 線擺動結構推導，非固定百分比 ──
        # SL  = entry 下方（多）/ 上方（空）最近擺動點（支撐/壓力），跌破代表訊號失效
        # TP1 = entry 方向第一個壓力/支撐擺動點
        # TP2/TP3 = 再往後的歷史水位，找不到時以 ATR 補全
        entry = ma8
        sl, tp1, tp2, tp3 = find_market_structure_levels(df, entry, direction, atr)

        # 趨勢延續：MA8 > EMA89 已多根，價格是否回踩 MA8 附近（再入場機會）
        pullback_pct = abs(price - ma8) / ma8 * 100
        is_pullback = (pullback_pct <= 1.5 and adx >= 20)  # 回踩 MA8 1.5% 以內

        # 斐波那契匯流
        _fib_lbl, _fib_px, _fib_near, _fib_dist = get_fibonacci_context(df, entry, direction)

        entry = round_to_tick(entry, inst_id)
        sl    = round_to_tick(sl,    inst_id)
        tp1   = round_to_tick(tp1,   inst_id)
        tp2   = round_to_tick(tp2,   inst_id)
        tp3   = round_to_tick(tp3,   inst_id)
        return {
            'asset':        inst_id.split('-')[0],
            'dir':          direction,
            'tf':           "1H" if tf == "1h" else "4H",
            'price':        price,
            'ma8':          round(ma8, 6),
            'ema89':        round(ema89, 6),
            'rsi':          round(rsi, 1),
            'adx':          round(adx, 1),
            'vol_pct':      vol_pct,
            'gap_pct':      round(gap_pct, 2),
            'passed':       passed,
            'filters':      {'adx_ok': adx_ok, 'vol_ok': vol_ok,
                             'slope_ok': slope_ok, 'no_div': no_div},
            'entry':        entry,
            'sl':           sl,
            'tp1':          tp1,
            'tp2':          tp2,
            'tp3':          tp3,
            'recent_cross': recent_cross,
            'is_pullback':  is_pullback,
            'pullback_pct': round(pullback_pct, 2),
            'crossed':      recent_cross,
            'fib_level':    _fib_lbl,
            'fib_price':    _fib_px,
            'fib_near':     _fib_near,
            'fib_dist':     _fib_dist,
        }
    except Exception as e:
        print(f"⚠️ fetch_trend_state({inst_id}) 失敗：{e}")
        return None


def fetch_range_signal(asset, tf, max_leverage=20, ref_trends=None, market_fr=0.0):
    """
    盤整區間反彈策略（ADX 15-28）。
    觸及確認支撐帶 + RSI ≤ 45 + 下影線 → 做多；
    觸及確認壓力帶 + RSI ≥ 55 + 上影線 → 做空。
    SL/TP 以區間兩端為自然目標，不用 ATR 倍數。
    """
    bar_param = _TF_BAR.get(tf, "1H")
    url = (f"{BASE_URL}/api/v5/market/candles"
           f"?instId={asset}&bar={bar_param}&limit=100")
    try:
        res = requests.get(url, timeout=2.0).json()
        if res.get('code') != '0' or len(res.get('data', [])) < 50:
            return None

        df = pd.DataFrame(res['data'],
                          columns=['ts','open','high','low','close',
                                   'vol','volCcy','volCcyQuote','state'])
        for col in ['open','high','low','close','vol']:
            df[col] = df[col].astype(float)
        df = df.iloc[::-1].reset_index(drop=True)

        # 計算指標
        df['MA8']   = df['close'].rolling(8).mean()
        df['EMA89'] = df['close'].ewm(span=89, adjust=False).mean()
        delta = df['close'].diff()
        gain  = delta.where(delta > 0, 0).rolling(14).mean()
        loss  = (-delta.where(delta < 0, 0)).rolling(14).mean()
        df['RSI'] = 100 - (100 / (1 + gain / (loss + 1e-10)))
        df['H_L']  = df['high'] - df['low']
        df['H_PC'] = (df['high'] - df['close'].shift(1)).abs()
        df['L_PC'] = (df['low']  - df['close'].shift(1)).abs()
        df['TR']   = df[['H_L','H_PC','L_PC']].max(axis=1)
        df['ATR']  = df['TR'].rolling(14).mean()
        pdm = df['high'].diff().clip(lower=0)
        mdm = (-df['low'].diff()).clip(lower=0)
        mask = pdm >= mdm
        _pdm = pdm.where(mask, 0)
        _mdm = mdm.where(~mask, 0)
        atr_s = df['ATR'] + 1e-10
        plus_di  = 100 * _pdm.rolling(14).mean() / atr_s
        minus_di = 100 * _mdm.rolling(14).mean() / atr_s
        dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di + 1e-10)
        df['ADX'] = dx.rolling(14).mean()

        c      = df.iloc[-1]
        c_prev = df.iloc[-2]   # 最後一根已完全收盤的 K 棒

        price   = float(c['close'])
        adx     = float(c['ADX'])
        rsi     = float(c['RSI'])
        atr     = float(c['ATR'])
        avg_vol = float(df['vol'].iloc[-22:-2].mean())

        # ① ADX 必須在盤整範圍（15-28）；太亂 < 15 不做，趨勢 > 28 改走趨勢策略
        if adx < 15 or adx > 28:
            return None

        # ② 識別區間邊界（需多次觸及確認）
        sup_lo, sup_hi, res_lo, res_hi = find_range_bounds(df)
        if sup_lo is None:
            return None

        # ③ 區間寬度需有意義：至少 2×ATR，避免極窄震盪帶
        if (res_lo - sup_hi) < atr * 2:
            return None

        # ④ 偵測前一根 K 棒的影線（已收盤，確認拒絕）
        o_prev = float(c_prev['open'])
        cl_prev = float(c_prev['close'])
        h_prev = float(c_prev['high'])
        l_prev = float(c_prev['low'])
        body      = abs(cl_prev - o_prev)
        upper_wk  = h_prev - max(cl_prev, o_prev)
        lower_wk  = min(cl_prev, o_prev) - l_prev
        min_body  = atr * 0.05  # 避免 body=0 除零

        direction = None

        # 做多條件：現價在支撐帶附近 + RSI ≤ 45 + 前一棒下影線 ≥ 1.2× 實體
        near_sup = sup_lo <= price <= sup_hi * 1.008
        if near_sup and rsi <= 45 and lower_wk >= max(body, min_body) * 1.2:
            direction = "多"

        # 做空條件：現價在壓力帶附近 + RSI ≥ 55 + 前一棒上影線 ≥ 1.2× 實體
        near_res = res_lo * 0.992 <= price <= res_hi
        if near_res and rsi >= 55 and upper_wk >= max(body, min_body) * 1.2 and direction is None:
            direction = "空"

        if direction is None:
            return None

        # ④-B 鏈參考方向過濾（區間策略補充）
        # 區間反彈訊號若逆大市方向，被反向趨勢打穿支撐/壓力的機率大幅提升。
        # 強烈逆向（ref_coin 明確 bear/bull 且與訊號方向相反）→ 直接丟棄。
        _sym_r = asset.split('-')[0]
        _ref_r = _CHAIN_REF.get(_sym_r, "BTC")
        _rt_r  = (ref_trends or {}).get(_ref_r, "neutral")
        if _rt_r == "bear" and direction == "多":
            return None   # 區間多單 + 鏈參考幣空頭 → 支撐極易被打穿，跳過
        if _rt_r == "bull" and direction == "空":
            return None   # 區間空單 + 鏈參考幣多頭 → 壓力極易被突破，跳過

        # ⑤ 成交量確認：反彈應量縮，放量 > 1.8× 均量代表可能突破，跳過
        if float(c['vol']) > avg_vol * 1.8:
            return None

        # ⑥ 確認邊界未被有效突破（收盤未站穩區間外）
        if direction == "多" and price < sup_lo * 0.997:
            return None
        if direction == "空" and price > res_hi * 1.003:
            return None

        # SL / TP（以區間兩端為自然目標）
        midpoint = (sup_hi + res_lo) / 2
        if direction == "多":
            sl_price = sup_lo * 0.997
            tp1      = midpoint
            tp2      = res_lo
            tp3      = res_hi
        else:
            sl_price = res_hi * 1.003
            tp1      = midpoint
            tp2      = sup_hi
            tp3      = sup_lo

        # ── 盈虧比保護：區間策略最低 1.2:1 ──
        # 區間單進場在邊界，TP1=中線，TP2=對側，天然結構比趨勢更緊。
        # 若區間太窄（TP1 < 1.2× 風險），TP1 後保本幾乎沒有獲利可言。
        _risk_r   = abs(price - sl_price)
        _reward_r = abs(tp1 - price)
        if _risk_r > 0 and _reward_r < _risk_r * 1.2:
            return None   # 區間太窄 R:R 不達標，跳過

        # 區間策略評分（與趨勢策略評分完全獨立）
        if direction == "多":
            rsi_score  = max(0, 45 - rsi) * 2          # RSI 越低越理想（最高 ~90分）
        else:
            rsi_score  = max(0, rsi - 55) * 2

        wick_ratio = (lower_wk if direction == "多" else upper_wk) / max(body, min_body)
        wick_score = min(20, wick_ratio * 8)            # 影線越長越確認拒絕

        width_score = min(15, ((res_lo - sup_hi) / atr) * 3)  # 區間越寬 TP 空間越大

        raw = rsi_score + wick_score + width_score

        # 映射成技術分百分比（區間策略上限 80，保留與趨勢策略的差距）
        win_rate = min(80, max(0, int(raw * 0.8)))
        if win_rate < 60:
            return None

        leverage = score_to_leverage(win_rate, max_leverage, signal_type='range')

        # ── POC 匯流確認 ──
        _poc_price_r = compute_poc(df)
        _poc_bonus_r, _poc_label_r = poc_check(_poc_price_r, price, atr)
        raw      += _poc_bonus_r
        win_rate  = min(80, max(0, int(raw * 0.8)))
        leverage  = score_to_leverage(win_rate, max_leverage, signal_type='range')

        # ── FVG 公平價值缺口匯流確認 ──
        _fvg_lo_r, _fvg_hi_r = detect_fvg(df, price, direction)
        _fvg_bonus_r, _fvg_label_r = fvg_check(_fvg_lo_r, _fvg_hi_r, price)
        raw      += _fvg_bonus_r
        win_rate  = min(80, max(0, int(raw * 0.8)))
        leverage  = score_to_leverage(win_rate, max_leverage, signal_type='range')

        tf_label = _TF_LABEL.get(tf, tf.upper())
        dir_tag  = "支撐做多" if direction == "多" else "壓力做空"
        tf_tag   = f"{tf_label}區間{dir_tag}"

        if direction == "多":
            entry_desc = (f"⚡ {tf_tag}  現價 {format_price(price)} 觸及支撐帶"
                          f"[{format_price(sup_lo)}–{format_price(sup_hi)}]，"
                          f"影線拒絕 RSI {rsi:.0f}，"
                          f"止損 {format_price(sl_price)}，TP {format_price(tp1)} / {format_price(tp2)}")
        else:
            entry_desc = (f"⚡ {tf_tag}  現價 {format_price(price)} 觸及壓力帶"
                          f"[{format_price(res_lo)}–{format_price(res_hi)}]，"
                          f"影線拒絕 RSI {rsi:.0f}，"
                          f"止損 {format_price(sl_price)}，TP {format_price(tp1)} / {format_price(tp2)}")
        if _poc_label_r:
            entry_desc += f"  |  {_poc_label_r}"
        if _fvg_label_r:
            entry_desc += f"  |  {_fvg_label_r}"

        # 主力動向（輕量取得，用於推播顯示完整度）
        try:
            funding_rate_r, ls_ratio_r = get_market_sentiment(asset)
        except Exception:
            funding_rate_r, ls_ratio_r = 0.0, 1.0
        sentiment_note_r, _ = build_sentiment_note(direction, funding_rate_r, ls_ratio_r)

        return {
            "asset":          asset.split('-')[0],
            "dir":            direction,
            "leverage":       leverage,
            "win_rate":       win_rate,
            "tf":             tf_tag,
            "order_type":     "區間反彈單",
            "score":          raw,
            "entry":          price,
            "sl":             sl_price,
            "tp1":            tp1,
            "tp2":            tp2,
            "tp3":            tp3,
            "adx":            round(adx, 1),
            "rsi":            round(rsi, 1),
            "entry_type":     entry_desc,
            # 空單：signal_price 略高於 entry → 突破空模式（等低點跌至 entry 填單）
            # 多單：signal_price 略低於 entry → 突破多模式（等高點升至 entry 填單）
            "signal_price":   price * 1.0002 if direction == "空" else price * 0.9998,
            "signal_type":    "range",
            "range_sup":      [sup_lo, sup_hi],
            "range_res":      [res_lo, res_hi],
            "sentiment_note": sentiment_note_r,
            "ls_ratio":       ls_ratio_r,
            "entry_fr":       round(funding_rate_r * 100, 4),
            "vol_confirmed":  True,   # 已確認量縮（非放量突破）
            "tf_note":        f"支撐[{format_price(sup_lo)}–{format_price(sup_hi)}]→壓力[{format_price(res_lo)}–{format_price(res_hi)}]",
            "poc_price":      _poc_price_r,
            "poc_label":      _poc_label_r,
            "fvg_lo":         _fvg_lo_r,
            "fvg_hi":         _fvg_hi_r,
            "fvg_label":      _fvg_label_r,
            "atr_trail":      round(float(atr) * 2.5, 8),
        }
    except Exception:
        return None


def fetch_divergence_signal(asset, tf, max_leverage=20, ref_trends=None, market_fr=0.0):
    """
    量價背離轉折訊號（ADX 20-40，趨勢末段捕捉反轉）。

    頂背離（做空）：近期出現更高高點，但 RSI 創低 + 量縮 + 前一棒上影線
    底背離（做多）：近期出現更低低點，但 RSI 創高 + 量縮 + 前一棒下影線

    進場時機比 MA 交叉早 3-8 根，止損以最近結構高/低點為準。
    """
    bar_param = _TF_BAR.get(tf, "1H")
    url = (f"{BASE_URL}/api/v5/market/candles"
           f"?instId={asset}&bar={bar_param}&limit=100")
    try:
        res = requests.get(url, timeout=2.0).json()
        if res.get('code') != '0' or len(res.get('data', [])) < 60:
            return None

        df = pd.DataFrame(res['data'],
                          columns=['ts','open','high','low','close',
                                   'vol','volCcy','volCcyQuote','state'])
        for col in ['open','high','low','close','vol']:
            df[col] = df[col].astype(float)
        df = df.iloc[::-1].reset_index(drop=True)

        # 指標計算
        df['EMA89'] = df['close'].ewm(span=89, adjust=False).mean()
        df['MA8']   = df['close'].rolling(8).mean()
        delta = df['close'].diff()
        gain  = delta.where(delta > 0, 0).rolling(14).mean()
        loss  = (-delta.where(delta < 0, 0)).rolling(14).mean()
        df['RSI'] = 100 - (100 / (1 + gain / (loss + 1e-10)))
        df['H_L']  = df['high'] - df['low']
        df['H_PC'] = (df['high'] - df['close'].shift(1)).abs()
        df['L_PC'] = (df['low']  - df['close'].shift(1)).abs()
        df['TR']   = df[['H_L','H_PC','L_PC']].max(axis=1)
        df['ATR']  = df['TR'].rolling(14).mean()
        pdm = df['high'].diff().clip(lower=0)
        mdm = (-df['low'].diff()).clip(lower=0)
        mask = pdm >= mdm
        _pdm = pdm.where(mask, 0)
        _mdm = mdm.where(~mask, 0)
        atr_s = df['ATR'] + 1e-10
        plus_di  = 100 * _pdm.rolling(14).mean() / atr_s
        minus_di = 100 * _mdm.rolling(14).mean() / atr_s
        dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di + 1e-10)
        df['ADX'] = dx.rolling(14).mean()

        c      = df.iloc[-1]
        c_prev = df.iloc[-2]   # 最後一根已收盤 K 棒

        price   = float(c['close'])
        adx     = float(c['ADX'])
        rsi     = float(c['RSI'])
        atr     = float(c['ATR'])
        ema89   = float(c['EMA89'])
        avg_vol = float(df['vol'].iloc[-22:-2].mean())

        # ① ADX 20-40：趨勢末段；太弱（盤整）由區間策略處理，太強（趨勢中）由趨勢策略處理
        if adx < 20 or adx > 40:
            return None

        # ② 找最近兩個擺動高點 / 兩個擺動低點（在最近 40 根裡，n=2 側翼確認）
        n = 2
        lookback_start = max(0, len(df) - 42)
        highs_arr = df['high'].values
        lows_arr  = df['low'].values
        rsis_arr  = df['RSI'].values
        vols_arr  = df['vol'].values

        swing_highs_list = []   # [(idx, price, rsi, vol)]
        swing_lows_list  = []
        for i in range(lookback_start + n, len(df) - n - 1):
            if highs_arr[i] == max(highs_arr[i-n:i+n+1]):
                swing_highs_list.append((i, float(highs_arr[i]),
                                         float(rsis_arr[i]), float(vols_arr[i])))
            if lows_arr[i] == min(lows_arr[i-n:i+n+1]):
                swing_lows_list.append((i, float(lows_arr[i]),
                                        float(rsis_arr[i]), float(vols_arr[i])))

        # 取最近兩個擺動點
        sh = swing_highs_list[-2:] if len(swing_highs_list) >= 2 else []
        sl_pts = swing_lows_list[-2:] if len(swing_lows_list) >= 2 else []

        direction = None
        div_desc  = ""

        # ③-A 頂背離：新高 > 舊高，但 RSI 新高 < RSI 舊高，且量縮
        if sh:
            old_h, new_h = sh[0], sh[1]
            price_higher = new_h[1] > old_h[1] * 1.002          # 價格確實更高
            rsi_lower    = new_h[2] < old_h[2] - 2.0            # RSI 卻更低（背離至少 2pt）
            vol_shrink   = new_h[3] < old_h[3] * 0.92           # 量縮 8% 以上
            if price_higher and rsi_lower and vol_shrink:
                direction = "空"
                div_desc  = (f"頂背離：前高 {old_h[1]:.4g}(RSI {old_h[2]:.0f}) → "
                             f"新高 {new_h[1]:.4g}(RSI {new_h[2]:.0f})，量縮 {(1-new_h[3]/old_h[3])*100:.0f}%")

        # ③-B 底背離：新低 < 舊低，但 RSI 新低 > RSI 舊低，且量縮
        if sl_pts and direction is None:
            old_l, new_l = sl_pts[0], sl_pts[1]
            price_lower  = new_l[1] < old_l[1] * 0.998
            rsi_higher   = new_l[2] > old_l[2] + 2.0
            vol_shrink   = new_l[3] < old_l[3] * 0.92
            if price_lower and rsi_higher and vol_shrink:
                direction = "多"
                div_desc  = (f"底背離：前低 {old_l[1]:.4g}(RSI {old_l[2]:.0f}) → "
                             f"新低 {new_l[1]:.4g}(RSI {new_l[2]:.0f})，量縮 {(1-new_l[3]/old_l[3])*100:.0f}%")

        if direction is None:
            return None

        # ③-C 鏈參考方向過濾（背離策略補充）
        # 背離是逆趨勢反轉訊號，若大市環境方向強烈一致，背離失敗率大幅上升。
        # BTC/鏈參考幣方向強烈相反 → 背離反彈空間有限，跳過。
        _sym_d = asset.split('-')[0]
        _ref_d = _CHAIN_REF.get(_sym_d, "BTC")
        _rt_d  = (ref_trends or {}).get(_ref_d, "neutral")
        if _rt_d == "bear" and direction == "多":
            return None   # 底背離多單 + 鏈幣空頭 → 大跌趨勢中假反彈機率高
        if _rt_d == "bull" and direction == "空":
            return None   # 頂背離空單 + 鏈幣多頭 → 強多趨勢中假頂機率高

        # ④ 前一根 K 棒需有拒絕影線（確認賣/買盤確實退縮）
        o_p  = float(c_prev['open'])
        cl_p = float(c_prev['close'])
        h_p  = float(c_prev['high'])
        l_p  = float(c_prev['low'])
        body     = abs(cl_p - o_p)
        upper_wk = h_p - max(cl_p, o_p)
        lower_wk = min(cl_p, o_p) - l_p
        min_body = atr * 0.05

        if direction == "空" and upper_wk < max(body, min_body) * 0.8:
            return None   # 頂背離需有上影線
        if direction == "多" and lower_wk < max(body, min_body) * 0.8:
            return None   # 底背離需有下影線

        # ⑤ 成交量確認：當前量不應放量（放量 = 突破，不是反轉）
        if float(c['vol']) > avg_vol * 1.8:
            return None

        # ⑥ SL/TP 使用 K 線結構水位
        sl_price, tp1, tp2, tp3 = find_market_structure_levels(df, price, direction, atr)

        # ── 盈虧比強制門檻：背離策略最低 1.5:1 ──
        # 背離是逆勢進場，止損通常放在結構高/低點之上，距離較大。
        # TP1 若不足 1.5R，整筆交易期望值接近負值。
        # 同樣策略：先嘗試升格 TP2；仍不足則拒絕。
        _risk_d = abs(price - sl_price)
        _tp1_d  = abs(tp1 - price)
        if _tp1_d < _risk_d * 1.5:
            _tp2_d = abs(tp2 - price)
            if _tp2_d >= _risk_d * 1.5:
                _ext_d = abs(tp3 - tp2) if abs(tp3 - tp2) > 0 else _risk_d
                if direction == "多":
                    tp1, tp2, tp3 = tp2, tp3, tp3 + _ext_d
                else:
                    tp1, tp2, tp3 = tp2, tp3, tp3 - _ext_d
            else:
                return None   # 背離訊號 R:R 不足，拒絕

        # ── 評分 ──
        # RSI 背離幅度（越大越確信）
        if direction == "空" and sh:
            rsi_gap = old_h[2] - new_h[2]     # 舊高RSI - 新高RSI，越正越好
        elif direction == "多" and sl_pts:
            rsi_gap = new_l[2] - old_l[2]     # 新低RSI - 舊低RSI，越正越好
        else:
            rsi_gap = 0
        rsi_score = min(40, rsi_gap * 2)

        # 影線分數
        wick = (upper_wk if direction == "空" else lower_wk)
        wick_score = min(20, (wick / max(body, min_body)) * 8)

        # RSI 極端位置加分（背離時 RSI 已在極端區間更可靠）
        if direction == "空":
            extreme_score = max(0, rsi - 60) * 0.8
        else:
            extreme_score = max(0, 40 - rsi) * 0.8

        raw = rsi_score + wick_score + extreme_score

        # 映射（背離策略上限 82）
        win_rate = min(82, max(0, int(raw * 0.9)))
        if win_rate < 62:
            return None

        leverage = score_to_leverage(win_rate, max_leverage, signal_type='divergence')

        # ── POC 匯流確認 ──
        _poc_price_d = compute_poc(df)
        _poc_bonus_d, _poc_label_d = poc_check(_poc_price_d, price, atr)
        raw      += _poc_bonus_d
        win_rate  = min(82, max(0, int(raw * 0.9)))
        leverage  = score_to_leverage(win_rate, max_leverage, signal_type='divergence')

        # ── FVG 公平價值缺口匯流確認 ──
        _fvg_lo_d, _fvg_hi_d = detect_fvg(df, price, direction)
        _fvg_bonus_d, _fvg_label_d = fvg_check(_fvg_lo_d, _fvg_hi_d, price)
        raw      += _fvg_bonus_d
        win_rate  = min(82, max(0, int(raw * 0.9)))
        leverage  = score_to_leverage(win_rate, max_leverage, signal_type='divergence')

        tf_label = _TF_LABEL.get(tf, tf.upper())
        dir_tag  = "頂背離做空" if direction == "空" else "底背離做多"
        tf_tag   = f"{tf_label}{dir_tag}"

        entry_desc = (f"⚡ {tf_tag}  {div_desc}  |  "
                      f"現價 {format_price(price)}  RSI {rsi:.0f}  ADX {adx:.0f}  "
                      f"止損 {format_price(sl_price)}  TP {format_price(tp1)} / {format_price(tp2)}")
        if _poc_label_d:
            entry_desc += f"  |  {_poc_label_d}"
        if _fvg_label_d:
            entry_desc += f"  |  {_fvg_label_d}"

        # 主力動向（用於推播顯示完整度）
        try:
            funding_rate_d, ls_ratio_d = get_market_sentiment(asset)
        except Exception:
            funding_rate_d, ls_ratio_d = 0.0, 1.0
        sentiment_note_d, _ = build_sentiment_note(direction, funding_rate_d, ls_ratio_d)

        return {
            "asset":          asset.split('-')[0],
            "dir":            direction,
            "leverage":       leverage,
            "win_rate":       win_rate,
            "tf":             tf_tag,
            "order_type":     "背離轉折單",
            "score":          raw,
            "entry":          price,
            "sl":             sl_price,
            "tp1":            tp1,
            "tp2":            tp2,
            "tp3":            tp3,
            "adx":            round(adx, 1),
            "rsi":            round(rsi, 1),
            "entry_type":     entry_desc,
            # 空單：signal_price 略高於 entry → 突破空模式（等低點跌至 entry 填單）
            # 多單：signal_price 略低於 entry → 突破多模式（等高點升至 entry 填單）
            "signal_price":   price * 1.0002 if direction == "空" else price * 0.9998,
            "signal_type":    "divergence",
            "div_desc":       div_desc,
            "sentiment_note": sentiment_note_d,
            "ls_ratio":       ls_ratio_d,
            "entry_fr":       round(funding_rate_d * 100, 4),
            "vol_confirmed":  True,
            "tf_note":        div_desc,   # 背離描述作為 tf_note 備用
            "poc_price":      _poc_price_d,
            "poc_label":      _poc_label_d,
            "fvg_lo":         _fvg_lo_d,
            "fvg_hi":         _fvg_hi_d,
            "fvg_label":      _fvg_label_d,
            "atr_trail":      round(float(atr) * 2.5, 8),
        }
    except Exception:
        return None


def fetch_near_miss_candidate(asset, tf, btc_trend="neutral", market_fr=0.0):
    """同 fetch_candle_sync 但追蹤各過濾器結果，回傳接近通過的訊號資訊"""
    bar_param = _TF_BAR.get(tf, "1H")
    url = f"{BASE_URL}/api/v5/market/candles?instId={asset}&bar={bar_param}&limit=100"
    try:
        res = requests.get(url, timeout=1.5).json()
        if res.get('code') != '0' or len(res['data']) < 90:
            return None
        df = pd.DataFrame(res['data'], columns=['ts','open','high','low','close','vol','volCcy','volCcyQuote','state'])
        for col in ['open','high','low','close','vol']:
            df[col] = df[col].astype(float)
        df = df.iloc[::-1].reset_index(drop=True)
        df['MA8']   = df['close'].rolling(8).mean()
        df['EMA89'] = df['close'].ewm(span=89, adjust=False).mean()
        delta = df['close'].diff()
        gain  = delta.where(delta > 0, 0).rolling(14).mean()
        loss  = (-delta.where(delta < 0, 0)).rolling(14).mean()
        df['RSI'] = 100 - (100 / (1 + gain / (loss + 1e-10)))
        df['H-L']  = df['high'] - df['low']
        df['H-PC'] = (df['high'] - df['close'].shift(1)).abs()
        df['L-PC'] = (df['low']  - df['close'].shift(1)).abs()
        df['TR']   = df[['H-L','H-PC','L-PC']].max(axis=1)
        df['ATR14']= df['TR'].rolling(14).mean()
        plus_dm  = df['high'].diff().clip(lower=0)
        minus_dm = (-df['low'].diff()).clip(lower=0)
        mask = plus_dm >= minus_dm
        plus_dm_clean  = plus_dm.where(mask, 0)
        minus_dm_clean = minus_dm.where(~mask, 0)
        atr14_s = df['ATR14'] + 1e-10
        plus_di  = 100 * plus_dm_clean.rolling(14).mean() / atr14_s
        minus_di = 100 * minus_dm_clean.rolling(14).mean() / atr14_s
        dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di + 1e-10)
        df['ADX'] = dx.rolling(14).mean()

        c_last = df.iloc[-1]

        # ── 近期交叉偵測（最近 8 根 K 棒內曾發生交叉）──
        direction = None
        cross_bar_idx = None
        for i in range(-9, -1):           # 檢查最近 8 根
            prev = df.iloc[i]
            curr = df.iloc[i + 1]
            if prev['MA8'] <= prev['EMA89'] and curr['MA8'] > curr['EMA89']:
                direction     = "多"
                cross_bar_idx = i + 1
                break
            if prev['MA8'] >= prev['EMA89'] and curr['MA8'] < curr['EMA89']:
                direction     = "空"
                cross_bar_idx = i + 1
                break

        # ── 即將交叉偵測（MA8 距 EMA89 在 1.5% 以內且朝正確方向靠近）──
        approaching_note = None
        if direction is None:
            ma8_now  = c_last['MA8']
            ema_now  = c_last['EMA89']
            ma8_prev = df.iloc[-2]['MA8']
            ema_prev = df.iloc[-2]['EMA89']
            gap_pct  = abs(ma8_now - ema_now) / (ema_now + 1e-10) * 100
            if gap_pct <= 1.5:
                if ma8_prev < ema_prev and ma8_now < ema_now:   # 多頭蓄勢
                    direction      = "多"
                    approaching_note = f"MA8趨近EMA89（差{gap_pct:.2f}%）"
                elif ma8_prev > ema_prev and ma8_now > ema_now:  # 空頭蓄勢
                    direction      = "空"
                    approaching_note = f"MA8趨近EMA89（差{gap_pct:.2f}%）"

        if direction is None:
            return None  # 沒有交叉也沒有接近，不算接近訊號

        current_price = c_last['close']
        current_rsi   = c_last['RSI']
        current_adx   = c_last['ADX']
        avg_vol_20    = df['vol'].iloc[-22:-2].mean()
        # 成交量取交叉那根（或最新根）
        vol_idx       = cross_bar_idx if cross_bar_idx is not None else -1
        cross_vol     = df['vol'].iloc[vol_idx]
        ema89_slope   = c_last['EMA89'] - df['EMA89'].iloc[-4]
        price_5ago    = df['close'].iloc[-6]
        rsi_5ago      = df['RSI'].iloc[-6]

        filters_passed = 0
        failed_at = None

        # ① ADX
        if current_adx < 20:
            failed_at = f"ADX偏弱（{current_adx:.0f}）"
        else:
            filters_passed += 1
            # ② 成交量
            if cross_vol <= avg_vol_20:
                failed_at = "成交量不足"
            else:
                filters_passed += 1
                # ③ EMA89 斜率
                slope_fail = (direction == "多" and ema89_slope <= 0) or (direction == "空" and ema89_slope >= 0)
                if slope_fail:
                    failed_at = "EMA89斜率偏平"
                else:
                    filters_passed += 1
                    # ④ RSI 背離
                    rsi_div = (direction == "多" and current_price > price_5ago and current_rsi < rsi_5ago) or \
                              (direction == "空" and current_price < price_5ago and current_rsi > rsi_5ago)
                    if rsi_div:
                        failed_at = "RSI背離"
                    else:
                        filters_passed += 1
                        # 全部通過
                        if approaching_note:
                            # 即將交叉且所有過濾器就緒 → 蓄勢接近訊號
                            failed_at = approaching_note
                        else:
                            # 已交叉且全部通過 → 正式訊號，不算接近訊號
                            return None

        # 至少通過 1 道才值得顯示（接近訊號門檻較低）
        if failed_at and filters_passed >= 1:
            current_atr  = c_last['ATR14']
            current_ma8  = c_last['MA8']
            current_ema89= c_last['EMA89']
            if tf == "15m":
                anchor_entry = current_ma8
                anchor_label = f"MA8={format_price(current_ma8)}"
                atr_mult  = 1.2
                tp_mults  = (1.0, 2.0, 3.5)
            elif tf == "30m":
                anchor_entry = current_ma8
                anchor_label = f"MA8={format_price(current_ma8)}"
                atr_mult  = 1.3
                tp_mults  = (1.2, 2.5, 4.0)
            elif tf == "1h":
                anchor_entry = current_ma8
                anchor_label = f"MA8={format_price(current_ma8)}"
                atr_mult  = 1.5
                tp_mults  = (1.5, 3.0, 5.0)
            elif tf == "4h":
                anchor_entry = current_ema89
                anchor_label = f"EMA89={format_price(current_ema89)}"
                atr_mult  = 2.0
                tp_mults  = (2.0, 4.0, 7.0)
            else:  # 1d
                anchor_entry = current_ema89
                anchor_label = f"EMA89={format_price(current_ema89)}"
                atr_mult  = 3.0
                tp_mults  = (3.0, 6.0, 10.0)
            tf_label = _TF_LABEL.get(tf, tf.upper())
            # 接近訊號也使用市場結構推導 SL/TP，確保水位落在真實支撐/壓力位
            sl_price, tp1, tp2, tp3 = find_market_structure_levels(
                df, anchor_entry, direction, current_atr)
            anchor_entry = round_to_tick(anchor_entry, asset)
            sl_price     = round_to_tick(sl_price,     asset)
            tp1          = round_to_tick(tp1,          asset)
            tp2          = round_to_tick(tp2,          asset)
            tp3          = round_to_tick(tp3,          asset)
            return {
                'asset':          asset.split('-')[0],
                'dir':            direction,
                'tf':             tf_label,
                'filters_passed': filters_passed,
                'failed_at':      failed_at,
                'rsi':            round(current_rsi, 1),
                'adx':            round(current_adx, 1),
                'entry':          anchor_entry,
                'entry_label':    anchor_label,
                'sl':             sl_price,
                'tp1':            tp1,
                'tp2':            tp2,
                'tp3':            tp3,
            }
    except Exception:
        pass
    return None


def fetch_smc_signal(asset, tf, max_leverage=20, ref_trends=None, market_fr=0.0):
    """
    Strategy 2: SMC / ICT 流動性獵取策略。

    多頭三步：
    1. 流動性獵取（Liquidity Sweep）：K 棒下破前低，下影線長，快速收回 = 止損收割完畢
    2. 結構轉換（CHoCH）：隨後 K 棒收盤突破最近局部高點，確認反轉
    3. 回踩進場：現價回踩訂單塊（OB）或公允價值缺口（FVG），或 Fib 38.2%-78.6%

    空頭三步：對稱反向。

    SL = 流動性獵取最低點（掃高最高點）之外 0.3%
    TP = Fib 擴展 1.272 / 1.618 / 2.0
    """
    bar_param = _TF_BAR.get(tf, "1H")
    url = f"{BASE_URL}/api/v5/market/candles?instId={asset}&bar={bar_param}&limit=200"
    try:
        res = requests.get(url, timeout=2.5).json()
        if res.get('code') != '0' or len(res['data']) < 60:
            return None
        df = pd.DataFrame(res['data'],
                          columns=['ts','open','high','low','close','vol',
                                   'volCcy','volCcyQuote','state'])
        for col in ['open','high','low','close','vol']:
            df[col] = df[col].astype(float)
        df = df.iloc[::-1].reset_index(drop=True)

        # ── 指標 ──
        df['EMA200'] = df['close'].ewm(span=200, adjust=False).mean()
        delta = df['close'].diff()
        gain  = delta.where(delta > 0, 0).rolling(14).mean()
        loss  = (-delta.where(delta < 0, 0)).rolling(14).mean()
        df['RSI'] = 100 - (100 / (1 + gain / (loss + 1e-10)))
        df['H-L']  = df['high'] - df['low']
        df['H-PC'] = (df['high'] - df['close'].shift(1)).abs()
        df['L-PC'] = (df['low']  - df['close'].shift(1)).abs()
        df['TR']   = df[['H-L','H-PC','L-PC']].max(axis=1)
        df['ATR14']= df['TR'].rolling(14).mean()
        pdm  = df['high'].diff().clip(lower=0)
        mdm  = (-df['low'].diff()).clip(lower=0)
        mask = pdm >= mdm
        pdm_c = pdm.where(mask, 0); mdm_c = mdm.where(~mask, 0)
        atr14 = df['ATR14'] + 1e-10
        plus_di  = 100 * pdm_c.rolling(14).mean() / atr14
        minus_di = 100 * mdm_c.rolling(14).mean() / atr14
        dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di + 1e-10)
        df['ADX'] = dx.rolling(14).mean()
        ema12 = df['close'].ewm(span=12, adjust=False).mean()
        ema26 = df['close'].ewm(span=26, adjust=False).mean()
        df['MACD']     = ema12 - ema26
        df['MACD_SIG'] = df['MACD'].ewm(span=9, adjust=False).mean()

        if len(df) < 40:
            return None

        c_last        = df.iloc[-1]
        current_price = float(c_last['close'])
        current_atr   = float(c_last['ATR14'])   if pd.notna(c_last['ATR14'])   else current_price * 0.01
        current_adx   = float(c_last['ADX'])     if pd.notna(c_last['ADX'])     else 0.0
        current_rsi   = float(c_last['RSI'])     if pd.notna(c_last['RSI'])     else 50.0
        c_ema200      = float(c_last['EMA200'])  if pd.notna(c_last['EMA200'])  else 0.0
        macd_now      = float(c_last['MACD'])    if pd.notna(c_last['MACD'])    else 0.0
        macd_sig      = float(c_last['MACD_SIG'])if pd.notna(c_last['MACD_SIG'])else 0.0

        tf_label = _TF_LABEL.get(tf, tf)
        scan_window = min(30, len(df) - 10)   # 掃描最近 30 根

        # ══════════════════════════════════════════════════════════════
        # 多頭 SMC 掃描
        # ══════════════════════════════════════════════════════════════
        bull_sweep_i   = None
        bull_sweep_low = None

        for _i in range(3, scan_window + 1):
            if _i + 5 >= len(df):
                break
            bar = df.iloc[-_i]
            # 前低 = 此根之前 scan_window 根的最低點
            _ref_end   = len(df) - _i
            _ref_start = max(0, _ref_end - scan_window)
            prev_low = float(df['low'].iloc[_ref_start:_ref_end].min())
            if prev_low <= 0:
                continue
            # 流動性獵取：跌破前低 ≥ 0.1%，但快速收回前低上方
            if float(bar['low']) < prev_low * 0.999 and float(bar['close']) > prev_low * 0.998:
                _body       = abs(float(bar['close']) - float(bar['open']))
                _lower_wick = min(float(bar['open']), float(bar['close'])) - float(bar['low'])
                # 下影線需 ≥ 1.2× 實體 或 ≥ 0.3× ATR（防止普通陰線誤判）
                if _lower_wick >= _body * 1.2 or _lower_wick >= current_atr * 0.3:
                    bull_sweep_i   = _i
                    bull_sweep_low = float(bar['low'])
                    break

        if bull_sweep_i is not None:
            # 掃描前的局部高點（CHoCH 參照：最近 10 根內最高 high）
            _win_s = min(bull_sweep_i + 10, len(df) - 1)
            _win_e = bull_sweep_i
            _pre_high = float(df['high'].iloc[len(df) - _win_s:len(df) - _win_e].max()) \
                        if _win_s > _win_e else current_price * 1.01

            # CHoCH 確認：掃描後（更近的 K 棒）有 close 突破 _pre_high
            _choch_ok = any(
                float(df.iloc[-_j]['close']) > _pre_high
                for _j in range(1, bull_sweep_i)
                if _j < len(df)
            )

            if _choch_ok and current_price < _pre_high:   # 目前仍在 CHoCH 水位下方 = 回踩中
                # 訂單塊（OB）= 掃描前最後一根陰線
                _ob_lo = _ob_hi = None
                for _k in range(bull_sweep_i, min(bull_sweep_i + 15, len(df) - 1)):
                    _bk = df.iloc[-_k]
                    if float(_bk['close']) < float(_bk['open']):
                        _ob_lo = float(min(_bk['open'], _bk['close']))
                        _ob_hi = float(max(_bk['open'], _bk['close']))
                        break

                # FVG（看多缺口：bar-1 low > bar+1 high）
                _fvg_lo = _fvg_hi = None
                for _f in range(2, min(bull_sweep_i + 8, len(df) - 2)):
                    _b1 = df.iloc[-(_f + 1)]
                    _b3 = df.iloc[-(_f - 1)]
                    if float(_b1['low']) > float(_b3['high']):
                        _fvg_lo = float(_b3['high'])
                        _fvg_hi = float(_b1['low'])
                        break

                # Fib 回調區間（38.2%–78.6%）
                _range   = _pre_high - bull_sweep_low
                _ret_lo  = bull_sweep_low + _range * 0.382
                _ret_hi  = bull_sweep_low + _range * 0.786

                _in_ob  = bool(_ob_lo and _ob_lo * 0.995 <= current_price <= _ob_hi * 1.005)
                _in_fvg = bool(_fvg_lo and _fvg_lo * 0.995 <= current_price <= _fvg_hi * 1.005)
                _in_ret = bool(_ret_lo <= current_price <= _ret_hi)

                if (_in_ob or _in_fvg or _in_ret) and \
                   (c_ema200 <= 0 or current_price >= c_ema200 * 0.995):
                    # ── 評分 ──
                    sc = 0
                    if current_adx >= 40: sc += 47
                    elif current_adx >= 30: sc += 35
                    elif current_adx >= 20: sc += 20
                    else: sc += 5
                    if 30 <= current_rsi <= 50: sc += 30
                    elif current_rsi < 30: sc += 18
                    elif 50 < current_rsi <= 65: sc += 10
                    if _in_ob:  sc += 15
                    if _in_fvg: sc += 12
                    if _in_ret: sc += 8
                    if macd_now > macd_sig: sc += 10
                    if c_ema200 > 0 and current_price > c_ema200: sc += 8
                    win_rate = score_to_win_rate(sc)
                    leverage = score_to_leverage(win_rate, max_leverage)

                    # ── SL / TP ──
                    sl_price    = round_to_tick(bull_sweep_low * 0.997, asset)
                    entry_price = round_to_tick(current_price, asset)
                    _fib_ext = get_fibonacci_extension(df, float(entry_price), "多", float(sl_price))
                    if _fib_ext:
                        tp1, tp2, tp3, _ = _fib_ext
                    else:
                        _r = float(entry_price) - float(sl_price)
                        tp1 = entry_price + _r * 1.5
                        tp2 = entry_price + _r * 2.5
                        tp3 = entry_price + _r * 3.5
                    # R:R 最低 1.5
                    if abs(float(tp1) - float(entry_price)) < abs(float(entry_price) - float(sl_price)) * 1.5:
                        _r = abs(float(entry_price) - float(sl_price))
                        tp1 = float(entry_price) + _r * 1.5
                        tp2 = float(entry_price) + _r * 2.5
                        tp3 = float(entry_price) + _r * 3.5
                    tp1 = round_to_tick(tp1, asset)
                    tp2 = round_to_tick(tp2, asset)
                    tp3 = round_to_tick(tp3, asset)

                    _loc = "OB回踩" if _in_ob else ("FVG回補" if _in_fvg else "Fib回調")
                    _fvg_label = f"FVG:{format_price(_fvg_lo)}-{format_price(_fvg_hi)}" if _fvg_lo else ""
                    return {
                        "asset": asset.split('-')[0], "dir": "多",
                        "leverage": leverage, "win_rate": win_rate,
                        "tf": f"{tf_label}SMC", "order_type": "SMC流動性單",
                        "score": sc, "entry": entry_price,
                        "sl": sl_price, "tp1": tp1, "tp2": tp2, "tp3": tp3,
                        "entry_type": f"🏦 {tf_label} SMC多｜掃低反轉｜{_loc}｜CHoCH確認",
                        "sentiment_note": "", "ls_ratio": 1.0,
                        "adx": round(current_adx, 1), "vol_confirmed": True,
                        "tf_note": "", "entry_fr": 0.0,
                        "fib_level": "", "fib_price": None, "fib_near": False, "fib_dist": None,
                        "poc_price": None, "poc_label": "",
                        "fvg_lo": _fvg_lo, "fvg_hi": _fvg_hi, "fvg_label": _fvg_label,
                        "candle_pattern": "", "tp_source": "📐Fib擴展" if _fib_ext else "📊ATR",
                        "tp_count": 3 if sc >= 65 else 2,
                        "signal_type": "smc",
                        "atr_trail": round(float(current_atr) * 2.5, 8),
                        "price": current_price, "is_market_entry": True,
                    }

        # ══════════════════════════════════════════════════════════════
        # 空頭 SMC 掃描（對稱邏輯）
        # ══════════════════════════════════════════════════════════════
        bear_sweep_i    = None
        bear_sweep_high = None

        for _i in range(3, scan_window + 1):
            if _i + 5 >= len(df):
                break
            bar = df.iloc[-_i]
            _ref_end   = len(df) - _i
            _ref_start = max(0, _ref_end - scan_window)
            prev_high = float(df['high'].iloc[_ref_start:_ref_end].max())
            if prev_high <= 0:
                continue
            # 流動性獵取：突破前高 ≥ 0.1%，但快速收回前高下方
            if float(bar['high']) > prev_high * 1.001 and float(bar['close']) < prev_high * 1.002:
                _body       = abs(float(bar['close']) - float(bar['open']))
                _upper_wick = float(bar['high']) - max(float(bar['open']), float(bar['close']))
                if _upper_wick >= _body * 1.2 or _upper_wick >= current_atr * 0.3:
                    bear_sweep_i    = _i
                    bear_sweep_high = float(bar['high'])
                    break

        if bear_sweep_i is not None:
            _win_s = min(bear_sweep_i + 10, len(df) - 1)
            _win_e = bear_sweep_i
            _pre_low = float(df['low'].iloc[len(df) - _win_s:len(df) - _win_e].min()) \
                       if _win_s > _win_e else current_price * 0.99

            _choch_ok = any(
                float(df.iloc[-_j]['close']) < _pre_low
                for _j in range(1, bear_sweep_i)
                if _j < len(df)
            )

            if _choch_ok and current_price > _pre_low:
                _ob_lo = _ob_hi = None
                for _k in range(bear_sweep_i, min(bear_sweep_i + 15, len(df) - 1)):
                    _bk = df.iloc[-_k]
                    if float(_bk['close']) > float(_bk['open']):
                        _ob_lo = float(min(_bk['open'], _bk['close']))
                        _ob_hi = float(max(_bk['open'], _bk['close']))
                        break

                _fvg_lo = _fvg_hi = None
                for _f in range(2, min(bear_sweep_i + 8, len(df) - 2)):
                    _b1 = df.iloc[-(_f + 1)]
                    _b3 = df.iloc[-(_f - 1)]
                    if float(_b3['low']) > float(_b1['high']):
                        _fvg_lo = float(_b1['high'])
                        _fvg_hi = float(_b3['low'])
                        break

                _range   = bear_sweep_high - _pre_low
                _ret_lo  = bear_sweep_high - _range * 0.786
                _ret_hi  = bear_sweep_high - _range * 0.382

                _in_ob  = bool(_ob_lo and _ob_lo * 0.995 <= current_price <= _ob_hi * 1.005)
                _in_fvg = bool(_fvg_lo and _fvg_lo * 0.995 <= current_price <= _fvg_hi * 1.005)
                _in_ret = bool(_ret_lo <= current_price <= _ret_hi)

                if (_in_ob or _in_fvg or _in_ret) and \
                   (c_ema200 <= 0 or current_price <= c_ema200 * 1.005):
                    sc = 0
                    if current_adx >= 40: sc += 47
                    elif current_adx >= 30: sc += 35
                    elif current_adx >= 20: sc += 20
                    else: sc += 5
                    if 50 <= current_rsi <= 70: sc += 30
                    elif current_rsi > 70: sc += 18
                    elif 35 <= current_rsi < 50: sc += 10
                    if _in_ob:  sc += 15
                    if _in_fvg: sc += 12
                    if _in_ret: sc += 8
                    if macd_now < macd_sig: sc += 10
                    if c_ema200 > 0 and current_price < c_ema200: sc += 8
                    win_rate = score_to_win_rate(sc)
                    leverage = score_to_leverage(win_rate, max_leverage)

                    sl_price    = round_to_tick(bear_sweep_high * 1.003, asset)
                    entry_price = round_to_tick(current_price, asset)
                    _fib_ext = get_fibonacci_extension(df, float(entry_price), "空", float(sl_price))
                    if _fib_ext:
                        tp1, tp2, tp3, _ = _fib_ext
                    else:
                        _r = float(sl_price) - float(entry_price)
                        tp1 = float(entry_price) - _r * 1.5
                        tp2 = float(entry_price) - _r * 2.5
                        tp3 = float(entry_price) - _r * 3.5
                    if abs(float(tp1) - float(entry_price)) < abs(float(entry_price) - float(sl_price)) * 1.5:
                        _r = abs(float(sl_price) - float(entry_price))
                        tp1 = float(entry_price) - _r * 1.5
                        tp2 = float(entry_price) - _r * 2.5
                        tp3 = float(entry_price) - _r * 3.5
                    tp1 = round_to_tick(tp1, asset)
                    tp2 = round_to_tick(tp2, asset)
                    tp3 = round_to_tick(tp3, asset)

                    _loc = "OB回踩" if _in_ob else ("FVG回補" if _in_fvg else "Fib回調")
                    _fvg_label = f"FVG:{format_price(_fvg_lo)}-{format_price(_fvg_hi)}" if _fvg_lo else ""
                    return {
                        "asset": asset.split('-')[0], "dir": "空",
                        "leverage": leverage, "win_rate": win_rate,
                        "tf": f"{tf_label}SMC", "order_type": "SMC流動性單",
                        "score": sc, "entry": entry_price,
                        "sl": sl_price, "tp1": tp1, "tp2": tp2, "tp3": tp3,
                        "entry_type": f"🏦 {tf_label} SMC空｜掃高反轉｜{_loc}｜CHoCH確認",
                        "sentiment_note": "", "ls_ratio": 1.0,
                        "adx": round(current_adx, 1), "vol_confirmed": True,
                        "tf_note": "", "entry_fr": 0.0,
                        "fib_level": "", "fib_price": None, "fib_near": False, "fib_dist": None,
                        "poc_price": None, "poc_label": "",
                        "fvg_lo": _fvg_lo, "fvg_hi": _fvg_hi, "fvg_label": _fvg_label,
                        "candle_pattern": "", "tp_source": "📐Fib擴展" if _fib_ext else "📊ATR",
                        "tp_count": 3 if sc >= 65 else 2,
                        "signal_type": "smc",
                        "atr_trail": round(float(current_atr) * 2.5, 8),
                        "price": current_price, "is_market_entry": True,
                    }
    except Exception:
        pass
    return None


def run_near_miss_scan():
    """掃描全市場接近通過訊號（交叉存在但被某過濾器擋下），回傳最多 5 筆"""
    all_assets, _ = get_all_okx_swap_assets()
    tasks    = [(a, tf) for a in all_assets for tf in ("15m", "30m", "1h", "4h", "1d")]
    results  = []
    lock     = threading.Lock()

    def task(asset, tf):
        r = fetch_near_miss_candidate(asset, tf)
        if r:
            with lock:
                results.append(r)

    with ThreadPoolExecutor(max_workers=80) as ex:
        list(ex.map(lambda t: task(*t), tasks))

    results.sort(key=lambda x: x['filters_passed'], reverse=True)
    return results[:3]


def run_strategy_scan():
    all_assets, leverage_map = get_all_okx_swap_assets()

    # 市場環境指標（只取一次，傳給所有 worker）
    # detect_market_regime 也一起並行，不額外增加等待時間
    print("📡 獲取市場環境指標...")
    with ThreadPoolExecutor(max_workers=3) as _env_ex:
        _f_ref    = _env_ex.submit(get_ecosystem_ref_trends)
        _f_fr     = _env_ex.submit(get_market_avg_funding_rate)
        _f_regime = _env_ex.submit(detect_market_regime)
    ref_trends     = _f_ref.result()
    market_fr      = _f_fr.result()
    market_regime  = _f_regime.result()   # 'trending' | 'ranging'
    print(f"   生態鏈趨勢: { {k: v for k, v in ref_trends.items()} } | 市場費率均值: {market_fr*100:.4f}%")

    trend_signals = []
    range_signals = []
    div_signals   = []
    smc_signals   = []
    tasks = [(asset, tf) for asset in all_assets for tf in ["15m", "30m", "1h", "4h", "1d"]]
    total = len(tasks)
    completed = 0
    lock = threading.Lock()

    scan_start = time.time()
    print(f"⏱️ 掃描開始時間：{datetime.datetime.now().strftime('%H:%M:%S')}（並行模式，共 {total} 項任務）")

    def scan_task(asset, tf):
        max_lev = leverage_map.get(asset, 20)
        results = []
        t = fetch_candle_sync(asset, tf, max_leverage=max_lev,
                              ref_trends=ref_trends, market_fr=market_fr)
        if t:
            results.append(('trend', t))
        r = fetch_range_signal(asset, tf, max_leverage=max_lev,
                               ref_trends=ref_trends, market_fr=market_fr)
        if r:
            results.append(('range', r))
        d = fetch_divergence_signal(asset, tf, max_leverage=max_lev,
                                    ref_trends=ref_trends, market_fr=market_fr)
        if d:
            results.append(('divergence', d))
        s = fetch_smc_signal(asset, tf, max_leverage=max_lev,
                             ref_trends=ref_trends, market_fr=market_fr)
        if s:
            results.append(('smc', s))
        return results

    with ThreadPoolExecutor(max_workers=80) as executor:
        futures = {executor.submit(scan_task, asset, tf): (asset, tf) for asset, tf in tasks}
        for future in as_completed(futures):
            with lock:
                completed += 1
                if completed % 100 == 0 or completed == total:
                    print(f"\r🔍 並行掃描進度：[{completed}/{total}]...", end="", flush=True)
            try:
                for sig_type, sig in (future.result() or []):
                    with lock:
                        if sig_type == 'trend':
                            trend_signals.append(sig)
                        elif sig_type == 'range':
                            range_signals.append(sig)
                        elif sig_type == 'smc':
                            smc_signals.append(sig)
                        else:
                            div_signals.append(sig)
            except Exception:
                pass

    elapsed = time.time() - scan_start
    print(f"\n✨ 全網掃描完畢！耗時：{elapsed:.1f} 秒（共 {len(all_assets)} 支幣種 × 5 時框）")

    # ── 趨勢訊號品質門檻（維持原有嚴格條件）──
    trend_signals.sort(key=lambda x: x['score'], reverse=True)
    trend_signals_raw = trend_signals[:]          # 篩選前原始清單（供大幣保底使用）
    trend_signals = [s for s in trend_signals
                     if s['win_rate'] >= MIN_WIN_RATE and s['adx'] >= MIN_ADX]

    # ── 區間訊號品質門檻（ADX 15-28，技術分 ≥ 60）──
    range_signals.sort(key=lambda x: x['score'], reverse=True)
    range_signals_raw = range_signals[:]
    range_signals = [s for s in range_signals if s['win_rate'] >= 60]

    # ── 背離訊號品質門檻（ADX 20-40，技術分 ≥ 62）──
    div_signals.sort(key=lambda x: x['score'], reverse=True)
    div_signals_raw = div_signals[:]
    div_signals = [s for s in div_signals if s['win_rate'] >= 62]

    # ── SMC 訊號品質門檻（技術分 ≥ 60，無 ADX 硬門檻——SMC 靠結構而非趨勢強度）──
    smc_signals.sort(key=lambda x: x['score'], reverse=True)
    smc_signals = [s for s in smc_signals if s['win_rate'] >= 60]

    print(f"   趨勢候選 {len(trend_signals)} 組 | 區間候選 {len(range_signals)} 組 | 背離候選 {len(div_signals)} 組 | SMC候選 {len(smc_signals)} 組")

    # ── 依市場階段動態調整策略優先順序 ──────────────────────────────────────
    # 趨勢市（ADX ≥ 22）：趨勢順勢突破策略命中率最高 → 趨勢前 2 優先
    # 震盪市（ADX < 22）：SMC 流動性獵取 + 區間反彈策略最有效 → SMC/區間優先
    if market_regime == 'ranging':
        # 震盪市：SMC×2 → 區間×1 → 趨勢×1 → 背離×1（最多 5 個）
        combined = smc_signals[:2] + range_signals[:1] + trend_signals[:1] + div_signals[:1]
        print(f"   🔄 震盪市模式：SMC/區間策略優先（SMC{len(smc_signals[:2])} + 區間{len(range_signals[:1])} + 趨勢{len(trend_signals[:1])} + 背離{len(div_signals[:1])}）")
    else:
        # 趨勢市（預設）：趨勢×2 → SMC×1 → 背離×1 → 區間×1（最多 5 個）
        combined = trend_signals[:2] + smc_signals[:1] + div_signals[:1] + range_signals[:1]
        print(f"   🚀 趨勢市模式：突破回踩策略優先（趨勢{len(trend_signals[:2])} + SMC{len(smc_signals[:1])} + 背離{len(div_signals[:1])} + 區間{len(range_signals[:1])}）")

    # ── 大幣保底名額（BTC / ETH 專屬第5槽，寬鬆門檻）──
    # BTC/ETH 走勢平滑，ADX 常在 30-38，被嚴格門檻直接淘汰。
    # 從篩選前原始清單中找大幣訊號（TA ≥ 78, ADX ≥ 30），補進獨立第5槽。
    # 只補「尚未在 combined 中的大幣」，且每次至多補 1 個（取分數最高者）。
    _MAJOR_COINS = {'BTC', 'ETH'}
    _in_combined  = {s['asset'] for s in combined}
    _missing_major = _MAJOR_COINS - _in_combined
    if _missing_major:
        _major_pool = [
            s for raw in (trend_signals_raw, div_signals_raw, range_signals_raw)
            for s in raw
            if s['asset'] in _missing_major
            and s['win_rate'] >= 70
            and s['adx'] >= 20
        ]
        # 同幣種只保留最高分
        _seen_maj = set()
        _major_dedup = []
        for _s in sorted(_major_pool, key=lambda x: x['score'], reverse=True):
            if _s['asset'] not in _seen_maj:
                _seen_maj.add(_s['asset'])
                _major_dedup.append(_s)
        if _major_dedup:
            combined = combined + [_major_dedup[0]]   # 最多補 1 個
            print(f"   🏦 大幣保底補入：{_major_dedup[0]['asset']} {_major_dedup[0]['dir']} "
                  f"TA={_major_dedup[0]['win_rate']} ADX={_major_dedup[0]['adx']:.1f}")

    with active_positions_lock:
        busy_pairs = {(p['asset'], p['dir']) for p in active_positions}
    with watch_list_lock:
        watch_assets = {w['asset'] for w in watch_list}

    # 去重①：同幣種同方向只保留第一個（趨勢優先，已排在前面）
    seen = set()
    deduped = []
    for s in combined:
        key = (s['asset'], s['dir'])
        if key not in seen:
            seen.add(key)
            deduped.append(s)

    # 去重②：板塊集中去重（防止同板塊集體拉升時倉位過度集中）
    # 原則：同一板塊（L1 / Meme_SOL / DeFi / AI 等）最多保留 2 個最強訊號。
    # 排序已按 score 由大到小，故直接依序保留，超過 2 個的略過並打印日誌。
    # 找不到板塊的幣種（未登錄在 _SECTOR_MAP）以幣種名稱自身為鍵，不受限制。
    _sector_count: dict[str, int] = {}
    sector_deduped = []
    for s in deduped:
        _sym    = s['asset']
        _sector = _SECTOR_MAP.get(_sym, _sym)   # 未知板塊 → 以自身名稱隔離，不被限制
        _sector_count[_sector] = _sector_count.get(_sector, 0) + 1
        if _sector_count[_sector] <= 2:
            sector_deduped.append(s)
        else:
            print(f"   🔇 板塊去重：{_sym}（{_sector} 板塊第 {_sector_count[_sector]} 個，略過）")
    if len(sector_deduped) < len(deduped):
        print(f"   📋 板塊去重後：{len(deduped)} → {len(sector_deduped)} 個訊號")
    deduped = sector_deduped

    now_ts = time.time()
    _MAJOR_COINS_COOL = {'BTC', 'ETH'}
    with recently_sent_lock:
        cooled_pairs = {k for k, ts in recently_sent_signals.items()
                        if now_ts - ts < (35 * 60 if k[0] in _MAJOR_COINS_COOL
                                          else SIGNAL_COOLDOWN_SECS)}

    top_signals = [
        s for s in deduped
        if (s['asset'], s['dir']) not in busy_pairs
        and s['asset'] not in watch_assets
        and (s['asset'], s['dir']) not in cooled_pairs
    ]

    excluded = len(deduped) - len(top_signals)
    cooled_out = len([s for s in deduped
                      if (s['asset'], s['dir']) in cooled_pairs])
    print(f"📊 掃描結果：趨勢 {len(trend_signals[:2])} + 區間 {len(range_signals[:1])} → "
          f"排除衝突 {excluded} 個（含冷卻 {cooled_out} 個）→ 推播 {len(top_signals)} 個")
    return top_signals

# ==================== 📊 5. 持倉監控系統 ====================
# Railway Volume 掛載在 /data，本機開發時退回當前目錄
_DATA_DIR = "/data" if os.path.isdir("/data") else "."
POSITIONS_FILE = os.path.join(_DATA_DIR, "active_positions.json")

def load_positions():
    """從 JSON 檔案讀取持倉記錄（Bot 啟動時呼叫）"""
    try:
        if os.path.exists(POSITIONS_FILE):
            with open(POSITIONS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            print(f"📂 已從檔案讀取 {len(data)} 筆持倉記錄")
            return data
    except Exception as e:
        print(f"⚠️ 讀取持倉檔案失敗：{e}")
    return []

def save_positions(positions):
    """將持倉記錄寫入 JSON 檔案（每次新增/移除時呼叫）"""
    try:
        with open(POSITIONS_FILE, "w", encoding="utf-8") as f:
            json.dump(positions, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"⚠️ 儲存持倉檔案失敗：{e}")

active_positions = load_positions()   # 啟動時從檔案恢復
active_positions_lock = threading.Lock()

# ==================== 📈 勝率統計系統 ====================
STATS_FILE = os.path.join(_DATA_DIR, "trade_stats.json")

def load_stats():
    try:
        if os.path.exists(STATS_FILE):
            with open(STATS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        print(f"⚠️ 讀取統計檔案失敗：{e}")
    return []

def save_stats(records):
    try:
        with open(STATS_FILE, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"⚠️ 儲存統計檔案失敗：{e}")

def record_trade_outcome(pos, outcome, exit_type=None):
    """
    記錄交易結果；outcome: win/loss
    exit_type: tp1/tp2/tp3/sl/breakeven/timeout/manual（None 時自動從 pos 推算）
    """
    records = load_stats()
    # 取進場時記錄的資金費率（若無則取當前費率）
    entry_fr = pos.get('entry_fr', None)
    if entry_fr is None:
        try:
            inst_id = pos['asset'] + "-USDT-SWAP"
            fr_res = requests.get(
                f"{BASE_URL}/api/v5/public/funding-rate?instId={inst_id}",
                timeout=3
            ).json()
            entry_fr = float(fr_res['data'][0]['fundingRate'])
        except Exception:
            entry_fr = 0.0

    # 推算出場類型（若未指定）
    if exit_type is None:
        if pos.get('tp3_hit'):
            exit_type = "tp3"
        elif pos.get('tp2_hit'):
            exit_type = "tp2"
        elif pos.get('tp1_hit'):
            exit_type = "tp1"
        elif outcome == 'loss':
            exit_type = "sl"
        else:
            exit_type = "unknown"

    # 用目標/止損價推算預估損益%（正數=獲利，負數=虧損）
    entry  = pos.get('entry') or 0
    pnl_pct = None
    if entry > 0:
        sign = 1 if pos.get('dir') == "多" else -1
        _tgt_map = {
            "tp3": pos.get('tp3'), "tp2": pos.get('tp2'),
            "tp1": pos.get('tp1'), "sl":  pos.get('sl'),
            "breakeven": entry,
        }
        _tgt = _tgt_map.get(exit_type)
        if _tgt:
            pnl_pct = round(sign * (_tgt - entry) / entry * 100, 3)

    records.append({
        "asset":       pos['asset'],
        "dir":         pos['dir'],
        "tf":          pos.get('tf', '—'),
        "signal_type": pos.get('signal_type', 'trend'),
        "entry":       entry or None,
        "sl":          pos.get('sl'),
        "tp1":         pos.get('tp1'),
        "adx":         pos.get('adx', 0),
        "score":       pos.get('score', 0),
        "win_rate":    pos.get('win_rate', 0),
        "entry_fr":    round(entry_fr * 100, 4),
        "exit_type":   exit_type,
        "pnl_pct":     pnl_pct,
        "outcome":     outcome,
        "timestamp":   time.time(),
        "open_time":   pos.get('timestamp', pos.get('signal_time', 0)),
    })
    save_stats(records)
    pnl_str = f"  P&L≈{pnl_pct:+.2f}%" if pnl_pct is not None else ""
    print(f"📊 記錄交易結果：{pos['asset']} {pos['dir']} → {outcome} ({exit_type}){pnl_str}  費率{entry_fr*100:.4f}%")

# 最近一次掃描結果快取（key = asset 如 "ETH"，value = signal dict）
# 供 /open 指令查詢，不自動加入持倉監控
last_scan_cache: dict = {}
last_scan_lock  = threading.Lock()
near_miss_cache: dict = {}
near_miss_lock  = threading.Lock()
# 推播冷卻緩存：同幣種同方向推出後 90 分鐘內不再重複推播
# key = (asset, dir)，value = 推播時間戳
recently_sent_signals: dict = {}
recently_sent_lock = threading.Lock()
SIGNAL_COOLDOWN_SECS = 90 * 60  # 90 分鐘
MIN_WIN_RATE = 90                     # 低於此勝率的訊號不推播
MIN_ADX      = 40                     # 只推播強勢趨勢（ADX ≥ 40）
WATCH_FILE = os.path.join(_DATA_DIR, "watch_list.json")

def load_watch_list():
    """Bot 啟動時從 JSON 檔案恢復自選監控清單"""
    try:
        if os.path.exists(WATCH_FILE):
            with open(WATCH_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            print(f"📂 已從檔案讀取 {len(data)} 筆自選監控記錄")
            return data
    except Exception as e:
        print(f"⚠️ 讀取自選監控檔案失敗：{e}")
    return []

def save_watch_list(wl):
    """每次新增/移除後寫入 JSON 檔案"""
    try:
        with open(WATCH_FILE, "w", encoding="utf-8") as f:
            json.dump(wl, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"⚠️ 儲存自選監控檔案失敗：{e}")

watch_list: list = load_watch_list()   # 啟動時從檔案恢復
watch_list_lock = threading.Lock()
WATCHLIST_INTERVAL_MINUTES = 5

def get_current_price(inst_id):
    try:
        url = f"{BASE_URL}/api/v5/market/ticker?instId={inst_id}"
        res = requests.get(url, timeout=3).json()
        if res.get('code') == '0':
            return float(res['data'][0]['last'])
    except:
        pass
    return None

def get_open_interest_change(inst_id):
    """回傳最近兩筆 OI 的變化率（正=OI增加=主力加倉，負=OI減少=主力撤退）"""
    try:
        url = f"{BASE_URL}/api/v5/rubik/stat/contracts/open-interest-volume?instId={inst_id}&period=1H"
        res = requests.get(url, timeout=3).json()
        if res.get('code') == '0' and len(res.get('data', [])) >= 2:
            oi_new = float(res['data'][0][1])
            oi_old = float(res['data'][1][1])
            if oi_old > 0:
                return (oi_new - oi_old) / oi_old * 100
    except:
        pass
    return 0.0

def get_volume_spike(inst_id):
    """比較最新 1H 成交量 vs 過去 5 根均值，>2倍視為異常量能"""
    try:
        url = f"{BASE_URL}/api/v5/market/candles?instId={inst_id}&bar=1H&limit=6"
        res = requests.get(url, timeout=3).json()
        if res.get('code') == '0' and len(res['data']) >= 6:
            vols = [float(r[5]) for r in res['data']]
            latest_vol = vols[0]
            avg_vol = sum(vols[1:]) / 5
            if avg_vol > 0:
                return latest_vol / avg_vol
    except:
        pass
    return 1.0

def evaluate_pending_order(pos):
    """分析掛單等待期間的訊號健康度，決定繼續等待或自動撤單。
    回傳 (summary: str, detail: str, auto_cancel: bool)
    auto_cancel=True → 條件已失效，主流程自動移除追蹤。
    """
    inst_id = pos['asset'] + '-USDT-SWAP'
    tf_raw  = pos.get('tf', '4H')

    # 正確對應時框
    if "15M" in tf_raw:
        tf_low = "15m"
    elif "1H" in tf_raw:
        tf_low = "1h"
    else:
        tf_low = "4h"

    # 寬限期：訊號剛建立時 MA8/EMA89 正在收斂，不做自動撤單評估
    # 15M → 30分鐘 | 1H → 60分鐘 | 4H → 120分鐘
    grace_sec = 1800 if "15M" in tf_raw else (3600 if "1H" in tf_raw else 7200)
    age_sec   = time.time() - pos.get('reported_at', time.time())
    if age_sec < grace_sec:
        remain = int((grace_sec - age_sec) / 60)
        return "✅ 繼續等待", f"訊號建立未滿寬限期，{remain}min 後開始評估", False

    st = fetch_coin_status(inst_id, tf_low)
    if not st:
        return "✅ 繼續等待", "無法取得市場資料，保持掛單", False

    direction  = pos['dir']
    hard_reasons = []
    soft_reasons = []

    sig_type = pos.get('signal_type', 'trend')

    # ── 硬性撤單條件 ──
    # 趨勢訊號：MA方向必須一致，否則趨勢已反轉
    # 區間/背離訊號：MA方向與訊號方向本就可能不一致，不能以此撤單
    if sig_type == 'trend' and st['dir'] != direction:
        hard_reasons.append(f"MA8/EMA89方向已反轉（訊號{direction}，現{st['dir']}）")

    if st['adx'] < 15:
        hard_reasons.append(f"ADX={st['adx']:.1f} 趨勢已完全消失")

    # 區間訊號：ADX 爆升超過35代表已突破區間，掛單失效
    if sig_type == 'range' and st['adx'] > 35:
        hard_reasons.append(f"ADX={st['adx']:.1f} 區間已被突破，不再適合區間反彈進場")

    # ── 軟性警訊（累積 ≥2 個 → 自動撤單）──
    # 僅趨勢訊號受 EMA89 斜率和量能影響；區間/背離訊號跳過
    if sig_type == 'trend':
        if 15 <= st['adx'] < 20:
            soft_reasons.append(f"ADX={st['adx']:.1f} 趨勢偏弱")

        if not st['filters']['slope_ok']:
            soft_reasons.append("EMA89斜率不利")

        if not st['filters']['vol_ok']:
            soft_reasons.append(f"成交量萎縮至均量{st['vol_pct']}%")

    # RSI背離：趨勢訊號視為警訊；背離訊號這本是觸發原因，無需檢查；區間訊號亦跳過
    if sig_type == 'trend' and not st['filters']['no_div']:
        soft_reasons.append("RSI背離出現")

    try:
        btc_trend = get_btc_trend()
        if (direction == "多" and btc_trend == "bear") or (direction == "空" and btc_trend == "bull"):
            soft_reasons.append("BTC大方向不利")
    except Exception:
        pass

    if hard_reasons or len(soft_reasons) >= 2:
        all_reasons = hard_reasons + soft_reasons
        detail = "、".join(all_reasons[:3])
        return "🚫 自動撤單", detail, True

    # ── 繼續等待 ──
    hints = soft_reasons[:1]   # 最多顯示 1 個小警訊
    passed = st['passed']
    bar    = "■" * passed + "□" * (4 - passed)
    summary = f"✅ 訊號仍有效  [{bar}] {passed}/4"
    detail  = ("、".join(hints) + "  " if hints else "") + f"ADX={st['adx']:.1f}"
    return summary, detail, False


def analyze_position(pos):
    """分析持倉狀態，回傳 (狀態標籤, 建議訊息, 是否需要推送)"""
    inst_id = pos['asset'] + '-USDT-SWAP'
    current_price = get_current_price(inst_id)
    if current_price is None:
        return None, None, False

    dir   = pos['dir']
    entry = pos['entry']
    sl    = pos['sl']
    tp1   = pos['tp1']
    # 安全取值：tp2/tp3 可能因舊持倉或 tp_count=1 而為 None，fallback 到合理估算值
    _tp1  = tp1
    tp2   = pos.get('tp2') or (_tp1 + (_tp1 - entry) * 1.5)
    tp3   = pos.get('tp3') or (_tp1 + (_tp1 - entry) * 2.5)

    _tf_str = pos.get('tf', '1H')
    if "15M" in _tf_str:
        bar = "15m"
    elif "30M" in _tf_str:
        bar = "30m"
    elif "1H" in _tf_str:
        bar = "1H"
    elif "1D" in _tf_str:
        bar = "1D"
    else:
        bar = "4H"

    # ── 先確認限價單是否已被成交 ──
    # 填單判定：只看「建倉後」開始的 K 棒，不使用 margin 往前延伸，
    # 避免舊棒高低點（訊號出現前）被誤判為已觸碰進場點。
    if not pos.get('filled', False):
        reported_at   = pos.get('reported_at', time.time())
        # 填單起始點 = 訊號發出的秒數（精確到秒，不對齊 K 棒開盤）
        # 對齊 K 棒開盤會把訊號發出「之前」的 K 棒價格（例如 4H bar 前 30 分鐘）
        # 誤算進去，導致還沒到進場點就判定已成交。
        fill_since = int(reported_at)
        # ── 以 1m K 線為主要填單判定，精度高、不受大時框對齊誤差影響 ──
        fill_high, fill_low = get_candle_range_since(inst_id, fill_since, '1m', no_margin=True)
        # ── 原始時框 K 線補充（只納入訊號發出「之後」新開盤的 K 棒）──
        _fh2, _fl2 = get_candle_range_since(inst_id, fill_since, bar, no_margin=True)
        if _fh2 is not None:
            fill_high = max(fill_high, _fh2) if fill_high is not None else _fh2
        if _fl2 is not None:
            fill_low  = min(fill_low,  _fl2) if fill_low  is not None else _fl2
        # 不把 current_price 納入 fill_eff：僅用 K 棒極值做填單判斷
        # 避免「現價本身就已在進場點另一側」時瞬間誤判已成交
        fill_eff_high = fill_high  # 純 K 棒最高點（含1m補充）
        fill_eff_low  = fill_low   # 純 K 棒最低點（含1m補充）

        # 填單方向判斷：依「訊號時現價 vs 進場點」區分突破 / 回調類型
        #   多頭突破（進場 > signal_price）→ 等價格「漲」到進場：high ≥ entry
        #   多頭回調（進場 ≤ signal_price）→ 等價格「跌」到進場：low  ≤ entry
        #   空頭突破（進場 < signal_price）→ 等價格「跌」到進場：low  ≤ entry
        #   空頭回調（進場 ≥ signal_price）→ 等價格「漲」到進場：high ≥ entry
        # ── 重要：回調型的「未成交」狀態本來就是現價在進場點另一側 ──
        # 不可加 fallback（「現價已過進場點」），否則回調型一開始就誤判已成交。
        signal_price = pos.get('signal_price', entry)  # 無記錄時 fallback = entry
        # ── 填單判定規則 ──
        # 優先用 K 線極值（fill_eff_high/low）；K 線 API 失敗時才用現價 fallback。
        # 不把 current_price 與 K 線資料並列 OR：K 線有資料時以 K 線為準，
        # 避免現價短暫剛好踩到進場點就誤判成交。
        if dir == "多":
            if entry > signal_price:
                # 突破多：等 K 線最高點升到進場點才成交
                if fill_eff_high is not None:
                    filled_by_candle = fill_eff_high >= entry
                else:
                    # K 線 API 失敗 → 用現價保底；需明顯高於進場點（+0.1%）才確認
                    filled_by_candle = current_price >= entry * 1.001
            else:
                # 回調多：等 K 線最低點跌到進場點才成交
                if fill_eff_low is not None:
                    filled_by_candle = fill_eff_low <= entry
                else:
                    # K 線 API 失敗 → 用現價保底；需明顯低於進場點（-0.1%）才確認
                    filled_by_candle = current_price <= entry * 0.999
        else:  # 空
            if entry < signal_price:
                # 突破空：等 K 線最低點跌到進場點才成交
                if fill_eff_low is not None:
                    filled_by_candle = fill_eff_low <= entry
                else:
                    # K 線 API 失敗 → 用現價保底；需明顯低於進場點（-0.1%）才確認
                    filled_by_candle = current_price <= entry * 0.999
            else:
                # 回調空：等 K 線最高點漲到進場點才成交
                if fill_eff_high is not None:
                    filled_by_candle = fill_eff_high >= entry
                else:
                    # K 線 API 失敗 → 用現價保底；需明顯高於進場點（+0.1%）才確認
                    filled_by_candle = current_price >= entry * 1.001
        if filled_by_candle:
            pos['filled']          = True
            pos['fill_ts']         = time.time()   # 記錄成交時間，供 TP/SL K 線範圍保護使用
            pos['last_checked_ts'] = time.time()   # 防止下次監控 fallback 到 reported_at（進場前）
            # 進場確認訊息：突破多/空頭顯示突破的高/低點，回調多/空頭顯示回落的低/高點
            if dir == "多":
                if entry > signal_price:   # 突破進場 → 顯示高點
                    extreme_str = f"（K線最高達 <code>{format_price(fill_eff_high)}</code>）"
                else:                      # 回調進場 → 顯示低點
                    extreme_str = f"（K線最低達 <code>{format_price(fill_eff_low)}</code>）"
            else:
                if entry < signal_price:   # 突破進場 → 顯示低點
                    extreme_str = f"（K線最低達 <code>{format_price(fill_eff_low)}</code>）"
                else:                      # 回調進場 → 顯示高點
                    extreme_str = f"（K線最高達 <code>{format_price(fill_eff_high)}</code>）"
            print(f"✅ {pos['asset']} {dir}單：K線已穿越進場點 {format_price(entry)}，開始監控 TP/SL")
            action = (f"🎯 K線已穿越進場點 <code>{format_price(entry)}</code>{extreme_str}\n"
                      f"現價 <code>{format_price(current_price)}</code>\n"
                      f"<b>限價單應已成交，開始監控 TP/SL</b>")
            return "✅ 已觸及進場點", action, True  # 推送進場確認，下次監控再做 TP/SL
        else:
            # ── 掛單尚未成交 ──
            # 1. 止損點已被突破 → 訊號作廢，自動取消
            # SL breach 使用 current_price（進場前現價直接穿越止損即作廢）
            sl_eff_low  = min(fill_eff_low,  current_price) if fill_eff_low  is not None else current_price
            sl_eff_high = max(fill_eff_high, current_price) if fill_eff_high is not None else current_price
            sl_breached = (
                (dir == "多" and sl_eff_low  <= sl) or
                (dir == "空" and sl_eff_high >= sl)
            )
            if sl_breached:
                note = (f"⛔ 現價 {format_price(current_price)} 已突破止損位 {format_price(sl)}，"
                        f"進場點 {format_price(entry)} 訊號作廢\n<b>已自動取消掛單追蹤</b>")
                return "🚫 掛單已取消", note, True

            # 2. 等待逾時（1H→2H、4H/手動→8H）→ 自動取消
            tf = pos.get('tf', '4H')
            if '15M' in tf:
                timeout_sec = 3600       # 15M：1小時
            elif '30M' in tf:
                timeout_sec = 5400       # 30M：1.5小時
            elif '1H' in tf:
                timeout_sec = 7200       # 1H：2小時
            elif '1D' in tf:
                timeout_sec = 259200     # 1D：72小時（3天）
            else:
                timeout_sec = 28800      # 4H：8小時
            pending_sec = time.time() - pos.get('reported_at', time.time())
            if pending_sec > timeout_sec:
                hours = int(pending_sec / 3600)
                note = (f"⏰ 掛單等待已超過 {hours} 小時，進場點 {format_price(entry)} 仍未觸及，"
                        f"現價 {format_price(current_price)}\n<b>訊號可能已失效，已自動取消掛單追蹤</b>")
                return "⏰ 掛單逾時取消", note, True

            # 3. 正常等待中 → 自動評估訊號健康度，決定繼續等待或自動撤單
            eval_summary, eval_detail, auto_cancel = evaluate_pending_order(pos)
            gap_pct = abs(current_price - entry) / entry * 100
            # gap_dir 顯示「價格需往哪個方向走才能到達成交條件」
            # 依 fill 模式判斷，而非現價 vs entry 位置
            _sp = pos.get('signal_price', entry)
            if dir == "多":
                gap_dir = "↑" if entry > _sp else "↓"   # 突破多（breakout）等漲 ↑，回調多等跌 ↓
            else:
                gap_dir = "↓" if entry < _sp else "↑"   # 突破空（breakout）等跌 ↓，回調空等漲 ↑
            gap_str = f"現價 <code>{format_price(current_price)}</code>，距進場點還差 {gap_pct:.2f}%{gap_dir}"
            if auto_cancel:
                note = (f"🤖 市場條件失效，<b>系統自動撤單</b>\n"
                        f"原因：{eval_detail}\n{gap_str}")
                return "🚫 掛單已取消", note, True
            else:
                note = f"{eval_summary}\n{gap_str}"
                return "⏳ 等待進場", note, False

    # ── 已成交：取上次監控後的 K 線高低點，捕捉 TP/SL 觸碰事件 ──
    since_ts = pos.get('last_checked_ts', pos.get('reported_at', time.time() - 3600))
    fill_ts  = pos.get('fill_ts', 0)
    # ── 關鍵保護：TP 時間窗口下界 = max(since_ts - 120, fill_ts) ──
    # 問題根源：fill_ts 與 last_checked_ts 在確認成交時同步設為 time.time()，
    # 下一輪監控 since_ts - 120 就會倒退進「成交那根 K 棒」，若那根 K 棒 wick 很長直接到 TP1，
    # TP1 在用戶根本沒開倉的情況下立即觸發。fill_ts 作為硬性下界可防止這個問題。
    _tp_since = int(max(since_ts - 120, fill_ts)) if fill_ts > 0 else int(since_ts - 120)
    # 以 1m K 線為主要 TP/SL 判定來源：精度最高，不受大時框 wick 誤差影響
    effective_high, effective_low = get_candle_range_since(inst_id, _tp_since, '1m', no_margin=True)
    # ── 關鍵：只有 API 成功回傳資料才推進 last_checked_ts ──
    # 若 API 超時回傳 None，保留舊 since_ts，下一輪重新覆蓋同一時段，防止 TP 觸碰被永久跳過
    _candle_api_ok = effective_high is not None
    effective_high = max(effective_high, current_price) if effective_high is not None else current_price
    effective_low  = min(effective_low,  current_price) if effective_low  is not None else current_price
    # 原始時框補充：since_ts 與 fill_ts 取較晚者，同樣避免重抓成交 K 棒
    _bar_secs = {"15m": 900, "30m": 1800, "1H": 3600, "4H": 14400, "1D": 86400}
    _bar_dur  = _bar_secs.get(bar, 3600)
    _tf_since = int(max(since_ts, fill_ts)) if fill_ts > 0 else int(since_ts)
    _rng_h, _rng_l = get_candle_range_since(inst_id, _tf_since, bar, no_margin=True)
    if _rng_h is not None:
        effective_high = max(effective_high, _rng_h)
        _candle_api_ok = True
    if _rng_l is not None:
        effective_low  = min(effective_low,  _rng_l)
    if _candle_api_ok:
        pos['last_checked_ts'] = time.time()  # 只在拿到有效資料後才推進時間窗口

    oi_change  = get_open_interest_change(inst_id)
    vol_spike  = get_volume_spike(inst_id)
    fr, ls_ratio = get_market_sentiment(inst_id)

    # ── 主力異常信號判斷 ──
    whale_warn = ""
    if vol_spike >= 2.5:
        whale_warn = f"⚡ 異常量能！成交量是均量 {vol_spike:.1f} 倍，方向不確定，<b>建議收緊止損，暫勿加倉</b>"
    elif vol_spike < 0.5:
        whale_warn = f"📉 量能萎縮（僅均量 {vol_spike*100:.0f}%），趨勢動能減弱，<b>建議縮小倉位或收緊止損</b>"
    if oi_change < -5:
        whale_warn += f"\n📉 OI 下降 {oi_change:.1f}%，主力正在撤退，<b>建議部分平倉或收緊止損</b>"
    elif oi_change > 5:
        whale_warn += f"\n📈 OI 上升 {oi_change:.1f}%，主力持續加倉，<b>可繼續持倉並同步上移止損鎖利</b>"

    # TP 確認緩衝：只要求超過 TP 0.01% 即觸發，避免漏掉真實觸碰
    # 0.01% 緩衝仍能擋掉數據噪音，但不會讓價格剛好踩到 TP1 的情況被漏掉
    # SL 不加緩衝（止損要快，只要 wick 碰到就觸發）
    TP_CONFIRM = 1.0001   # 多頭 TP：effective_high >= tp * TP_CONFIRM
                          # 空頭 TP：effective_low  <= tp / TP_CONFIRM

    # ── 持倉狀態判定（用 K 線高低點而非現價，防止監控間隔中的事件被漏掉）──
    if dir == "多":
        dist_to_sl_pct = (current_price - sl) / entry * 100
        # 止損：用 K 線低點（只要低點碰過 SL 就算觸發）
        if effective_low <= sl:
            if pos.get('tp2_hit'):
                # TP2 達標後 SL 已移至 TP1，現在回落至 TP1 → 鎖住 TP2 段利潤出場
                status = "🛡️ 回調至保本止損"
                action = (f"📉 K線低點 <code>{format_price(effective_low)}</code> 回落至止損位（TP1）<code>{format_price(sl)}</code>，"
                          f"現價 <code>{format_price(current_price)}</code>\n"
                          f"✅ TP2 段利潤已鎖定，剩餘倉位保本出場\n"
                          f"<b>⛔ 系統已停止追蹤此倉位</b>")
            elif pos.get('tp1_hit'):
                # TP1 達標後 SL 已移至進場成本，現在回落至成本 → 保本出場
                status = "🛡️ 回調至保本止損"
                action = (f"📉 K線低點 <code>{format_price(effective_low)}</code> 回落至保本止損（進場成本）<code>{format_price(sl)}</code>，"
                          f"現價 <code>{format_price(current_price)}</code>\n"
                          f"✅ TP1 段利潤已鎖定，剩餘倉位零風險出場\n"
                          f"<b>⛔ 系統已停止追蹤此倉位</b>")
            else:
                status = "🔴 止損觸發"
                action = (f"⛔ K線低點 <code>{format_price(effective_low)}</code> 已觸及止損位 <code>{format_price(sl)}</code>，"
                          f"現價 <code>{format_price(current_price)}</code>｜<b>建議立即平倉</b>")
            push = True
        # 止盈：用 K 線高點 + 0.05% 確認緩衝（防止 1-pip wick 誤觸）
        elif effective_high >= tp3 * TP_CONFIRM:
            status = "🟣 全部止盈"
            # 補記所有未標記的 TP 旗標（防止直接跳到 TP3 時狀態不一致）
            skipped = []
            if not pos.get('tp1_hit'):
                pos['tp1_hit'] = True
                skipped.append(f"TP1 <code>{format_price(tp1)}</code>")
            if not pos.get('tp2_hit'):
                pos['tp2_hit'] = True
                skipped.append(f"TP2 <code>{format_price(tp2)}</code>")
            skip_note = f"（同時穿越 {'、'.join(skipped)}）" if skipped else ""
            action = (f"🎯 K線高點 {format_price(effective_high)} 已達止盈3 {format_price(tp3)}{skip_note}，"
                      f"現價 {format_price(current_price)}")
            push = True
        elif effective_high >= tp2 * TP_CONFIRM:
            if pos.get('tp2_hit'):
                # TP2 已完成（tp_count==3），ATR×2.5 追蹤止損繼續鎖利剩餘 20%（多頭）
                _t_dist = pos.get('atr_trail', 0) or pos.get('trail_dist', entry * 0.015)
                trail_dist = _t_dist
                new_trail_sl = current_price - trail_dist
                _trail_moved = new_trail_sl > sl
                if _trail_moved:
                    pos['sl'] = new_trail_sl
                    sl = new_trail_sl
                status = "🔵 TP2已完成"
                action = (f"🎯 剩餘20%移動止盈中，等待TP3 <code>{format_price(tp3)}</code>\n"
                          f"追蹤止損（ATR×2.5）= <code>{format_price(sl)}</code>，現價 {format_price(current_price)}")
                deteri = check_market_deterioration(inst_id, dir, pos.get('tf','1H'))
                if deteri:
                    if not pos.get('deteri_alerted'):
                        status = "🚨 局勢惡化"
                        action += f"\n{deteri}"
                        pos['deteri_alerted'] = True
                        push = True
                    else:
                        push = False
                else:
                    if pos.get('deteri_alerted'):
                        pos['deteri_alerted'] = False
                        action += "\n✅ 市場局勢已恢復，持倉方向重新與趨勢一致"
                        push = True
                    elif _trail_moved:
                        # 追蹤止損收緊 → 推播一次，建議用戶考慮立即出場鎖利
                        # 頻率保護：30分鐘內只推一次，避免每輪監控都轟炸
                        _now_ts_tr = time.time()
                        _last_tr_push = pos.get('last_trail_push_ts', 0)
                        if _now_ts_tr - _last_tr_push >= 900:
                            pos['last_trail_push_ts'] = _now_ts_tr
                            status = "🔵 TP2已完成"
                            action = (f"📈 移動止損上移至 <code>{format_price(sl)}</code>（ATR×2.5）\n"
                                      f"現價 <code>{format_price(current_price)}</code>，距TP3 <code>{format_price(tp3)}</code> 還差 "
                                      f"{abs(tp3 - current_price) / current_price * 100:.2f}%\n"
                                      f"▸ <b>建議現在出場鎖定利潤</b>，或繼續持有等TP3（止損已鎖利）")
                            push = True
                        else:
                            push = False
                    else:
                        push = False
            else:
                _tp_count_long = pos.get('tp_count', 3)
                if _tp_count_long == 2:
                    # TP2 是最終目標 → 全部平倉
                    pos['tp2_hit'] = True
                    if not pos.get('tp1_hit'):
                        pos['tp1_hit'] = True
                    status = "🟣 全部止盈"
                    action = (f"🎯 K線高點 {format_price(effective_high)} 已達止盈2 {format_price(tp2)}，"
                              f"現價 {format_price(current_price)}\n<b>所有止盈目標達成，建議全部平倉</b>")
                else:
                    # tp_count == 3 → 40%平倉（TP2），剩餘20%立即啟動 ATR×2.5 移動止盈（多頭）
                    pos['tp2_hit'] = True
                    # 以 ATR×2.5 計算初始追蹤止損，取較保守的那個（比 TP1 高則用追蹤止損）
                    _atr_trail_dist = pos.get('atr_trail', 0)
                    if _atr_trail_dist > 0:
                        pos['trail_dist'] = _atr_trail_dist
                        _init_trail_sl = current_price - _atr_trail_dist
                        _new_sl_long = max(tp1, _init_trail_sl)   # 不低於 TP1（保底）
                    else:
                        _new_sl_long = tp1
                    pos['sl'] = _new_sl_long
                    sl = _new_sl_long
                    status = "🔵 止盈2達標"
                    if not pos.get('tp1_hit'):
                        # K線一根直接衝破 TP1→TP2，補記 TP1 並在訊息中說明
                        pos['tp1_hit'] = True
                        if _atr_trail_dist == 0:
                            pos['trail_dist'] = entry - pos.get('orig_sl', entry - (tp1 - entry))
                        action = (f"✅ K線高點 {format_price(effective_high)} 同時穿越 TP1+TP2，"
                                  f"現價 {format_price(current_price)}\n"
                                  f"▸ TP1 <code>{format_price(tp1)}</code> 已自動確認\n"
                                  f"▸ <b>建議合計平倉80%</b>（TP1段40% + TP2段40%）\n"
                                  f"⚠️ <b>請立即將止損上移至 <code>{format_price(sl)}</code></b>（ATR×2.5）\n"
                                  f"▸ 剩20%等待TP3 <code>{format_price(tp3)}</code>，止損移至保本以上")
                    else:
                        action = (f"✅ K線高點 {format_price(effective_high)} 已達止盈2 <code>{format_price(tp2)}</code>，"
                                  f"現價 {format_price(current_price)}\n"
                                  f"▸ <b>建議平倉40%</b>（TP2段）\n"
                                  f"⚠️ <b>請立即將止損上移至 <code>{format_price(sl)}</code></b>（ATR×2.5）\n"
                                  f"▸ 剩20%等待TP3 <code>{format_price(tp3)}</code>")
                push = True
        elif effective_high >= tp1 * TP_CONFIRM:
            if pos.get('tp1_hit'):
                # 追蹤止損：TP1 後每次監控都把 SL 往上拉緊鎖利
                trail_dist = pos.get('trail_dist', entry * 0.015)
                new_trail_sl = current_price - trail_dist
                if new_trail_sl > sl:
                    pos['sl'] = new_trail_sl
                    sl = new_trail_sl
                # TP1 已在前次通知，本次靜默監控等待 TP2/TP3
                _tc_now = pos.get('tp_count', 3)
                _rem_pct = 60 if _tc_now >= 3 else 50
                status = "🟢 TP1已完成"
                action = f"剩餘{_rem_pct}%持倉中，等待TP2 <code>{format_price(tp2)}</code>，追蹤止損 {format_price(sl)}，現價 {format_price(current_price)}"
                deteri = check_market_deterioration(inst_id, dir, pos.get('tf','1H'))
                if deteri:
                    if not pos.get('deteri_alerted'):
                        status = "🚨 局勢惡化"
                        action += f"\n{deteri}"
                        pos['deteri_alerted'] = True
                        push = True
                    else:
                        push = False
                else:
                    if pos.get('deteri_alerted'):
                        pos['deteri_alerted'] = False
                        action += "\n✅ 市場局勢已恢復，持倉方向重新與趨勢一致"
                        push = True
                    else:
                        push = False
            else:
                _tp_count_long = pos.get('tp_count', 3)
                if _tp_count_long == 1:
                    # 單一止盈目標 → 全部平倉
                    status = "🟣 全部止盈"
                    action = (f"🎯 K線高點 {format_price(effective_high)} 已達止盈 {format_price(tp1)}，"
                              f"現價 {format_price(current_price)}\n<b>止盈目標達成，建議全部平倉</b>")
                    pos['tp1_hit'] = True
                else:
                    # tp_count >= 2：依分倉制度決定平倉比例
                    # tp_count==3 → 40%平倉（TP1），剩60%等TP2(40%)+TP3(20%)
                    # tp_count==2 → 50%平倉（TP1），剩50%等TP2
                    _tp_cl_pct = 40 if pos.get('tp_count', 3) >= 3 else 50
                    _tp_remain = 60 if _tp_cl_pct == 40 else 50
                    status = "🟢 止盈1達標"
                    _f_r  = 0.0005
                    _sl_be = max(entry, 2 * entry * (1 + _f_r) / (1 - _f_r) - tp1)
                    _next_desc = "TP2（再平倉40%）+ TP3（20%移動止盈）" if _tp_cl_pct == 40 else f"TP2 <code>{format_price(tp2)}</code>"
                    action = (f"✅ K線高點 {format_price(effective_high)} 已達止盈1 <code>{format_price(tp1)}</code>\n"
                              f"▸ <b>建議平倉 {_tp_cl_pct}%</b>，止損已自動上移至保本點 <code>{format_price(_sl_be)}</code>\n"
                              f"▸ 剩餘 {_tp_remain}% 持倉繼續等待 {_next_desc}")
                    pos['tp1_hit']    = True
                    pos['trail_dist'] = entry - sl
                    pos['sl'] = _sl_be
                    sl = _sl_be
                push = True
        else:
            # 未觸及 SL/TP：信任策略止損，靜默監控
            # 只在「局勢明顯惡化」時才推送，正常浮盈浮虧不打擾
            status = "🔄 持倉中"
            action = f"持倉中，現價 {format_price(current_price)}，距止損 {dist_to_sl_pct:.1f}%"
            deteri = check_market_deterioration(inst_id, dir, pos.get('tf','1H'))
            if deteri:
                if not pos.get('deteri_alerted'):
                    status = "🚨 局勢惡化"
                    action = f"現價 {format_price(current_price)}，距止損 {dist_to_sl_pct:.1f}%\n{deteri}"
                    pos['deteri_alerted'] = True
                    push = True
                else:
                    push = False
            else:
                if pos.get('deteri_alerted'):
                    pos['deteri_alerted'] = False
                    action = f"現價 {format_price(current_price)}，距止損 {dist_to_sl_pct:.1f}%\n✅ 市場局勢已恢復，持倉方向重新與趨勢一致"
                    push = True
                else:
                    push = False
    else:  # 空
        dist_to_sl_pct = (sl - current_price) / entry * 100
        # 止損：用 K 線高點
        if effective_high >= sl:
            if pos.get('tp2_hit'):
                # TP2 達標後 SL 已移至 TP1，現在反彈至 TP1 → 鎖住 TP2 段利潤出場
                status = "🛡️ 回調至保本止損"
                action = (f"📈 K線高點 <code>{format_price(effective_high)}</code> 反彈至止損位（TP1）<code>{format_price(sl)}</code>，"
                          f"現價 <code>{format_price(current_price)}</code>\n"
                          f"✅ TP2 段利潤已鎖定，剩餘倉位保本出場\n"
                          f"<b>⛔ 系統已停止追蹤此倉位</b>")
            elif pos.get('tp1_hit'):
                # TP1 達標後 SL 已移至進場成本，現在反彈至成本 → 保本出場
                status = "🛡️ 回調至保本止損"
                action = (f"📈 K線高點 <code>{format_price(effective_high)}</code> 反彈至保本止損（進場成本）<code>{format_price(sl)}</code>，"
                          f"現價 <code>{format_price(current_price)}</code>\n"
                          f"✅ TP1 段利潤已鎖定，剩餘倉位零風險出場\n"
                          f"<b>⛔ 系統已停止追蹤此倉位</b>")
            else:
                status = "🔴 止損觸發"
                action = (f"⛔ K線高點 <code>{format_price(effective_high)}</code> 已觸及止損位 <code>{format_price(sl)}</code>，"
                          f"現價 <code>{format_price(current_price)}</code>｜<b>建議立即平倉</b>")
            push = True
        # 止盈：用 K 線低點 + 0.05% 確認緩衝（防止 1-pip wick 誤觸）
        elif effective_low <= tp3 / TP_CONFIRM:
            status = "🟣 全部止盈"
            # 補記所有未標記的 TP 旗標（防止直接跳到 TP3 時狀態不一致）
            skipped = []
            if not pos.get('tp1_hit'):
                pos['tp1_hit'] = True
                skipped.append(f"TP1 <code>{format_price(tp1)}</code>")
            if not pos.get('tp2_hit'):
                pos['tp2_hit'] = True
                skipped.append(f"TP2 <code>{format_price(tp2)}</code>")
            skip_note = f"（同時穿越 {'、'.join(skipped)}）" if skipped else ""
            action = (f"🎯 K線低點 {format_price(effective_low)} 已達止盈3 {format_price(tp3)}{skip_note}，"
                      f"現價 {format_price(current_price)}")
            push = True
        elif effective_low <= tp2 / TP_CONFIRM:
            if pos.get('tp2_hit'):
                # TP2 已完成，ATR×2.5 追蹤止損繼續鎖利剩餘 20%（空頭）
                _t_dist_s = pos.get('atr_trail', 0) or pos.get('trail_dist', entry * 0.015)
                trail_dist = _t_dist_s
                new_trail_sl = current_price + trail_dist
                _trail_moved_s = new_trail_sl < sl
                if _trail_moved_s:
                    pos['sl'] = new_trail_sl
                    sl = new_trail_sl
                status = "🔵 TP2已完成"
                action = (f"🎯 剩餘20%移動止盈中，等待TP3 <code>{format_price(tp3)}</code>\n"
                          f"追蹤止損（ATR×2.5）= <code>{format_price(sl)}</code>，現價 {format_price(current_price)}")
                deteri = check_market_deterioration(inst_id, dir, pos.get('tf','4H'))
                if deteri:
                    if not pos.get('deteri_alerted'):
                        status = "🚨 局勢惡化"
                        action += f"\n{deteri}"
                        pos['deteri_alerted'] = True
                        push = True
                    else:
                        push = False
                else:
                    if pos.get('deteri_alerted'):
                        pos['deteri_alerted'] = False
                        action += "\n✅ 市場局勢已恢復，持倉方向重新與趨勢一致"
                        push = True
                    elif _trail_moved_s:
                        # 追蹤止損下移收緊 → 推播一次，建議用戶考慮立即出場鎖利（空頭）
                        # 頻率保護：30分鐘內只推一次，避免每輪監控都轟炸
                        _now_ts_tr_s = time.time()
                        _last_tr_push_s = pos.get('last_trail_push_ts', 0)
                        if _now_ts_tr_s - _last_tr_push_s >= 900:
                            pos['last_trail_push_ts'] = _now_ts_tr_s
                            status = "🔵 TP2已完成"
                            action = (f"📉 移動止損下移至 <code>{format_price(sl)}</code>（ATR×2.5）\n"
                                      f"現價 <code>{format_price(current_price)}</code>，距TP3 <code>{format_price(tp3)}</code> 還差 "
                                      f"{abs(current_price - tp3) / current_price * 100:.2f}%\n"
                                      f"▸ <b>建議現在出場鎖定利潤</b>，或繼續持有等TP3（止損已鎖利）")
                            push = True
                        else:
                            push = False
                    else:
                        push = False
            else:
                _tp_count_short = pos.get('tp_count', 3)
                if _tp_count_short == 2:
                    # TP2 是最終目標 → 全部平倉
                    pos['tp2_hit'] = True
                    if not pos.get('tp1_hit'):
                        pos['tp1_hit'] = True
                    status = "🟣 全部止盈"
                    action = (f"🎯 K線低點 {format_price(effective_low)} 已達止盈2 {format_price(tp2)}，"
                              f"現價 {format_price(current_price)}\n<b>所有止盈目標達成，建議全部平倉</b>")
                else:
                    # tp_count == 3 → 40%平倉（TP2），剩餘20%立即啟動 ATR×2.5 移動止盈（空頭）
                    pos['tp2_hit'] = True
                    # 以 ATR×2.5 計算初始追蹤止損，取較保守的那個（比 TP1 低則用追蹤止損）
                    _atr_trail_dist_s = pos.get('atr_trail', 0)
                    if _atr_trail_dist_s > 0:
                        pos['trail_dist'] = _atr_trail_dist_s
                        _init_trail_sl_s = current_price + _atr_trail_dist_s
                        _new_sl_short = min(tp1, _init_trail_sl_s)   # 不高於 TP1（保底）
                    else:
                        _new_sl_short = tp1
                    pos['sl'] = _new_sl_short
                    sl = _new_sl_short
                    status = "🔵 止盈2達標"
                    if not pos.get('tp1_hit'):
                        # K線一根直接衝破 TP1→TP2，補記 TP1 並在訊息中說明（空頭）
                        pos['tp1_hit'] = True
                        if _atr_trail_dist_s == 0:
                            pos['trail_dist'] = pos.get('orig_sl', entry + (entry - tp1)) - entry
                        action = (f"✅ K線低點 {format_price(effective_low)} 同時穿越 TP1+TP2，"
                                  f"現價 {format_price(current_price)}\n"
                                  f"▸ TP1 <code>{format_price(tp1)}</code> 已自動確認\n"
                                  f"▸ <b>建議合計平倉80%</b>（TP1段40% + TP2段40%）\n"
                                  f"⚠️ <b>請立即將止損下移至 <code>{format_price(sl)}</code></b>（ATR×2.5）\n"
                                  f"▸ 剩20%等待TP3 <code>{format_price(tp3)}</code>，止損移至保本以上")
                    else:
                        action = (f"✅ K線低點 {format_price(effective_low)} 已達止盈2 <code>{format_price(tp2)}</code>，"
                                  f"現價 {format_price(current_price)}\n"
                                  f"▸ <b>建議平倉40%</b>（TP2段）\n"
                                  f"⚠️ <b>請立即將止損下移至 <code>{format_price(sl)}</code></b>（ATR×2.5）\n"
                                  f"▸ 剩20%等待TP3 <code>{format_price(tp3)}</code>")
                push = True
        elif effective_low <= tp1 / TP_CONFIRM:
            if pos.get('tp1_hit'):
                # 追蹤止損：TP1 後每次監控都把 SL 往下拉緊鎖利（空頭）
                trail_dist = pos.get('trail_dist', entry * 0.015)
                new_trail_sl = current_price + trail_dist
                if new_trail_sl < sl:
                    pos['sl'] = new_trail_sl
                    sl = new_trail_sl
                # TP1 已在前次通知，本次靜默監控等待 TP2/TP3（空頭）
                _tc_now_s = pos.get('tp_count', 3)
                _rem_pct_s = 60 if _tc_now_s >= 3 else 50
                status = "🟢 TP1已完成"
                action = f"剩餘{_rem_pct_s}%持倉中，等待TP2 <code>{format_price(tp2)}</code>，追蹤止損 {format_price(sl)}，現價 {format_price(current_price)}"
                deteri = check_market_deterioration(inst_id, dir, pos.get('tf','4H'))
                if deteri:
                    if not pos.get('deteri_alerted'):
                        status = "🚨 局勢惡化"
                        action += f"\n{deteri}"
                        pos['deteri_alerted'] = True
                        push = True
                    else:
                        push = False
                else:
                    if pos.get('deteri_alerted'):
                        pos['deteri_alerted'] = False
                        action += "\n✅ 市場局勢已恢復，持倉方向重新與趨勢一致"
                        push = True
                    else:
                        push = False
            else:
                _tp_count_short = pos.get('tp_count', 3)
                if _tp_count_short == 1:
                    # 單一止盈目標 → 全部平倉
                    status = "🟣 全部止盈"
                    action = (f"🎯 K線低點 {format_price(effective_low)} 已達止盈 {format_price(tp1)}，"
                              f"現價 {format_price(current_price)}\n<b>止盈目標達成，建議全部平倉</b>")
                    pos['tp1_hit'] = True
                else:
                    # tp_count >= 2：依分倉制度決定平倉比例（空頭）
                    # tp_count==3 → 40%平倉（TP1），剩60%等TP2(40%)+TP3(20%)
                    # tp_count==2 → 50%平倉（TP1），剩50%等TP2
                    _tp_cl_pct_s = 40 if pos.get('tp_count', 3) >= 3 else 50
                    _tp_remain_s = 60 if _tp_cl_pct_s == 40 else 50
                    status = "🟢 止盈1達標"
                    _f_r  = 0.0005
                    _sl_be_s = min(entry, 2 * entry * (1 - _f_r) / (1 + _f_r) - tp1)
                    _next_desc_s = "TP2（再平倉40%）+ TP3（20%移動止盈）" if _tp_cl_pct_s == 40 else f"TP2 <code>{format_price(tp2)}</code>"
                    action = (f"✅ K線低點 {format_price(effective_low)} 已達止盈1 <code>{format_price(tp1)}</code>\n"
                              f"▸ <b>建議平倉 {_tp_cl_pct_s}%</b>，止損已自動下移至保本點 <code>{format_price(_sl_be_s)}</code>\n"
                              f"▸ 剩餘 {_tp_remain_s}% 持倉繼續等待 {_next_desc_s}")
                    pos['tp1_hit']    = True
                    pos['trail_dist'] = sl - entry
                    pos['sl'] = _sl_be_s
                    sl = _sl_be_s
                push = True
        else:
            # 未觸及 SL/TP：信任策略止損，靜默監控
            # 只在「局勢明顯惡化」時才推送，正常浮盈浮虧不打擾
            status = "🔄 持倉中"
            action = f"持倉中，現價 {format_price(current_price)}，距止損 {dist_to_sl_pct:.1f}%"
            deteri = check_market_deterioration(inst_id, dir, pos.get('tf','4H'))
            if deteri:
                if not pos.get('deteri_alerted'):
                    status = "🚨 局勢惡化"
                    action = f"現價 {format_price(current_price)}，距止損 {dist_to_sl_pct:.1f}%\n{deteri}"
                    pos['deteri_alerted'] = True
                    push = True
                else:
                    push = False
            else:
                if pos.get('deteri_alerted'):
                    pos['deteri_alerted'] = False
                    action = f"現價 {format_price(current_price)}，距止損 {dist_to_sl_pct:.1f}%\n✅ 市場局勢已恢復，持倉方向重新與趨勢一致"
                    push = True
                else:
                    push = False

    full_action = action
    if whale_warn:
        full_action += f"\n{whale_warn}"

    # 加入 OI/多空比 狀態補充
    long_pct  = round(ls_ratio / (ls_ratio + 1) * 100)
    short_pct = 100 - long_pct
    full_action += f"\n📊 主力資料：OI變化{oi_change:+.1f}% | 多{long_pct}%:空{short_pct}% | 費率{fr*100:.4f}%"

    # ── 累計記錄 TP 達標旗標（只增不減，跨監控週期保留）──
    # 必須與主鏈 TP 判斷使用相同的 TP_CONFIRM 緩衝，否則 flag 會在主鏈未觸發時就被設為 True
    if pos.get('filled'):
        if dir == "多":
            if effective_high >= tp1 * TP_CONFIRM: pos['tp1_hit'] = True
            if effective_high >= tp2 * TP_CONFIRM: pos['tp2_hit'] = True
            if effective_high >= tp3 * TP_CONFIRM: pos['tp3_hit'] = True
        else:
            if effective_low <= tp1 / TP_CONFIRM: pos['tp1_hit'] = True
            if effective_low <= tp2 / TP_CONFIRM: pos['tp2_hit'] = True
            if effective_low <= tp3 / TP_CONFIRM: pos['tp3_hit'] = True

    return status, full_action, push

def build_coin_conclusion(pos_list, current_price):
    """針對同一幣種多筆持倉，給出一致的操作建議（單筆時回傳 None）"""
    if len(pos_list) <= 1:
        return None

    dirs = [p['dir'] for p in pos_list]
    has_long  = "多" in dirs
    has_short = "空" in dirs

    if has_long and has_short:
        return ("⚠️ <b>多空對沖警告：</b>此幣種同時存在多空倉位，"
                "請確認是否為刻意對沖策略，建議擇一方向管理風險")

    direction_word = "做多" if has_long else "做空"

    if not current_price:
        return f"📊 共 {len(pos_list)} 筆{direction_word}，無法取得現價"

    # 計算每筆損益，並找出最有利 / 最不利的倉位
    pos_with_pnl = []
    for p in pos_list:
        if p['dir'] == "多":
            pnl = (current_price - p['entry']) / p['entry'] * 100
        else:
            pnl = (p['entry'] - current_price) / p['entry'] * 100
        pos_with_pnl.append((p, pnl))

    pos_with_pnl.sort(key=lambda x: x[1], reverse=True)   # 最有利排前
    lead_pos, lead_pnl   = pos_with_pnl[0]   # 最有利（P&L 最高）
    lag_pos,  lag_pnl    = pos_with_pnl[-1]  # 最不利（P&L 最低）

    lead_entry = format_price(lead_pos['entry'])
    lag_entry  = format_price(lag_pos['entry'])

    # 最有利倉位是否已達止盈
    lead_tp1_hit = (
        (lead_pos['dir'] == "多" and current_price >= lead_pos['tp1']) or
        (lead_pos['dir'] == "空" and current_price <= lead_pos['tp1'])
    )
    lead_tp2_hit = (
        (lead_pos['dir'] == "多" and current_price >= lead_pos['tp2']) or
        (lead_pos['dir'] == "空" and current_price <= lead_pos['tp2'])
    )

    prefix = f"📌 <b>綜合建議（{len(pos_list)} 筆{direction_word}）：</b>"

    # ── 情境判斷 ──────────────────────────────────────────
    if lead_tp2_hit:
        # 最有利倉已達TP2 → 最不利倉立刻移至成本
        return (f"{prefix}進場 <code>{lead_entry}</code> 已達止盈2 🎯，"
                f"<b>立即將進場 <code>{lag_entry}</code> 止損上移至成本 <code>{lag_entry}</code>，全力保護第二筆倉位利潤</b>")

    elif lead_tp1_hit:
        # 最有利倉達TP1 → 不利倉移至成本
        return (f"{prefix}進場 <code>{lead_entry}</code> 已達止盈1 ({lead_pnl:+.1f}%) ✅，"
                f"<b>建議立即將進場 <code>{lag_entry}</code> 止損上移至成本 <code>{lag_entry}</code> 保護倉位</b>")

    elif lead_pnl >= 2 and lag_pnl >= 0:
        # 兩筆都獲利，有利倉獲利更多 → 建議同步收緊止損
        return (f"{prefix}兩筆均獲利 ({lead_pnl:+.1f}% / {lag_pnl:+.1f}%)，"
                f"<b>建議統一將止損上移至各自成本（{lead_entry} / {lag_entry}），鎖定利潤</b>")

    elif lead_pnl >= 1 and lag_pnl < 0:
        # 一賺一虧 → 以獲利倉對沖，不利倉移至成本
        return (f"{prefix}進場 <code>{lead_entry}</code> 獲利 {lead_pnl:+.1f}%，"
                f"進場 <code>{lag_entry}</code> 虧損 {lag_pnl:+.1f}% → "
                f"<b>建議將虧損倉 <code>{lag_entry}</code> 止損收緊至成本，避免進一步擴大損失</b>")

    elif lead_pnl >= -1 and lag_pnl >= -1:
        # 兩筆都貼近成本 → 以較不利為基準統一防守
        return (f"{prefix}兩筆均貼近成本 ({lead_pnl:+.1f}% / {lag_pnl:+.1f}%)，"
                f"<b>建議以較不利進場點 <code>{lag_entry}</code> 為基準，統一收緊止損防守</b>")

    elif lead_pnl < 0 and lag_pnl < -3:
        # 兩筆均虧損 → 統一減倉
        return (f"{prefix}兩筆{direction_word}均虧損 ({lead_pnl:+.1f}% / {lag_pnl:+.1f}%) → "
                f"<b>⛔ 建議以進場 <code>{lag_entry}</code> 較不利倉為主，"
                f"統一收緊止損或各減倉 50% 控制風險</b>")

    else:
        return (f"{prefix}進場 <code>{lead_entry}</code> ({lead_pnl:+.1f}%) / "
                f"進場 <code>{lag_entry}</code> ({lag_pnl:+.1f}%) → "
                f"<b>持續觀察，以較不利的 <code>{lag_entry}</code> 倉位止損為優先管理目標</b>")

def run_position_monitor():
    """每小時 xx:30 執行，監控所有活躍持倉"""
    with active_positions_lock:
        positions = list(active_positions)

    if not positions:
        print("📭 無活躍持倉需要監控")
        return

    la_tz = pytz.timezone('America/Los_Angeles')
    now_str = datetime.datetime.now(la_tz).strftime('%Y-%m-%d %H:%M')

    alerts = []
    pending_alerts = []   # 掛單等待中，需要單獨帶按鈕發送
    to_remove = []

    for pos in positions:
        age_hours = (time.time() - pos['reported_at']) / 3600

        # 未成交掛單過期（1H訊號4小時、4H訊號12小時）→ 強制清除並通知
        if not pos.get('filled', False):
            _pos_tf = pos.get('tf', '1H')
            if '15M' in _pos_tf:
                expiry_h = 2    # 15M：2小時
            elif '30M' in _pos_tf:
                expiry_h = 3    # 30M：3小時
            elif '1H' in _pos_tf:
                expiry_h = 4    # 1H：4小時
            elif '1D' in _pos_tf:
                expiry_h = 96   # 1D：4天
            else:
                expiry_h = 12   # 4H：12小時
            if age_hours > expiry_h:
                inst_id = pos['asset'] + '-USDT-SWAP'
                current_price = get_current_price(inst_id)
                price_str = format_price(current_price) if current_price else "無法取得"
                gap = ""
                if current_price:
                    gap_pct = abs(current_price - pos['entry']) / pos['entry'] * 100
                    _sp_exp = pos.get('signal_price', pos['entry'])
                    if pos['dir'] == "多":
                        arrow = "↑" if pos['entry'] > _sp_exp else "↓"
                    else:
                        arrow = "↓" if pos['entry'] < _sp_exp else "↑"
                    gap = f"距進場點還差 {gap_pct:.2f}%{arrow}"
                dir_tag = "🟢 <b>做多</b>" if pos['dir'] == "多" else "🔴 <b>做空</b>"
                cancel_msg = (
                    f"🚫🚫🚫 <b>【 ⛔ 撤　單 ⛔ 】</b> 🚫🚫🚫\n\n"
                    f"<b>{pos['asset']}</b>  {dir_tag}  <b>{pos['tf']}</b>\n"
                    f"掛單超過 <b>{expiry_h}H</b> 未成交，系統自動撤單。\n\n"
                    f"• 掛單進場點：<code>{format_price(pos['entry'])}</code>\n"
                    f"• 現價：<code>{price_str}</code>  {gap}\n\n"
                    f"❌ <b>請立即取消此掛單</b>，等待下次訊號重新進場。"
                )
                text_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
                requests.post(text_url, json={"chat_id": str(TELEGRAM_CHAT_ID), "text": cancel_msg, "parse_mode": "HTML"})
                print(f"📤 撤單通知已發送：{pos['asset']} {pos['dir']}")
                to_remove.append(pos)
                continue  # 已處理，跳至下一筆
            # 未過期的掛單：繼續往下呼叫 analyze_position，偵測止損突破/指標惡化

        # 已成交持倉：依時框設最大追蹤期，逾時發最後警告再移除
        _filled_tf = pos.get('tf', '1H')
        if '15M' in _filled_tf:
            _filled_max_h = 24
        elif '30M' in _filled_tf:
            _filled_max_h = 36
        elif '1H' in _filled_tf:
            _filled_max_h = 48
        elif '1D' in _filled_tf:
            _filled_max_h = 168   # 1D：最多追蹤 7 天
        else:
            _filled_max_h = 72    # 4H：3 天
        if pos.get('filled', False) and age_hours > _filled_max_h:
            inst_id = pos['asset'] + '-USDT-SWAP'
            current_price = get_current_price(inst_id)
            oi_change = get_open_interest_change(inst_id)
            fr, ls_ratio = get_market_sentiment(inst_id)
            price_str = format_price(current_price) if current_price else "無法取得"

            if current_price:
                if pos['dir'] == "多":
                    pnl_pct = (current_price - pos['entry']) / pos['entry'] * 100
                    trend = "獲利中" if pnl_pct > 0 else "虧損中"
                else:
                    pnl_pct = (pos['entry'] - current_price) / pos['entry'] * 100
                    trend = "獲利中" if pnl_pct > 0 else "虧損中"
                pnl_label = f"{'📈' if pnl_pct > 0 else '📉'} {trend} {pnl_pct:+.2f}%"
            else:
                pnl_label = "無法計算損益"

            if oi_change < -3 or (pos['dir'] == "多" and ls_ratio < 0.95) or (pos['dir'] == "空" and ls_ratio > 1.05):
                recommendation = "⚠️ 主力動向偏向不利，<b>建議現價平倉</b>，勿繼續持有"
            elif abs(pnl_pct if current_price else 0) < 0.5:
                recommendation = "🔄 盤整超過 24 小時，趨勢動能減弱，<b>建議計劃性平倉 50% 降低風險</b>"
            elif pnl_pct > 0:
                recommendation = f"✅ 目前獲利，建議<b>止損移至成本保護利潤</b>，剩餘倉位等待止盈2"
            else:
                recommendation = "⛔ 虧損超過 24 小時未止損，<b>建議立即平倉停損</b>，等待下次訊號"

            expiry_alert = (pos, f"⏰ 持倉超過24H",
                f"現價：<code>{price_str}</code>  {pnl_label}\n"
                f"   OI變化：{oi_change:+.1f}% | 多空比：{ls_ratio:.2f}\n"
                f"   {recommendation}")
            alerts.append(expiry_alert)
            # ── 逾時移除前記錄結果（防止 TP1 已達標的勝場被漏記）──
            if pos.get('tp1_hit') or pos.get('tp2_hit'):
                record_trade_outcome(pos, "win")
            elif pos.get('filled', False):
                record_trade_outcome(pos, "loss")
            to_remove.append(pos)
            continue

        try:
            status, action, push = analyze_position(pos)
        except Exception as _e:
            print(f"⚠️ analyze_position 發生錯誤 [{pos.get('asset','?')} {pos.get('dir','?')}]：{_e}")
            import traceback; traceback.print_exc()
            continue
        if status is None:
            continue

        if push:
            if status == "⏳ 等待進場":
                pending_alerts.append((pos, status, action))
            else:
                alerts.append((pos, status, action))

        # 止損觸發 / 全部止盈 / 保本回調 / 掛單取消 → 移除追蹤 + 記錄結果
        OUTCOME_MAP = {
            "🟣 全部止盈":        "win",
            "🛡️ 回調至保本止損":  "win",
            "🔴 止損觸發":        "loss",
        }
        if status in ("🔴 止損觸發", "🟣 全部止盈", "🛡️ 回調至保本止損",
                      "🚫 掛單已取消", "⏰ 掛單逾時取消"):
            if status in OUTCOME_MAP and pos.get('filled', False):
                record_trade_outcome(pos, OUTCOME_MAP[status])
            to_remove.append(pos)

    # 清理過期 / 已結束的持倉，並儲存最新 TP 旗標
    with active_positions_lock:
        for p in to_remove:
            if p in active_positions:
                active_positions.remove(p)
        save_positions(active_positions)   # 無論有無移除，都存一次（保留 tp_hit 旗標）

    # ── 掛單等待中：逐筆推送機器人直觀判斷，無需用戶選擇 ──
    la_tz_p = pytz.timezone('America/Los_Angeles')
    now_str_p = datetime.datetime.now(la_tz_p).strftime('%H:%M PT')
    text_url_p = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    for pos_p, _, action_p in pending_alerts:
        try:
            d_p  = "🟩多" if pos_p['dir'] == "多" else "🟥空"
            tf_p = pos_p.get('tf', '4H')
            msg_p = (
                f"⏳ <b>{pos_p['asset']} 掛單更新</b>  {now_str_p}\n"
                f"{d_p}  {tf_p}  "
                f"進場 <code>{format_price(pos_p['entry'])}</code>  止損 <code>{format_price(pos_p['sl'])}</code>\n"
                f"{action_p}"
            )
            requests.post(text_url_p, json={
                "chat_id": str(TELEGRAM_CHAT_ID), "text": msg_p,
                "parse_mode": "HTML"
            })
        except Exception as _pe:
            print(f"⚠️ pending_alert 發送錯誤 [{pos_p.get('asset','?')}]：{_pe}")

    if not alerts:
        print(f"✅ 持倉監控完成，{len(positions)} 筆持倉狀態正常")
        return

    # 發送警報（按幣種分組）
    # 收集全部持倉的幣種分組（用於綜合結論）
    all_by_asset = defaultdict(list)
    for p in positions:
        all_by_asset[p['asset']].append(p)

    # 有警報的幣種分組
    alerts_by_asset = defaultdict(list)
    for pos, status, action in alerts:
        alerts_by_asset[pos['asset']].append((pos, status, action))

    msg = f"<b>【持倉監控警報】</b>  <code>{now_str} PT</code>\n"
    msg += f"監控 <b>{len(positions)}</b> 筆  ·  <b>{len(alerts_by_asset)}</b> 幣種需注意\n"
    msg += "─────────────────────────\n"

    # 各狀態對應的持倉建議
    REC = {
        "✅ 已觸及進場點":     ("確認成交，開始監控TP/SL", "🎯"),
        "🎯 待確認進場":       ("請按鈕確認是否已進場",    "🎯"),
        "🚫 掛單已取消":       ("止損已破，掛單自動取消",  "🚫"),
        "⏰ 掛單逾時取消":     ("逾時未成交，自動取消",   "⏰"),
        "🔵 TP2已完成":        ("繼續持有等TP3",  "⏳"),
        "🔴 止損觸發":         ("現價出場止損",   "🚪"),
        "🛡️ 回調至保本止損":   ("出場保本",       "🛡️"),
        "🟣 全部止盈":         ("全數出場",       "🎊"),
        "🔵 止盈2達標":        ("平倉40%，剩20%啟動移動止盈", "📤"),
        "🟢 止盈1達標":        ("平倉40%，止損移至保本", "📤"),
        "⚠️ 即將觸及保本止損": ("主動出場保本",   "⚠️"),
        "⚠️ 接近止損":         ("考慮現價出場",   "⚠️"),
        "🚨 局勢惡化":         ("建議現價出場",   "🚪"),
        "🟢 TP1已完成":        ("繼續持有等TP2",  "⏳"),
        "🔄 持倉中":           ("繼續持有",       "✅"),
        "⏳ 等待進場":         ("等待限價單成交", "⏳"),
    }

    for asset, asset_alerts in alerts_by_asset.items():
        try:
            inst_id = asset + '-USDT-SWAP'
            cp = get_current_price(inst_id)
            price_str = format_price(cp) if cp else "—"
            msg += f"<b>{asset}</b>  現價 <code>{price_str}</code>\n"

            for i, (pos, status, action) in enumerate(asset_alerts, 1):
                d   = "🟩<b>多</b>" if pos['dir'] == "多" else "🟥<b>空</b>"
                sub = f"#{i} " if len(asset_alerts) > 1 else ""
                msg += f"{sub}{d}  {pos['tf']}  {status}\n"
                msg += "<pre>"
                msg += f"進場  {format_price(pos['entry'])}\n"
                msg += f"止損  {format_price(pos['sl'])}\n"
                msg += "</pre>"
                msg += f"▸ {action}\n"
                if status in ("🚫 掛單已取消", "⏰ 掛單逾時取消"):
                    msg += f"<i>（掛單已自動取消並移除追蹤）</i>\n"
                elif status in ("🔴 止損觸發", "🛡️ 回調至保本止損", "🟣 全部止盈"):
                    rec_text, rec_icon = REC.get(status, ("自行判斷", "❓"))
                    msg += f"<b>【持倉建議】{rec_icon} {rec_text}</b>\n"
                    msg += f"<i>（此持倉已自動移除追蹤）</i>\n"
                else:
                    rec_text, rec_icon = REC.get(status, ("自行判斷", "❓"))
                    msg += f"<b>【持倉建議】{rec_icon} {rec_text}</b>\n"

            conclusion = build_coin_conclusion(all_by_asset[asset], cp)
            if conclusion:
                msg += f"{conclusion}\n"
            msg += "─────────────────────────\n"
        except Exception as _ae:
            print(f"⚠️ 警報組合錯誤 [{asset}]：{_ae}")
            msg += f"<i>（{asset} 狀態組合時發生錯誤，請稍後再查）</i>\n"
            msg += "─────────────────────────\n"

    msg += "<i>以上為自動監控建議，請結合自身判斷操作。</i>"

    # B. 監控警報底部加「已了解」按鈕
    monitor_markup = {"inline_keyboard": [[{"text": "📌 已了解", "callback_data": "ack_monitor"}]]}
    text_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    resp = requests.post(text_url, json={
        "chat_id": str(TELEGRAM_CHAT_ID), "text": msg,
        "parse_mode": "HTML", "reply_markup": monitor_markup
    })
    if resp.json().get("ok"):
        print(f"✅ 持倉監控警報已發送（{len(alerts)} 筆）")
    else:
        print(f"❌ 持倉監控警報發送失敗：{resp.json()}")

# ==================== 🚀 6. 動態精度渲染發送引擎 ====================
# ==================== 🔍 指定幣種即時分析 ====================
def _build_strategy_entries(inst_id, s1, s4, s1d):
    """
    計算 4 大策略的進場點位（不依賴現價，依趨勢推算合理限價掛單位）。
    回傳 dict，失敗回傳 None。
    """
    try:
        url4h = f"{BASE_URL}/api/v5/market/candles?instId={inst_id}&bar=4H&limit=150"
        r4h   = requests.get(url4h, timeout=5).json()
        if r4h.get('code') != '0' or not r4h.get('data'):
            return None
        df = pd.DataFrame(r4h['data'],
                          columns=['ts','open','high','low','close','vol','volCcy','volCcyQuote','state'])
        for col in ['open','high','low','close']:
            df[col] = df[col].astype(float)
        df = df.iloc[::-1].reset_index(drop=True)
    except Exception:
        return None

    # ── 基礎指標 ──
    atr  = s4['atr']  if s4  else (s1['atr']  if s1 else df['close'].iloc[-1] * 0.01)
    is_bull = bool(s4['ma_above']) if s4 else (bool(s1['ma_above']) if s1 else True)
    recent_high = (s4 or s1)['recent_high']
    recent_low  = (s4 or s1)['recent_low']

    # ── 策略① 趨勢突破 EMA（4H MA8 回踩限價）──
    ma8_4h  = s4['ma8']   if s4 else s1['ma8']
    ema89_4h = s4['ema89'] if s4 else s1['ema89']
    if is_bull:
        ema_entry = round(ma8_4h, 8)            # 回踩 MA8 掛多
        ema_sl    = ema_entry - atr * 1.5
        ema_tp1   = ema_entry + atr * 2.0
        ema_tp2   = ema_entry + atr * 4.0
    else:
        ema_entry = round(ma8_4h, 8)            # 反彈至 MA8 掛空
        ema_sl    = ema_entry + atr * 1.5
        ema_tp1   = ema_entry - atr * 2.0
        ema_tp2   = ema_entry - atr * 4.0

    # ── 策略② 區間反轉 RSI（近期高低點±緩衝）──
    range_long_entry  = recent_low  + atr * 0.3
    range_short_entry = recent_high - atr * 0.3
    range_sl_buf      = atr * 1.2

    # ── 策略③ 多因子 Fib + MACD ──
    window     = min(60, len(df))
    swing_high = df['high'].iloc[-window:].max()
    swing_low  = df['low'].iloc[-window:].min()
    sw_range   = swing_high - swing_low + 1e-10
    fib236 = swing_high - 0.236 * sw_range
    fib382 = swing_high - 0.382 * sw_range
    fib500 = swing_high - 0.500 * sw_range
    fib618 = swing_high - 0.618 * sw_range
    fib786 = swing_high - 0.786 * sw_range

    ema12  = df['close'].ewm(span=12, adjust=False).mean()
    ema26  = df['close'].ewm(span=26, adjust=False).mean()
    macd_l = ema12 - ema26
    sig_l  = macd_l.ewm(span=9, adjust=False).mean()
    hist   = macd_l - sig_l
    macd_bull  = float(hist.iloc[-1]) > 0
    macd_cross = float(hist.iloc[-1]) > 0 and float(hist.iloc[-2]) <= 0

    if is_bull:
        fib_entry = fib618 if fib618 > swing_low else fib500
        fib_sl    = fib_entry - atr * 1.2
        fib_tp1   = fib382
        fib_tp2   = fib236
    else:
        fib_entry = fib236 if fib236 < swing_high else fib382
        fib_sl    = fib_entry + atr * 1.2
        fib_tp1   = fib500
        fib_tp2   = fib618

    # ── 策略④ SMC/ICT 訂單塊（OB）──
    # 看漲OB：趨勢向上時，找最後一根下跌K（空方被消化前的最後抵抗）
    # 看跌OB：趨勢向下時，找最後一根上漲K
    recent_df = df.iloc[-20:-1]
    if is_bull:
        bear_c = recent_df[recent_df['close'] < recent_df['open']]
        if not bear_c.empty:
            obc     = bear_c.iloc[-1]
            ob_low  = float(obc['low'])
            ob_high = float(obc['open'])   # 下跌K開盤 = OB頂
        else:
            ob_low  = recent_low
            ob_high = recent_low + atr * 0.5
        ob_sl   = ob_low - atr * 0.5
        ob_tp1  = ob_high + atr * 2.0
    else:
        bull_c = recent_df[recent_df['close'] > recent_df['open']]
        if not bull_c.empty:
            obc     = bull_c.iloc[-1]
            ob_low  = float(obc['close'])  # 上漲K收盤 = OB底
            ob_high = float(obc['high'])
        else:
            ob_low  = recent_high - atr * 0.5
            ob_high = recent_high
        ob_sl   = ob_high + atr * 0.5
        ob_tp1  = ob_low - atr * 2.0

    return {
        'is_bull': is_bull,
        'atr': atr,
        # S1
        'ema_entry': ema_entry, 'ema_sl': ema_sl,
        'ema_tp1': ema_tp1, 'ema_tp2': ema_tp2,
        # S2
        'range_long_entry': range_long_entry,
        'range_short_entry': range_short_entry,
        'range_sl_buf': range_sl_buf,
        'recent_high': recent_high, 'recent_low': recent_low,
        # S3
        'swing_high': swing_high, 'swing_low': swing_low,
        'fib236': fib236, 'fib382': fib382, 'fib500': fib500,
        'fib618': fib618, 'fib786': fib786,
        'fib_entry': fib_entry, 'fib_sl': fib_sl,
        'fib_tp1': fib_tp1, 'fib_tp2': fib_tp2,
        'macd_bull': macd_bull, 'macd_cross': macd_cross,
        # S4
        'ob_low': ob_low, 'ob_high': ob_high,
        'ob_sl': ob_sl, 'ob_tp1': ob_tp1,
    }


def analyze_coin_snapshot(inst_id, bar_param="1H"):
    """分析指定幣種當前狀態（不需要發生交叉），回傳完整快照"""
    url = f"{BASE_URL}/api/v5/market/candles?instId={inst_id}&bar={bar_param}&limit=100"
    try:
        res = requests.get(url, timeout=3.0).json()
        if not (res.get('code') == '0' and len(res['data']) >= 50):
            return None
        df = pd.DataFrame(res['data'], columns=['ts','open','high','low','close','vol','volCcy','volCcyQuote','state'])
        for col in ['open','high','low','close','vol']:
            df[col] = df[col].astype(float)
        df = df.iloc[::-1].reset_index(drop=True)

        df['MA8']   = df['close'].rolling(8).mean()
        df['EMA89'] = df['close'].ewm(span=89, adjust=False).mean()

        delta = df['close'].diff()
        gain  = delta.where(delta > 0, 0).rolling(14).mean()
        loss  = (-delta.where(delta < 0, 0)).rolling(14).mean()
        df['RSI'] = 100 - (100 / (1 + gain / (loss + 1e-10)))

        df['H-L']   = df['high'] - df['low']
        df['H-PC']  = (df['high'] - df['close'].shift(1)).abs()
        df['L-PC']  = (df['low']  - df['close'].shift(1)).abs()
        df['TR']    = df[['H-L','H-PC','L-PC']].max(axis=1)
        df['ATR14'] = df['TR'].rolling(14).mean()

        plus_dm  = df['high'].diff().clip(lower=0)
        minus_dm = (-df['low'].diff()).clip(lower=0)
        mask = plus_dm >= minus_dm
        plus_dm_c  = plus_dm.where(mask, 0)
        minus_dm_c = minus_dm.where(~mask, 0)
        atr14 = df['ATR14'] + 1e-10
        plus_di  = 100 * plus_dm_c.rolling(14).mean() / atr14
        minus_di = 100 * minus_dm_c.rolling(14).mean() / atr14
        dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di + 1e-10)
        df['ADX'] = dx.rolling(14).mean()

        c = df.iloc[-1]
        p = df.iloc[-2]

        # 近期高低點（支撐/壓力參考）
        recent_high = df['high'].iloc[-20:].max()
        recent_low  = df['low'].iloc[-20:].min()

        is_cross_up   = (p['MA8'] <= p['EMA89']) and (c['MA8'] > c['EMA89'])
        is_cross_down = (p['MA8'] >= p['EMA89']) and (c['MA8'] < c['EMA89'])

        return {
            'price':    c['close'],
            'ma8':      c['MA8'],
            'ema89':    c['EMA89'],
            'rsi':      c['RSI'],
            'adx':      c['ADX'],
            'atr':      c['ATR14'],
            'ma_above': c['MA8'] > c['EMA89'],    # True=多頭排列
            'cross_up':   is_cross_up,
            'cross_down': is_cross_down,
            'recent_high': recent_high,
            'recent_low':  recent_low,
        }
    except:
        return None

def send_coin_analysis(asset_input, chat_id):
    """處理 /eth /btc /sol 等指令，發送即時分析報告（1H/4H/1D/1W 四框綜合）"""
    symbol = asset_input.upper().strip('/')
    inst_id = f"{symbol}-USDT-SWAP"

    # 先確認幣種存在
    check_url = f"{BASE_URL}/api/v5/market/candles?instId={inst_id}&bar=1H&limit=3"
    try:
        chk = requests.get(check_url, timeout=3.0).json()
        if chk.get('code') != '0' or not chk.get('data'):
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": chat_id, "text": f"❌ 找不到 {inst_id}，請確認幣種名稱（例如：/eth /btc /sol）"}
            )
            return
    except:
        pass

    s1  = analyze_coin_snapshot(inst_id, "1H")
    s4  = analyze_coin_snapshot(inst_id, "4H")
    s1d = analyze_coin_snapshot(inst_id, "1D")
    s1w = analyze_coin_snapshot(inst_id, "1W")
    funding_rate, ls_ratio = get_market_sentiment(inst_id)

    if not s1 or not s4:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": f"⚠️ {symbol} 數據獲取失敗，請稍後再試"}
        )
        return

    price     = s1['price']
    long_pct  = round(ls_ratio / (ls_ratio + 1) * 100)
    short_pct = 100 - long_pct

    # ── 單框趨勢文字 ──
    def trend_label(s, tf_name):
        if s is None:
            return f"<b>{tf_name}</b>  ⬜ 數據不足"
        if s['adx'] < 20:
            return f"<b>{tf_name}</b>  ⬜ 盤整  RSI {s['rsi']:.0f}"
        elif s['ma_above']:
            bar = "■■■■" if s['adx'] >= 50 else ("■■■□" if s['adx'] >= 30 else ("■■□□" if s['adx'] >= 20 else "■□□□"))
            return f"<b>{tf_name}</b>  🟩多 {bar}  RSI {s['rsi']:.0f}"
        else:
            bar = "■■■■" if s['adx'] >= 50 else ("■■■□" if s['adx'] >= 30 else ("■■□□" if s['adx'] >= 20 else "■□□□"))
            return f"<b>{tf_name}</b>  🟥空 {bar}  RSI {s['rsi']:.0f}"

    # ── 四框綜合評分（加權投票） ──
    # 權重：1W=4, 1D=3, 4H=2, 1H=1（大週期決定方向）
    def frame_vote(s, weight):
        if s is None or s['adx'] < 20:   # 與主掃描一致：<20 = 盤整，不納入投票
            return 0
        return weight if s['ma_above'] else -weight

    score = (frame_vote(s1w, 4) + frame_vote(s1d, 3) +
             frame_vote(s4,  2) + frame_vote(s1,  1))
    # score 範圍：-10 ~ +10

    # ── 綜合結論 ──
    def build_conclusion(score, s1, s4, s1d, s1w):
        lines = []

        if score >= 7:
            lines.append("🚀 <b>強烈多頭共識（四框一致向上）</b>")
            lines.append(f"  適合做多，1H限價掛 <b>{format_price(s1['ma8'])}</b>（MA8）")
            lines.append(f"  止損（最近支撐）：<b>{format_price(s1['sl'])}</b>  TP1：<b>{format_price(s1['tp1'])}</b> / TP2：<b>{format_price(s1['tp2'])}</b>")
            lines.append(f"  轉空觀察：等1D MA8跌破EMA89 <b>{format_price(s1d['ema89'])}</b>")

        elif score >= 4:
            # 大週期多，短週期可能分歧
            lines.append("✅ <b>中長線偏多，短線須謹慎</b>")
            if s1 and not s1['ma_above']:
                lines.append(f"  1H暫時偏空，等1H轉多（突破 <b>{format_price(s1['ema89'])}</b>）再進場")
            elif s1 and s1['rsi'] > 68:
                lines.append(f"  1H RSI過熱({s1['rsi']:.0f})，等回踩 <b>{format_price(s1['ma8'])}</b> 再進多")
            else:
                lines.append(f"  可輕倉做多，1H限價 <b>{format_price(s1['ma8'])}</b>")
            if s1d:
                lines.append(f"  做空需等日線轉空（EMA89：<b>{format_price(s1d['ema89'])}</b>）")

        elif score >= 1:
            lines.append("⚠️ <b>大週期分歧，建議觀望為主</b>")
            if s1w and s1w['ma_above']:
                lines.append(f"  週線偏多但日線未確認，等1D MA8站上EMA89 <b>{format_price(s1d['ema89'] if s1d else 0)}</b>")
            else:
                lines.append(f"  多空力量接近，方向不明，避免重倉")
            lines.append(f"  做多觀察點：突破近期高點 <b>{format_price(s1['recent_high'])}</b>")
            lines.append(f"  做空觀察點：跌破近期低點 <b>{format_price(s1['recent_low'])}</b>")

        elif score == 0:
            lines.append("⬜ <b>多空均衡，建議觀望</b>")
            lines.append(f"  突破 <b>{format_price(s1['recent_high'])}</b> 偏多，跌破 <b>{format_price(s1['recent_low'])}</b> 偏空")
            if s1d:
                lines.append(f"  日線 EMA89：<b>{format_price(s1d['ema89'])}</b> 為中線關鍵支撐/壓力")

        elif score >= -3:
            lines.append("⚠️ <b>大週期分歧，建議觀望為主</b>")
            if s1w and not s1w['ma_above']:
                lines.append(f"  週線偏空但日線未確認，等1D跌破EMA89 <b>{format_price(s1d['ema89'] if s1d else 0)}</b>")
            lines.append(f"  做空觀察點：跌破近期低點 <b>{format_price(s1['recent_low'])}</b>")
            lines.append(f"  做多觀察點：反彈突破 <b>{format_price(s1['recent_high'])}</b>")

        elif score >= -6:
            lines.append("✅ <b>中長線偏空，短線須謹慎</b>")
            if s1 and s1['ma_above']:
                lines.append(f"  1H暫時偏多，等1H轉空（跌破 <b>{format_price(s1['ema89'])}</b>）再進空")
            elif s1 and s1['rsi'] < 32:
                lines.append(f"  1H RSI超賣({s1['rsi']:.0f})，等反彈至 <b>{format_price(s1['ma8'])}</b> 再進空")
            else:
                lines.append(f"  可輕倉做空，1H限價 <b>{format_price(s1['ma8'])}</b>")
            if s1d:
                lines.append(f"  做多需等日線轉多（EMA89：<b>{format_price(s1d['ema89'])}</b>）")

        else:  # score <= -7
            lines.append("📉 <b>強烈空頭共識（四框一致向下）</b>")
            lines.append(f"  適合做空，1H限價掛 <b>{format_price(s1['ma8'])}</b>（MA8）")
            lines.append(f"  止損（最近壓力）：<b>{format_price(s1['sl'])}</b>  TP1：<b>{format_price(s1['tp1'])}</b> / TP2：<b>{format_price(s1['tp2'])}</b>")
            lines.append(f"  轉多觀察：等1D MA8突破EMA89 <b>{format_price(s1d['ema89'])}</b>")

        return lines

    conclusion_lines = build_conclusion(score, s1, s4, s1d, s1w)

    # ── 資金費率情緒 ──
    sentiment_note_text = build_sentiment_note("多", funding_rate, ls_ratio)[0]
    if '主力偏多' in sentiment_note_text:
        sentiment_tag = "偏多"
    elif '主力偏空' in sentiment_note_text:
        sentiment_tag = "偏空"
    else:
        sentiment_tag = "中性"
    fr_pct = funding_rate * 100

    # ── 評分視覺化（-10 到 +10 → 進度條） ──
    score_bar_val = max(0, min(10, score + 5))  # 0~10
    score_bar = "█" * score_bar_val + "░" * (10 - score_bar_val)
    score_tag  = ("強多" if score >= 7 else
                  "偏多" if score >= 4 else
                  "微多" if score >= 1 else
                  "均衡" if score == 0 else
                  "微空" if score >= -3 else
                  "偏空" if score >= -6 else "強空")

    # ── 組裝訊息 ──
    msg  = f"🔍 <b>{symbol} 多週期分析</b>\n"
    msg += f"<pre>現價  {format_price(price)}</pre>\n\n"
    msg += "<b>── 各週期趨勢 ──</b>\n"
    msg += trend_label(s1,  "1H") + "\n"
    msg += trend_label(s4,  "4H") + "\n"
    msg += trend_label(s1d, "1D") + "\n"
    msg += trend_label(s1w, "1W") + "\n\n"
    msg += f"<b>綜合評分</b>  [{score_bar}]  <b>{score_tag}</b> ({score:+d}/10)\n\n"
    msg += "<b>─── 操作建議 ───</b>\n"
    for line in conclusion_lines:
        msg += f"{line}\n"
    msg += "\n"
    msg += f"<b>主力情緒</b>  {sentiment_tag}  費率{fr_pct:+.4f}%\n"
    msg += f"<b>多空比</b>    多{long_pct}%：空{short_pct}%\n"

    # ── 有效時限（依四框一致性加權判斷） ──
    la_tz = pytz.timezone('America/Los_Angeles')
    now_la = datetime.datetime.now(la_tz)

    # 預算各時框收盤時間
    next_1h  = now_la.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(hours=1)
    cur_4h   = (now_la.hour // 4) * 4
    next_4h  = now_la.replace(hour=cur_4h, minute=0, second=0, microsecond=0) + datetime.timedelta(hours=4)
    next_1d  = (now_la + datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

    # 計算有多少大週期與結論同向
    bull_conclusion = score > 0
    def tf_aligned(s):
        return s is not None and s['adx'] >= 20 and (s['ma_above'] == bull_conclusion)

    large_tf_count = sum([
        tf_aligned(s1w),   # 1W：最穩定
        tf_aligned(s1d),   # 1D
        tf_aligned(s4),    # 4H
    ])

    # 有效期規則：
    #   3框（4H+1D+1W）同向 → 有效至下根1D收盤（趨勢最穩定）
    #   2框（任意兩個大框）同向 → 有效至下根4H收盤
    #   1框（僅4H或僅1D）同向 → 有效至下根4H收盤
    #   0框（僅1H或全盤整）→ 有效至下根1H收盤
    if score == 0 or (s1 and s1['adx'] < 18 and large_tf_count == 0):
        expire_time = next_1h
        expire_note = f"下根1H收盤 {next_1h.strftime('%H:%M')} PT（多空均衡，短線為主）"
    elif large_tf_count == 0:
        expire_time = next_1h
        expire_note = f"下根1H收盤 {next_1h.strftime('%H:%M')} PT（僅1H信號，短線為主）"
    elif large_tf_count == 1:
        expire_time = next_4h
        expire_note = f"下根4H收盤 {next_4h.strftime('%H:%M')} PT（單一大框確認）"
    elif large_tf_count == 2:
        expire_time = next_4h
        expire_note = f"下根4H收盤 {next_4h.strftime('%H:%M')} PT（兩框同向確認）"
    else:  # large_tf_count == 3
        expire_time = next_1d
        expire_note = f"下根1D收盤 {next_1d.strftime('%m/%d %H:%M')} PT（三框高度一致）"

    # ── 4 大策略進場參考 ──
    se = _build_strategy_entries(inst_id, s1, s4, s1d)
    if se:
        bull       = se['is_bull']
        d_long     = "🟩多" if bull else "🟥空"
        d_short    = "🟥空" if bull else "🟩多"
        macd_tag   = ("🟩金叉↑" if se['macd_cross'] else
                      "🟩看漲"  if se['macd_bull']  else "🟥看跌")
        ob_type    = "看漲OB" if bull else "看跌OB"

        msg += "\n<b>── 4 大策略進場參考（4H趨勢基準）──</b>\n"

        # ① 趨勢突破 EMA
        msg += f"\n①<b>趨勢突破 EMA</b>  {d_long}  回踩4H MA8限價\n<pre>"
        msg += f"進場  {format_price(se['ema_entry'])}\n"
        msg += f"止損  {format_price(se['ema_sl'])}\n"
        msg += f"TP1   {format_price(se['ema_tp1'])}  TP2  {format_price(se['ema_tp2'])}\n"
        msg += "</pre>"

        # ② 區間反轉 RSI
        msg += f"\n②<b>區間反轉 RSI</b>  高低點邊界\n<pre>"
        msg += f"做多  {format_price(se['range_long_entry'])}（近期低 {format_price(se['recent_low'])}）RSI≤30\n"
        msg += f"做空  {format_price(se['range_short_entry'])}（近期高 {format_price(se['recent_high'])}）RSI≥70\n"
        msg += f"止損  進場後 ±{format_price(se['range_sl_buf'])}（ATR×1.2）\n"
        msg += "</pre>"

        # ③ 多因子 Fib + MACD
        msg += f"\n③<b>多因子 Fib+MACD</b>  {macd_tag}\n<pre>"
        msg += f"波段高  {format_price(se['swing_high'])}  波段低  {format_price(se['swing_low'])}\n"
        msg += f"0.236   {format_price(se['fib236'])}  0.382  {format_price(se['fib382'])}\n"
        msg += f"0.500   {format_price(se['fib500'])}  0.618  {format_price(se['fib618'])}\n"
        msg += f"建議進場  {format_price(se['fib_entry'])}（{d_long} Fib回踩）\n"
        msg += f"止損  {format_price(se['fib_sl'])}\n"
        msg += f"TP1   {format_price(se['fib_tp1'])}  TP2  {format_price(se['fib_tp2'])}\n"
        msg += "</pre>"

        # ④ SMC/ICT 訂單塊
        msg += f"\n④<b>SMC/ICT 訂單塊</b>\n<pre>"
        msg += f"{ob_type}  {format_price(se['ob_low'])} ~ {format_price(se['ob_high'])}\n"
        msg += f"進場  等回測OB後出現確認K（吞噬/針形）\n"
        msg += f"止損  {format_price(se['ob_sl'])}\n"
        msg += f"TP1   {format_price(se['ob_tp1'])}\n"
        msg += "</pre>"

    msg += "\n"
    msg += f"<i>🕐 分析時間：{now_la.strftime('%m/%d %H:%M')} PT</i>\n"
    msg += f"<i>⏰ 有效至：{expire_note}，超時請重新輸入 /{symbol} 更新</i>\n"

    # 若訊息超過 Telegram 4096 限制，截斷策略區段後補末尾
    _TG_MAX = 4000
    if len(msg) > _TG_MAX:
        cut = msg.rfind("\n\n<b>──", 0, _TG_MAX)
        if cut == -1:
            cut = _TG_MAX
        tail = f"\n\n<i>🕐 {now_la.strftime('%m/%d %H:%M')} PT  |  ⏰ {expire_note}</i>"
        msg = msg[:cut] + tail

    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"}
    )

def get_tick_sz(inst_id: str) -> float:
    """回傳 OKX 合約最小價格精度（tickSz），無資料時回傳 0"""
    return _tick_size_map.get(inst_id, 0.0)

def round_to_tick(price: float, inst_id: str) -> float:
    """將價格捨入至 OKX 合約的最小 tick size（避免 OKX 拒單）"""
    tick = get_tick_sz(inst_id)
    if tick <= 0:
        return price
    s = f"{tick:.12f}".rstrip('0')
    decimals = len(s.split('.')[1]) if '.' in s else 0
    return round(round(price / tick) * tick, decimals)

def format_price(p, inst_id=None):
    """
    依 OKX tickSz 或價格區間自動選擇小數位數：
    inst_id 有值時優先用 tickSz → BTC 1位、ETH/SOL 2位、XRP/ADA 4位等
    無 inst_id 時 fallback 到價格區間規則（≥$10→2位，$1–10→3位，<$1→動態）
    """
    if p is None or p == 0:
        return "0"
    if inst_id:
        tick = get_tick_sz(inst_id)
        if tick > 0:
            s = f"{tick:.12f}".rstrip('0')
            decimals = len(s.split('.')[1]) if '.' in s else 0
            rounded = round(round(p / tick) * tick, decimals)
            return f"{rounded:,.{decimals}f}" if decimals > 0 else f"{int(rounded):,}"
    # fallback（無 inst_id 或 tick 資料未載入時）
    if p >= 10: return f"{p:,.2f}"
    if p >= 1:  return f"{p:,.3f}"
    num_zeros = math.floor(-math.log10(abs(p)))
    precision = num_zeros + 4
    return f"{p:.{precision}f}"

def answer_callback(callback_id, text="", alert=False):
    """回應 Telegram inline button 點擊"""
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",
            json={"callback_query_id": callback_id, "text": text, "show_alert": alert},
            timeout=3
        )
    except Exception:
        pass


def fetch_coin_news(coin: str, max_items: int = 2) -> list[str]:
    """用 Google News RSS 抓取幣種相關新聞標題（繁中優先，免 key）"""
    try:
        query   = requests.utils.quote(f"{coin} crypto")
        url     = (f"https://news.google.com/rss/search"
                   f"?q={query}&hl=zh-TW&gl=TW&ceid=TW:zh-Hant")
        resp    = requests.get(url, timeout=5,
                               headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code != 200:
            return []
        root    = ET.fromstring(resp.content)
        items   = root.findall(".//item")
        titles  = []
        for item in items[:max_items]:
            t = item.findtext("title") or ""
            # Google News title 格式常帶 "- 媒體名"，把尾巴截掉
            t = t.rsplit(" - ", 1)[0].strip()
            if t:
                titles.append(t)
        return titles
    except Exception:
        return []


def send_html_report_via_requests(valid_signals, mode_title="實時雷達速報", target_chat_id=None, include_news=False, auto_track=False):
    if target_chat_id is None:
        target_chat_id = TELEGRAM_CHAT_ID

    la_tz = pytz.timezone('America/Los_Angeles')
    now_str = datetime.datetime.now(la_tz).strftime('%Y-%m-%d %H:%M')

    html_message = f"<b>【{mode_title}】</b>  <code>{now_str} PT</code>\n"
    html_message += "─────────────────────────\n"

    for idx, item in enumerate(valid_signals, 1):
        win_rate = item.get('win_rate', 70)
        # 星號代表 TA 品質等級（非歷史勝率）
        if win_rate >= 88:   stars = "★★★★★"
        elif win_rate >= 82: stars = "★★★★"
        elif win_rate >= 75: stars = "★★★"
        elif win_rate >= 65: stars = "★★"
        else:                stars = "★"

        adx_val = item.get('adx', 0)
        if adx_val >= 50:   adx_level, adx_bar = "強", "■■■■"
        elif adx_val >= 30: adx_level, adx_bar = "中", "■■■□"
        elif adx_val >= 20: adx_level, adx_bar = "低", "■■□□"
        else:               adx_level, adx_bar = "弱", "■□□□"
        medal   = ["🥇","🥈","🥉","#4","#5"][idx-1]

        # 方向：用顏色+文字，不堆疊其他圖示
        dir_display = "🟩<b>多</b>" if item['dir'] == "多" else "🟥<b>空</b>"

        # 主力動向精簡：方向標籤 + 費率數字 + 多空比百分比
        sentiment = item.get('sentiment_note','')
        if '主力偏多' in sentiment:   dir_tag = "偏多"
        elif '主力偏空' in sentiment: dir_tag = "偏空"
        else:                         dir_tag = "中性"
        # 只取費率數字（去除括號和前綴文字）
        fr_match   = next((p for p in sentiment.split('，') if '費率' in p), '')
        fr_num_str = fr_match.split('費率')[-1].strip('（）').strip() if fr_match else '0%'
        ls_ratio   = item.get('ls_ratio', 1.0)
        long_pct   = round(ls_ratio / (ls_ratio + 1) * 100)
        short_pct  = 100 - long_pct
        sentiment_short = f"{dir_tag}  費率{fr_num_str}  多{long_pct}%:空{short_pct}%"

        # ── 標題行 ──
        sig_type = item.get('signal_type', 'trend')
        type_badge = {"trend": "📈趨勢", "range": "↔️區間", "divergence": "🔄背離", "smc": "🏦SMC"}.get(sig_type, "📈趨勢")
        html_message += (f"{medal} <b>{item['asset']}</b>  {dir_display}  "
                         f"⚡<b>{item['leverage']}</b>  {item['tf']}  "
                         f"{type_badge}  TA分<b>{win_rate}</b> {stars}\n")
        # ── 策略標籤 + 脈絡資訊（依訊號類型顯示不同內容）──
        sig_type = item.get('signal_type', 'trend')
        if sig_type == 'range':
            ctx_label = "區間"
            sup = item.get('range_sup', [0, 0])
            res = item.get('range_res', [0, 0])
            ctx_extra = (f"支撐[{format_price(sup[0])}–{format_price(sup[1])}]"
                         f"→壓力[{format_price(res[0])}–{format_price(res[1])}]")
        elif sig_type == 'divergence':
            ctx_label = "背離"
            ctx_extra = item.get('div_desc', '')
        elif sig_type == 'smc':
            ctx_label = "SMC"
            ctx_extra = item.get('entry_type', '').replace(f"{item.get('tf','')} ", '')
        else:
            ctx_label = "趨勢"
            ctx_extra = item.get('tf_note', '')
        html_message += f"{ctx_label} {adx_bar} <b>{adx_level}</b>  {ctx_extra}  ｜  {sentiment_short}\n"
        # ── 分隔 ──
        html_message += "┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
        # ── 進場 / 止損 / TP（等寬對齊，CJK=2格 ASCII=1格，統一補至6格） ──
        _tc = item.get('tp_count', 3)
        _mkt_tag = "(市價)" if item.get('is_market_entry') else ""
        html_message += "<pre>"
        html_message += f"進場  {format_price(item['entry'])}{_mkt_tag}\n"
        html_message += f"止損  {format_price(item['sl'])}\n"
        if _tc == 1:
            html_message += f"TP1   {format_price(item['tp1'])}  (全倉平倉)\n"
        elif _tc == 2:
            html_message += f"TP1   {format_price(item['tp1'])}  (平倉50%)\n"
            html_message += f"TP2   {format_price(item['tp2'])}  (平倉50%)\n"
        else:
            html_message += f"TP1   {format_price(item['tp1'])}  ← 平倉40%｜SL移保本\n"
            html_message += f"TP2   {format_price(item['tp2'])}  ← 平倉40%｜SL鎖TP1\n"
            html_message += f"TP3   {format_price(item['tp3'])}  ← 20%移動止盈(ATR×2.5)\n"
        html_message += "</pre>"
        # ── 分倉操作說明（含費後保本點）──
        if _tc >= 3:
            _f_msg = 0.0005
            _e_msg, _t1_msg = item['entry'], item['tp1']
            if item['dir'] == '多':
                _sl_be_msg = max(_e_msg, 2 * _e_msg * (1 + _f_msg) / (1 - _f_msg) - _t1_msg)
            else:
                _sl_be_msg = min(_e_msg, 2 * _e_msg * (1 - _f_msg) / (1 + _f_msg) - _t1_msg)
            html_message += (f"▸ TP1達標 → <b>平倉40%</b>，止損移至 <code>{format_price(_sl_be_msg)}</code>（保本）\n"
                             f"▸ TP2達標 → <b>再平倉40%</b>，止損鎖至TP1，剩20%啟動移動止盈\n")
        elif _tc == 2:
            _f_msg = 0.0005
            _e_msg, _t1_msg = item['entry'], item['tp1']
            if item['dir'] == '多':
                _sl_be_msg = max(_e_msg, 2 * _e_msg * (1 + _f_msg) / (1 - _f_msg) - _t1_msg)
            else:
                _sl_be_msg = min(_e_msg, 2 * _e_msg * (1 - _f_msg) / (1 + _f_msg) - _t1_msg)
            html_message += f"▸ TP1達標 → 平倉50%，止損移至 <code>{format_price(_sl_be_msg)}</code>（含手續費保本）\n"
        # ── 斐波那契回撤匯流 ──
        _fib_lbl  = item.get('fib_level')
        _fib_near = item.get('fib_near', False)
        if _fib_lbl:
            _fib_px   = item.get('fib_price', 0)
            _fib_dist = item.get('fib_dist', 0)
            _fib_inst = item['asset'] + '-USDT-SWAP'
            if _fib_near:
                html_message += f"📐 Fib <b>{_fib_lbl}</b>  <code>{format_price(_fib_px, _fib_inst)}</code>  ✅ 斐波那契匯流\n"
            else:
                html_message += f"📐 Fib {_fib_lbl}  <code>{format_price(_fib_px, _fib_inst)}</code>  ({_fib_dist:.1f}% away)\n"
        # ── POC 成交量聚集點 ──
        _poc_lbl = item.get('poc_label', '')
        if _poc_lbl:
            html_message += f"🎯 {_poc_lbl}\n"
        # ── FVG 公平價值缺口 ──
        _fvg_lbl = item.get('fvg_label', '')
        if _fvg_lbl:
            html_message += f"↔️ {_fvg_lbl}\n"
        # ── 時事新聞（僅定時播報）──
        if include_news:
            news_items = fetch_coin_news(item['asset'])
            if news_items:
                html_message += "📰 <i>"
                html_message += "  ·  ".join(news_items)
                html_message += "</i>\n"
        html_message += "─────────────────────────\n"

    html_message += ("<i>TA分 = 各策略技術指標加權評分（趨勢：ADX＋RSI＋量能＋多框確認 | 區間：RSI位置＋拒絕影線＋區間寬度 | 背離：背離幅度＋影線＋超買超賣）\n"
                    "此為即時技術品質評分，★★★★★ 代表當前指標組合最佳，非回測勝率；槓桿僅供參考，請自行控管風險</i>")

    # A. 追蹤快捷按鈕（定時：已自動監控；手動：點擊追蹤）
    if auto_track:
        buttons = [[{"text": f"📌 已自動監控 {item['asset']}{item['dir']}", "callback_data": "ack_monitor"}]
                   for item in valid_signals]
    else:
        buttons = [[{"text": f"✅ 追蹤 {item['asset']}{item['dir']}", "callback_data": f"open_{item['asset']}_{item['dir']}"}]
                   for item in valid_signals]
    reply_markup = {"inline_keyboard": buttons}

    text_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    resp = requests.post(text_url, json={
        "chat_id": str(target_chat_id), "text": html_message,
        "parse_mode": "HTML", "reply_markup": reply_markup
    })
    result = resp.json()
    if result.get("ok"):
        print(f"✅ 精選 5 強報告發送成功 → chat_id={target_chat_id}")
    else:
        print(f"❌ 報告發送失敗：{result}")

# ==================== 📐 Fib 多級別全市場掃描（附掛 /scan）====================

def _fib_structural_swings(df, is_bull, atr, lookback=150, min_move_atr=1.5):
    """
    Zigzag 結構算法，專門抓「上一個兩端都已確認的完整波段」作為 Fib 參考。

    邏輯：
      - price 從當前極值反轉 ≥ min_move_atr × ATR 才確認一個結構轉折點
      - 只用「已確認」的 pivot（不含仍在跑的最新段）
      - 多頭：抓最近一個已確認 H→L（上一段大跌），Fib 量那段 → 反彈能到哪
      - 空頭：抓最近一個已確認 L→H（上一段大漲），Fib 量那段 → 回落能到哪
      - 若找不到反向結構，fallback 用最後兩個已確認 pivot

    回傳 (sl_idx, swing_low, sh_idx, swing_high) 或 None。
    """
    threshold = atr * min_move_atr
    start     = max(0, len(df) - lookback)
    sub       = df.iloc[start:].reset_index(drop=True)
    offset    = start

    confirmed: list = []        # 兩端都已確認的結構點 (df_idx, price, 'H'|'L')
    cur_type  = 'H'
    cur_val   = float(sub['high'].iloc[0])
    cur_sub_i = 0

    for i in range(len(sub)):
        h = float(sub['high'].iloc[i])
        l = float(sub['low'].iloc[i])
        if cur_type == 'H':
            if h >= cur_val:
                cur_val, cur_sub_i = h, i
            elif cur_val - l >= threshold:      # 下跌夠大 → 確認這個結構高點
                confirmed.append((offset + cur_sub_i, cur_val, 'H'))
                cur_type, cur_val, cur_sub_i = 'L', l, i
        else:
            if l <= cur_val:
                cur_val, cur_sub_i = l, i
            elif h - cur_val >= threshold:      # 上漲夠大 → 確認這個結構低點
                confirmed.append((offset + cur_sub_i, cur_val, 'L'))
                cur_type, cur_val, cur_sub_i = 'H', h, i
    # cur_type/cur_val/cur_sub_i 是目前還在跑、尚未確認的段 → 不納入

    if len(confirmed) < 2:
        # 已確認點不足，把當前在跑的段也加進來勉強撐一下
        confirmed.append((offset + cur_sub_i, cur_val, cur_type))
        if len(confirmed) < 2:
            return None

    # 相鄰 pair 列表：[(p_older, p_newer), ...]
    pairs = list(zip(confirmed[:-1], confirmed[1:]))

    # 結構性波段最小振幅門檻：range 須 ≥ 3×ATR 才算有效結構段
    # 避免抓到大趨勢內部的微小 sub-swing
    min_struct_range = atr * 3.0

    def _pick_lh(pair_list):
        """從 L→H pair 清單挑選：優先最近的結構性（≥ 3×ATR）pair；
        若全部都太小，退而選 range 最大的那個。"""
        structural = [(p1, p2) for p1, p2 in pair_list
                      if (p2[1] - p1[1]) >= min_struct_range]
        chosen = structural[-1] if structural else max(pair_list, key=lambda x: x[1][1] - x[0][1])
        return chosen

    def _pick_hl(pair_list):
        """從 H→L pair 清單挑選：優先最近的結構性（≥ 3×ATR）pair；
        若全部都太小，退而選 range 最大的那個。"""
        structural = [(p1, p2) for p1, p2 in pair_list
                      if (p1[1] - p2[1]) >= min_struct_range]
        chosen = structural[-1] if structural else max(pair_list, key=lambda x: x[0][1] - x[1][1])
        return chosen

    if is_bull:
        # 多頭主趨勢 L→H：Fib 從頂點往下量回撤支撐位
        lh_pairs = [(p1, p2) for p1, p2 in pairs if p1[2] == 'L' and p2[2] == 'H']
        if lh_pairs:
            (sl_idx, sl_val, _), (sh_idx, sh_val, _) = _pick_lh(lh_pairs)
        else:
            p1, p2 = confirmed[-2], confirmed[-1]
            if p1[2] == 'L':
                sl_idx, sl_val, sh_idx, sh_val = p1[0], p1[1], p2[0], p2[1]
            else:
                sl_idx, sl_val, sh_idx, sh_val = p2[0], p2[1], p1[0], p1[1]
    else:
        # 空頭主趨勢 H→L：Fib 從底點往上量反彈阻力位
        hl_pairs = [(p1, p2) for p1, p2 in pairs if p1[2] == 'H' and p2[2] == 'L']
        if hl_pairs:
            (sh_idx, sh_val, _), (sl_idx, sl_val, _) = _pick_hl(hl_pairs)
        else:
            p1, p2 = confirmed[-2], confirmed[-1]
            if p1[2] == 'H':
                sh_idx, sh_val, sl_idx, sl_val = p1[0], p1[1], p2[0], p2[1]
            else:
                sh_idx, sh_val, sl_idx, sl_val = p2[0], p2[1], p1[0], p1[1]

    if sh_idx == sl_idx:
        return None
    return sl_idx, sl_val, sh_idx, sh_val


def _fib_all_levels_check(inst_id, bar_param="4H"):
    """
    取指定時框 K線，計算 Fib 0.382 / 0.500 / 0.618 / 0.786 四個回撤位，
    判斷現價落在哪一個 ±2% 區間（同時只取最近一個）。
    回傳 dict 或 None。
    """
    _THRESH = {
        'f382': (0.382, 2.0),
        'f500': (0.500, 2.0),
        'f618': (0.618, 2.0),
        'f786': (0.786, 2.5),
    }
    try:
        url = f"{BASE_URL}/api/v5/market/candles?instId={inst_id}&bar={bar_param}&limit=300"
        r   = requests.get(url, timeout=8).json()
        if r.get('code') != '0' or len(r.get('data', [])) < 40:
            return None
        df = pd.DataFrame(r['data'],
                          columns=['ts','open','high','low','close','vol','volCcy','volCcyQuote','state'])
        for col in ['open','high','low','close']:
            df[col] = df[col].astype(float)
        df = df.iloc[::-1].reset_index(drop=True)

        price = float(df['close'].iloc[-1])

        # ── 趨勢方向 ──
        df['MA8']   = df['close'].rolling(8).mean()
        df['EMA89'] = df['close'].ewm(span=89, adjust=False).mean()
        is_bull = float(df['MA8'].iloc[-1]) > float(df['EMA89'].iloc[-1])

        # ── RSI ──
        delta = df['close'].diff()
        gain  = delta.where(delta > 0, 0).rolling(14).mean()
        loss  = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rsi   = float((100 - (100 / (1 + gain / (loss + 1e-10)))).iloc[-1])

        # ── MACD histogram ──
        ema12 = df['close'].ewm(span=12, adjust=False).mean()
        ema26 = df['close'].ewm(span=26, adjust=False).mean()
        hist  = (ema12 - ema26) - (ema12 - ema26).ewm(span=9, adjust=False).mean()
        macd_bull = float(hist.iloc[-1]) > 0

        # ── ATR ──
        df['TR'] = df[['high','low','close']].assign(
            hl=df['high']-df['low'],
            hc=(df['high']-df['close'].shift(1)).abs(),
            lc=(df['low'] -df['close'].shift(1)).abs()
        )[['hl','hc','lc']].max(axis=1)
        atr = float(df['TR'].rolling(14).mean().iloc[-1])

        # ── 分形擺動高低點 ──
        swings = _fib_structural_swings(df, is_bull, atr)
        if swings is None:
            return None
        sl_idx, swing_low, sh_idx, swing_high = swings

        sw_range = swing_high - swing_low
        if sw_range < atr * 0.5:
            return None

        # 多頭：從趨勢低點量到高點，回撤算支撐；空頭：從趨勢高點量到低點，反彈算阻力
        if is_bull:
            fibs = {
                'f382': swing_high - 0.382 * sw_range,
                'f500': swing_high - 0.500 * sw_range,
                'f618': swing_high - 0.618 * sw_range,
                'f786': swing_high - 0.786 * sw_range,
            }
        else:
            fibs = {
                'f382': swing_low + 0.382 * sw_range,
                'f500': swing_low + 0.500 * sw_range,
                'f618': swing_low + 0.618 * sw_range,
                'f786': swing_low + 0.786 * sw_range,
            }

        # ── 找最近的 Fib 級別（在門檻內才算） ──
        best_key, best_dist = None, 999
        for key, (ratio, thresh) in _THRESH.items():
            fib_val = fibs[key]
            dist_pct = abs(price - fib_val) / fib_val * 100
            if dist_pct <= thresh and dist_pct < best_dist:
                best_key  = key
                best_dist = dist_pct

        symbol = inst_id.replace('-USDT-SWAP', '')
        base = {
            'inst_id':    inst_id,
            'symbol':     symbol,
            'price':      price,
            'is_bull':    is_bull,
            'macd_bull':  macd_bull,
            'rsi':        rsi,
            'swing_high': swing_high,
            'swing_low':  swing_low,
            'atr':        atr,
        }

        if best_key is not None:
            return {**base, 'level': best_key, 'fib_val': fibs[best_key], 'dist_pct': best_dist}

        # ── 無近距命中 → 檢查是否已跌破/突破 0.786 防線 ──
        f786_val = fibs['f786']
        if is_bull:
            dist_786 = (f786_val - price) / f786_val * 100   # 正值 = 跌破
        else:
            dist_786 = (price - f786_val) / f786_val * 100   # 正值 = 突破

        if dist_786 > 2.5:          # 超過 ±2.5% 門檻才算確認破位
            return {**base, 'level': 'broken', 'fib_val': f786_val, 'dist_pct': dist_786}

        return None
    except Exception:
        return None


def _build_fib_msg(tf_label, tf_bar, assets, now_str):
    """對單一時框掃描並組裝 Fib 回撤訊息字串。"""
    buckets: dict = {'f382': [], 'f500': [], 'f618': [], 'f786': [], 'broken': []}

    with ThreadPoolExecutor(max_workers=60) as ex:
        futs = {ex.submit(_fib_all_levels_check, a, tf_bar): a for a in assets}
        for fut in as_completed(futs):
            try:
                hit = fut.result()
                if hit:
                    buckets[hit['level']].append(hit)
            except Exception:
                pass

    for k in buckets:
        buckets[k].sort(key=lambda x: x['dist_pct'])

    def _dir(h):
        return "🟩多" if h['is_bull'] else "🟥空"
    def _macd(h):
        ok = (h['macd_bull'] and h['is_bull']) or (not h['macd_bull'] and not h['is_bull'])
        return "MACD✅" if ok else "MACD⚠️"

    total = sum(len(v) for v in buckets.values())
    msg  = f"📐 <b>【Fib 回撤指數 — {tf_label}】</b>  <code>{now_str} PT</code>\n"
    msg += f"流動性合約 {len(assets)} 支 | 命中 {total} 支（±2%）\n"
    msg += "═════════════════════════\n"

    # ── 0.382：強勢格局 ──
    b382 = buckets['f382']
    msg += f"\n🟢 <b>0.382 — 強勢格局（淺回踩）</b>  {len(b382)} 支\n"
    if b382:
        msg += "<i>趨勢完整，僅淺回踩，多空依然強勢</i>\n"
        for h in b382:
            msg += (f"  {_dir(h)} <b>{h['symbol']}</b>  距{h['dist_pct']:.1f}%  "
                    f"RSI {h['rsi']:.0f}  {_macd(h)}\n")
    else:
        msg += "  — 暫無\n"

    # ── 0.500：多空交戰 ──
    b500 = buckets['f500']
    msg += f"\n⚖️ <b>0.500 — 多空交戰（需等確認）</b>  {len(b500)} 支\n"
    if b500:
        msg += "<i>黃金中點，方向未定，需等 K 線確認突破或跌破</i>\n"
        for h in b500:
            d   = _dir(h)
            adv = "多方優勢" if h['is_bull'] else "空方優勢"
            macd_align = (h['macd_bull'] and h['is_bull']) or (not h['macd_bull'] and not h['is_bull'])
            macd_note  = "MACD同向" if macd_align else "MACD背離⚠️"
            if h['is_bull']:
                long_note  = f"守 {format_price(h['fib_val'])} 偏多，跌破轉空"
                short_note = f"跌破 {format_price(h['fib_val'])} 才確立空方"
            else:
                long_note  = f"突破 {format_price(h['fib_val'])} 才確立多方"
                short_note = f"壓於 {format_price(h['fib_val'])} 偏空，突破轉多"
            msg += (f"  {d} <b>{h['symbol']}</b>  距{h['dist_pct']:.1f}%  "
                    f"RSI {h['rsi']:.0f}  <b>{adv}</b>（{macd_note}）\n"
                    f"    多：{long_note}\n"
                    f"    空：{short_note}\n")
    else:
        msg += "  — 暫無\n"

    # ── 0.618：黃金進場區 ──
    b618 = buckets['f618']
    msg += f"\n✅ <b>0.618 — 黃金回踩（可尋機進場）</b>  {len(b618)} 支\n"
    if b618:
        msg += "<i>黃金回撤比例，趨勢方向進場的經典位置</i>\n"
        for h in b618:
            macd_str = "MACD✅ 可找進場K" if _macd(h) == "MACD✅" else "MACD⚠️ 等確認K"
            msg += (f"  {_dir(h)} <b>{h['symbol']}</b>  距{h['dist_pct']:.1f}%  "
                    f"RSI {h['rsi']:.0f}  {macd_str}\n")
    else:
        msg += "  — 暫無\n"

    # ── 0.786：深度回踩仍撐 ──
    b786 = buckets['f786']
    msg += f"\n🔴 <b>0.786 — 深度回踩（最後防線）</b>  {len(b786)} 支\n"
    if b786:
        msg += "<i>深度回撤仍未破位，嚴控止損，等確認K再進</i>\n"
        for h in b786:
            hold_note = "撐住" if h['is_bull'] else "壓住"
            msg += (f"  {_dir(h)} <b>{h['symbol']}</b>  距{h['dist_pct']:.1f}%  "
                    f"RSI {h['rsi']:.0f}  {hold_note}  {_macd(h)}\n")
    else:
        msg += "  — 暫無\n"

    # ── 結構已破壞：跌破/突破 0.786 超過 2.5% ──
    b_broken = buckets['broken']
    msg += f"\n⛔ <b>結構已破壞（0.786 防線失守）</b>  {len(b_broken)} 支\n"
    if b_broken:
        msg += "<i>已確認跌破/突破0.786，趨勢結構受損，建議清倉或反向觀察</i>\n"
        for h in b_broken:
            direction = "跌破" if h['is_bull'] else "突破"
            flip      = "⚠️ 考慮翻空" if h['is_bull'] else "⚠️ 考慮翻多"
            msg += (f"  {_dir(h)} <b>{h['symbol']}</b>  {direction}0.786  "
                    f"距{h['dist_pct']:.1f}%  RSI {h['rsi']:.0f}  {flip}  {_macd(h)}\n")
    else:
        msg += "  — 暫無\n"

    msg += "\n<i>⚠️ 僅供技術參考，請結合 K 線形態確認</i>"
    return msg


def _send_long_msg(text_url, chat_id, msg):
    """超過 3800 字元時按段落分頁發送。"""
    _TG_LIMIT = 3800
    if len(msg) <= _TG_LIMIT:
        requests.post(text_url, json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"})
        return
    sections = msg.split('\n\n')
    page, pages = '', []
    for sec in sections:
        if len(page) + len(sec) + 2 > _TG_LIMIT:
            pages.append(page)
            page = sec
        else:
            page += ('\n\n' if page else '') + sec
    if page:
        pages.append(page)
    for pg in pages:
        requests.post(text_url, json={"chat_id": chat_id, "text": pg, "parse_mode": "HTML"})


def send_fib_scan_report(target_chat_id):
    """
    掃描全市場 1H / 4H / 1D 三個時框的 Fib 回撤位，
    各發一則獨立訊息（共 3 則）。
    """
    la_tz    = pytz.timezone('America/Los_Angeles')
    now_str  = datetime.datetime.now(la_tz).strftime('%Y-%m-%d %H:%M')
    text_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    assets, _ = get_all_okx_swap_assets()

    for tf_label, tf_bar in [("1H 短線", "1H"), ("4H 波段", "4H"), ("1D 中線", "1D")]:
        try:
            msg = _build_fib_msg(tf_label, tf_bar, assets, now_str)
            _send_long_msg(text_url, target_chat_id, msg)
            print(f"📐 Fib {tf_label} 報告發送完成")
        except Exception as _fe:
            print(f"⚠️ Fib {tf_label} 報告失敗：{_fe}")


def send_fibcheck_report(raw_text, chat_id):
    """
    /fibcheck BTC [1H|4H|1D]
    顯示機器人實際抓取的 Fib 高低點、四個回撤位、現價距離，供使用者核對。
    """
    text_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    def _reply(msg):
        requests.post(text_url, json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"})

    parts = raw_text.strip().split()
    if len(parts) < 2:
        _reply("❌ 用法：<code>/fibcheck BTC</code> 或 <code>/fibcheck BTC 4H</code>")
        return

    coin   = parts[1].upper().replace("USDT", "").replace("-", "").replace("SWAP", "")
    bar    = parts[2].upper() if len(parts) >= 3 else "4H"
    valid_bars = {"1H", "4H", "1D", "15M", "30M"}
    if bar not in valid_bars:
        _reply(f"❌ 時框無效，請使用：{', '.join(sorted(valid_bars))}")
        return

    inst_id = f"{coin}-USDT-SWAP"

    try:
        url = f"{BASE_URL}/api/v5/market/candles?instId={inst_id}&bar={bar}&limit=300"
        r   = requests.get(url, timeout=10).json()
        if r.get('code') != '0':
            _reply(f"❌ OKX API 錯誤：{r.get('msg','未知')}（合約 {inst_id} 可能不存在）")
            return
        data = r.get('data', [])
        if len(data) < 40:
            _reply(f"❌ K 線資料不足（{len(data)} 根），無法計算")
            return

        df = pd.DataFrame(data,
                          columns=['ts','open','high','low','close','vol','volCcy','volCcyQuote','state'])
        for col in ['open','high','low','close']:
            df[col] = df[col].astype(float)
        df['ts'] = pd.to_datetime(df['ts'].astype(int), unit='ms', utc=True)
        df = df.iloc[::-1].reset_index(drop=True)   # 舊→新

        price = float(df['close'].iloc[-1])

        # 趨勢
        df['MA8']   = df['close'].rolling(8).mean()
        df['EMA89'] = df['close'].ewm(span=89, adjust=False).mean()
        is_bull = float(df['MA8'].iloc[-1]) > float(df['EMA89'].iloc[-1])

        # RSI
        delta = df['close'].diff()
        gain  = delta.where(delta > 0, 0).rolling(14).mean()
        loss  = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rsi   = float((100 - (100 / (1 + gain / (loss + 1e-10)))).iloc[-1])

        # MACD
        ema12 = df['close'].ewm(span=12, adjust=False).mean()
        ema26 = df['close'].ewm(span=26, adjust=False).mean()
        hist  = (ema12 - ema26) - (ema12 - ema26).ewm(span=9, adjust=False).mean()
        macd_bull = float(hist.iloc[-1]) > 0

        # ATR
        df['TR'] = df[['high','low','close']].assign(
            hl=df['high']-df['low'],
            hc=(df['high']-df['close'].shift(1)).abs(),
            lc=(df['low'] -df['close'].shift(1)).abs()
        )[['hl','hc','lc']].max(axis=1)
        atr = float(df['TR'].rolling(14).mean().iloc[-1])

        # ── 分形結構高低點 ──
        swings = _fib_structural_swings(df, is_bull, atr)
        if swings is None:
            _reply(f"❌ {coin} {bar}：找不到 Zigzag 結構擺動（波動太小或趨勢太新），請換時框再試")
            return
        sl_idx, swing_low, sh_idx, swing_high = swings

        sl_ts = df.loc[sl_idx, 'ts']
        sh_ts = df.loc[sh_idx, 'ts']
        total_rows = len(df)
        sh_bars_ago = total_rows - 1 - sh_idx
        sl_bars_ago = total_rows - 1 - sl_idx

        la_tz   = pytz.timezone('America/Los_Angeles')
        sh_time = pd.Timestamp(sh_ts).tz_convert(la_tz).strftime('%m/%d %H:%M')
        sl_time = pd.Timestamp(sl_ts).tz_convert(la_tz).strftime('%m/%d %H:%M')

        sw_range = swing_high - swing_low

        # Fib 位階
        if is_bull:
            fibs = {
                '0.236': swing_high - 0.236 * sw_range,
                '0.382': swing_high - 0.382 * sw_range,
                '0.500': swing_high - 0.500 * sw_range,
                '0.618': swing_high - 0.618 * sw_range,
                '0.786': swing_high - 0.786 * sw_range,
            }
            direction = "🟩 多頭（回踩支撐）"
        else:
            fibs = {
                '0.236': swing_low + 0.236 * sw_range,
                '0.382': swing_low + 0.382 * sw_range,
                '0.500': swing_low + 0.500 * sw_range,
                '0.618': swing_low + 0.618 * sw_range,
                '0.786': swing_low + 0.786 * sw_range,
            }
            direction = "🟥 空頭（反彈阻力）"

        # 按時間順序排列起訖點（讓方向一目瞭然）
        if is_bull:
            # 多頭：L→H，低點是起點（較早），高點是終點（較近）
            start_label = "📍 趨勢起點（Swing Low）"
            start_price, start_time, start_bars = swing_low, sl_time, sl_bars_ago
            end_label   = "🏁 趨勢頂點（Swing High）"
            end_price,   end_time,   end_bars   = swing_high, sh_time, sh_bars_ago
            fib_title   = "Fib 回撤位（從頂點往下量，尋找回踩支撐）"
        else:
            # 空頭：H→L，高點是起點（較早），低點是終點（較近）
            start_label = "📍 趨勢起點（Swing High）"
            start_price, start_time, start_bars = swing_high, sh_time, sh_bars_ago
            end_label   = "🏁 趨勢底點（Swing Low）"
            end_price,   end_time,   end_bars   = swing_low, sl_time, sl_bars_ago
            fib_title   = "Fib 回撤位（從底點往上量，尋找反彈阻力）"

        # 組裝訊息
        msg  = f"📐 <b>Fib 高低點診斷 — {coin} {bar}</b>\n"
        msg += f"趨勢：{direction}\n"
        msg += f"MA8={format_price(float(df['MA8'].iloc[-1]))}  EMA89={format_price(float(df['EMA89'].iloc[-1]))}\n"
        msg += f"RSI {rsi:.1f}  MACD {'↑多' if macd_bull else '↓空'}  ATR {format_price(atr)}\n"
        msg += "─────────────────────\n"
        msg += f"{start_label}：<b>{format_price(start_price)}</b>\n"
        msg += f"   時間：{start_time} PT（{start_bars} 根前）\n"
        msg += f"   　↓\n"
        msg += f"{end_label}：<b>{format_price(end_price)}</b>\n"
        msg += f"   時間：{end_time} PT（{end_bars} 根前）\n"
        msg += f"📏 振幅：{format_price(sw_range)}  （ATR 的 {sw_range/atr:.1f}x）\n"
        msg += "─────────────────────\n"
        msg += f"<b>{fib_title}</b>\n"
        for ratio, fval in fibs.items():
            dist = abs(price - fval) / fval * 100
            arrow = " ◀ 現價在此" if dist <= 2.5 else ""
            msg += f"  {ratio}：<b>{format_price(fval)}</b>  距 {dist:.1f}%{arrow}\n"
        msg += "─────────────────────\n"
        msg += f"現價：<b>{format_price(price)}</b>\n"
        msg += f"<i>（掃描時 ±2% 內才會出現在 Fib 報告，±2.5% 限 0.786）</i>"

        _reply(msg)

    except Exception as e:
        _reply(f"❌ 計算失敗：{e}")
        import traceback; print(traceback.format_exc())


# ==================== 📡 7. 原生無衝突監聽引擎 ====================
def scan_worker_thread(msg_title, target_chat_id, silent_on_empty=False, include_news=False, auto_track=False, include_fib=False):
    try:
     _scan_worker_thread_impl(msg_title, target_chat_id, silent_on_empty, include_news, auto_track, include_fib)
    except Exception as _swe:
        print(f"❌ scan_worker_thread 未捕獲例外：{_swe}")
        import traceback; traceback.print_exc()

def _scan_worker_thread_impl(msg_title, target_chat_id, silent_on_empty=False, include_news=False, auto_track=False, include_fib=False):
    valid_signals = run_strategy_scan()
    if valid_signals:
        send_html_report_via_requests(valid_signals, mode_title=msg_title,
                                      target_chat_id=target_chat_id, include_news=include_news,
                                      auto_track=auto_track)
        sent_at = time.time()
        with last_scan_lock:
            last_scan_cache.clear()
            for sig in valid_signals:
                last_scan_cache[sig['asset']] = {**sig, 'cached_at': sent_at}
        with recently_sent_lock:
            for sig in valid_signals:
                recently_sent_signals[(sig['asset'], sig['dir'])] = sent_at

        if auto_track:
            # 定時掃描：自動將訊號加入持倉監控，無需手動按追蹤
            auto_added = []
            with active_positions_lock:
                for sig in valid_signals:
                    already = any(p['asset'] == sig['asset'] and p['dir'] == sig['dir']
                                  for p in active_positions)
                    if not already:
                        _is_mkt = sig.get('is_market_entry', False)
                        new_pos = {
                            'asset':          sig['asset'],
                            'dir':            sig['dir'],
                            'tf':             sig['tf'],
                            'entry':          sig['entry'],
                            'sl':             sig['sl'],
                            'orig_sl':        sig['sl'],
                            'tp1':            sig['tp1'],
                            'tp2':            sig['tp2'],
                            'tp3':            sig['tp3'],
                            'tp_count':       sig.get('tp_count', 3),
                            'atr_trail':      sig.get('atr_trail', 0),   # TP3 移動止盈距離 = ATR×2.5
                            'signal_price':   sig.get('price', sig['entry']),
                            'signal_type':    sig.get('signal_type', 'trend'),
                            'adx':            sig.get('adx', 0),
                            'score':          sig.get('score', 0),
                            'win_rate':       sig.get('win_rate', 0),
                            'reported_at':    sent_at,
                            'last_checked_ts': sent_at,
                            'filled':         _is_mkt,
                            'fill_ts':        sent_at if _is_mkt else 0,
                            'entry_fr':       sig.get('entry_fr'),
                        }
                        active_positions.append(new_pos)
                        auto_added.append(f"{sig['asset']}{sig['dir']}")
                if auto_added:
                    save_positions(active_positions)
            if auto_added:
                print(f"📌 定時掃描自動監控：{', '.join(auto_added)}")
        else:
            print(f"📦 訊號已快取 {len(valid_signals)} 筆，等待 /open 指令確認")
    else:
        print(f"📭 掃描無訊號（{msg_title}）")

    # ── 手動 /scan 才附送 Fib 回撤指數報告（定時自動速報不發）──
    if include_fib:
        try:
            print("📐 開始 Fib 回撤指數掃描（1H/4H/1D）...")
            send_fib_scan_report(target_chat_id)
            print("📐 Fib 報告全部發送完成")
        except Exception as _fe:
            print(f"⚠️ Fib 報告發送失敗：{_fe}")

def _holding_quick_status(pos, cp):
    """
    從 pos dict 直接判斷狀態，完全不呼叫任何 API。
    背景監控每 5 分鐘會更新 pos 內的 sl/tp_hit 等欄位，summary 直接讀即可。
    回傳 (status_label, one_line_action)
    """
    if not pos.get('filled', False):
        return "⏳ 等待進場", f"掛單進場價 {format_price(pos['entry'])}"

    dir   = pos['dir']
    entry = pos['entry']
    sl    = pos['sl']
    tp1   = pos.get('tp1', 0)
    tp2   = pos.get('tp2', 0)
    tp3   = pos.get('tp3', 0)

    # 現價快速損益估算
    if cp:
        pnl_pct = (cp - entry) / entry * 100 * (1 if dir == "多" else -1)
        pnl_str = f"{'📈' if pnl_pct >= 0 else '📉'} {pnl_pct:+.2f}%"
    else:
        pnl_str = "現價取得失敗"

    if pos.get('tp3_hit'):
        return "🟣 TP3已達標", f"全部止盈完成  {pnl_str}"
    if pos.get('tp2_hit'):
        trail_sl = format_price(sl)
        return "🔵 TP2已完成", f"移動止損 {trail_sl}  {pnl_str}  等TP3 {format_price(tp3)}"
    if pos.get('tp1_hit'):
        return "🟡 TP1已完成", f"止損鎖至 {format_price(sl)}  {pnl_str}  等TP2 {format_price(tp2)}"

    # 未到任何 TP → 顯示與 SL/TP1 的距離
    if cp:
        sl_dist  = abs(cp - sl)  / cp * 100
        tp1_dist = abs(tp1 - cp) / cp * 100
        return "🟢 持倉中", f"距SL {sl_dist:.1f}%  距TP1 {tp1_dist:.1f}%  {pnl_str}"
    return "🟢 持倉中", "—"


def send_holding_summary(chat_id):
    """發送目前所有活躍持倉的狀態總覽（輕量快照版，不呼叫 K 線 API）"""
    with active_positions_lock:
        positions = list(active_positions)

    text_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    if not positions:
        requests.post(text_url, json={"chat_id": chat_id,
            "text": "📭 目前沒有追蹤中的持倉。請先執行 /scan 產生訊號。"})
        return

    la_tz   = pytz.timezone('America/Los_Angeles')
    now_str = datetime.datetime.now(la_tz).strftime('%Y-%m-%d %H:%M')

    # ── 僅取現價（每個資產 1 次 ticker 呼叫，全部並行）──
    unique_assets = list({pos['asset'] for pos in positions})
    price_cache: dict = {}
    def _fp(asset):
        return asset, get_current_price(asset + '-USDT-SWAP')
    try:
        with ThreadPoolExecutor(max_workers=min(len(unique_assets), 20)) as _pex:
            for asset, cp in _pex.map(_fp, unique_assets, timeout=12):
                price_cache[asset] = cp
    except Exception as _pe:
        print(f"⚠️ holding 取現價失敗：{_pe}")

    # ── 分兩組：已成交持倉 / 等待進場掛單 ──
    filled_pos  = [p for p in positions if p.get('filled', False)]
    pending_pos = [p for p in positions if not p.get('filled', False)]

    def _build_asset_blocks(pos_list):
        """把持倉列表按資產分組，組出 block 字串列表"""
        by_a: dict = defaultdict(list)
        for pos in pos_list:
            by_a[pos['asset']].append(pos)
        blocks = []
        for asset, group in by_a.items():
            cp        = price_cache.get(asset)
            price_str = format_price(cp) if cp else "—"
            blk = f"<b>{asset}</b>  現價 <code>{price_str}</code>\n"
            for i, pos in enumerate(group, 1):
                status, action = _holding_quick_status(pos, cp)
                age_h = (time.time() - pos['reported_at']) / 3600
                d     = "🟩<b>多</b>" if pos['dir'] == "多" else "🟥<b>空</b>"
                sub   = f"#{i} " if len(group) > 1 else ""
                t1 = "✅" if pos.get('tp1_hit') else "  "
                t2 = "✅" if pos.get('tp2_hit') else "  "
                t3 = "✅" if pos.get('tp3_hit') else "  "
                blk += f"{sub}{d}  {pos.get('tf','?')}  {status}  <i>({age_h:.1f}h前)</i>\n"
                blk += "<pre>"
                blk += f"進場  {format_price(pos['entry'])}\n"
                blk += f"止損  {format_price(pos['sl'])}\n"
                blk += f"TP1   {format_price(pos.get('tp1',0))} {t1}\n"
                blk += f"TP2   {format_price(pos.get('tp2',0))} {t2}\n"
                blk += f"TP3   {format_price(pos.get('tp3',0))} {t3}\n"
                blk += "</pre>"
                blk += f"▸ {action}\n"
            blk += "─────────────────────────\n"
            blocks.append(blk)
        return blocks

    # ── 組裝兩大段落 ──
    _TG_LIMIT = 3800
    holding_markup = {"inline_keyboard": [[
        {"text": "🔄 重新掃描", "callback_data": "cmd_scan"},
        {"text": "📊 查看勝率", "callback_data": "cmd_stats"}
    ]]}
    tracked = list({p['asset'] for p in positions})
    _eg = tracked[0] if tracked else "ETH"
    footer = (f"<i>📌 /open {_eg}  確認開倉\n"
              f"🗑️ /close {_eg}  解除監控\n"
              f"⚡ 警報僅在 TP/SL/市場惡化時推送</i>")

    def _send_section(section_header, asset_blocks, is_last_section):
        """把一組 asset_blocks 依字元上限分頁發送"""
        if not asset_blocks:
            return
        pages = []
        cur   = section_header
        for blk in asset_blocks:
            if len(cur) + len(blk) > _TG_LIMIT:
                pages.append(cur)
                cur = blk
            else:
                cur += blk
        if cur.strip():
            if is_last_section:
                cur += footer
            pages.append(cur)
        for idx, page in enumerate(pages):
            is_last_page = is_last_section and idx == len(pages) - 1
            payload = {"chat_id": chat_id, "text": page, "parse_mode": "HTML"}
            if is_last_page:
                payload["reply_markup"] = holding_markup
            resp = requests.post(text_url, json=payload)
            if not resp.json().get("ok"):
                print(f"⚠️ holding 訊息發送失敗：{resp.text[:200]}")

    # 第一頁：持倉中（已成交）
    sec1_header = (f"<b>【持倉中】</b>  <code>{now_str} PT</code>\n"
                   f"已成交 <b>{len(filled_pos)}</b> 筆  /  等待進場 <b>{len(pending_pos)}</b> 筆\n"
                   f"─────────────────────────\n")
    filled_blocks  = _build_asset_blocks(filled_pos)
    pending_blocks = _build_asset_blocks(pending_pos)

    has_pending = bool(pending_blocks)
    _send_section(sec1_header, filled_blocks, is_last_section=not has_pending)

    # 第二頁：等待進場（掛單中）
    if has_pending:
        sec2_header = (f"<b>【等待進場】</b>  掛單監控中\n"
                       f"─────────────────────────\n")
        _send_section(sec2_header, pending_blocks, is_last_section=True)

# ==================== 📐 Fib 0.618 全市場掃描 ====================
def _fib618_check_asset(inst_id):
    """
    取單一合約的4H K線，計算 Fib 0.618 並判斷現價是否在附近（±2%）。
    回傳 dict 或 None。
    """
    try:
        url = f"{BASE_URL}/api/v5/market/candles?instId={inst_id}&bar=4H&limit=100"
        r   = requests.get(url, timeout=5).json()
        if r.get('code') != '0' or len(r.get('data', [])) < 40:
            return None
        df = pd.DataFrame(r['data'],
                          columns=['ts','open','high','low','close','vol','volCcy','volCcyQuote','state'])
        for col in ['open','high','low','close']:
            df[col] = df[col].astype(float)
        df = df.iloc[::-1].reset_index(drop=True)

        price = float(df['close'].iloc[-1])

        # ── EMA + ADX（判斷趨勢方向）──
        df['MA8']   = df['close'].rolling(8).mean()
        df['EMA89'] = df['close'].ewm(span=89, adjust=False).mean()
        is_bull = float(df['MA8'].iloc[-1]) > float(df['EMA89'].iloc[-1])

        # ── ATR ──
        df['TR'] = (df[['high','low','close']].assign(
            hl=df['high']-df['low'],
            hc=(df['high']-df['close'].shift(1)).abs(),
            lc=(df['low'] -df['close'].shift(1)).abs()
        )[['hl','hc','lc']].max(axis=1))
        atr = float(df['TR'].rolling(14).mean().iloc[-1])

        # ── 尋找最近一波擺動：漲勢找「最低→最高」，跌勢找「最高→最低」──
        window = min(80, len(df))
        sub    = df.iloc[-window:]

        if is_bull:
            # 看多：找近期擺動低點（先低後高），計算回踩 Fib
            swing_low_idx  = int(sub['low'].idxmin())
            swing_high_idx = int(sub.loc[swing_low_idx:, 'high'].idxmax())
            swing_low   = float(sub.loc[swing_low_idx, 'low'])
            swing_high  = float(sub.loc[swing_high_idx, 'high'])
        else:
            # 看空：找近期擺動高點（先高後低），計算反彈 Fib
            swing_high_idx = int(sub['high'].idxmax())
            swing_low_idx  = int(sub.loc[swing_high_idx:, 'low'].idxmin())
            swing_high  = float(sub.loc[swing_high_idx, 'high'])
            swing_low   = float(sub.loc[swing_low_idx, 'low'])

        sw_range = swing_high - swing_low
        if sw_range < atr * 0.5:          # 擺動幅度太小，跳過
            return None

        fib618 = swing_high - 0.618 * sw_range
        fib500 = swing_high - 0.500 * sw_range
        fib382 = swing_high - 0.382 * sw_range

        # ── 現價是否在 Fib 0.618 ± 2% 以內 ──
        dist_pct = abs(price - fib618) / fib618 * 100
        if dist_pct > 2.0:
            return None

        # ── MACD 方向確認 ──
        ema12  = df['close'].ewm(span=12, adjust=False).mean()
        ema26  = df['close'].ewm(span=26, adjust=False).mean()
        hist   = (ema12 - ema26) - (ema12 - ema26).ewm(span=9, adjust=False).mean()
        macd_ok = (float(hist.iloc[-1]) > 0) if is_bull else (float(hist.iloc[-1]) < 0)

        return {
            'inst_id':    inst_id,
            'symbol':     inst_id.replace('-USDT-SWAP',''),
            'price':      price,
            'fib618':     fib618,
            'fib500':     fib500,
            'fib382':     fib382,
            'swing_high': swing_high,
            'swing_low':  swing_low,
            'dist_pct':   dist_pct,
            'is_bull':    is_bull,
            'atr':        atr,
            'macd_ok':    macd_ok,
            'sl':         fib618 - atr * 1.2 if is_bull else fib618 + atr * 1.2,
            'tp1':        fib382,
            'tp2':        swing_high if is_bull else swing_low,
        }
    except Exception:
        return None


def scan_fib618_setups(chat_id):
    """全市場掃描 Fib 0.618 回踩機會，發送結果到 Telegram。"""
    text_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    requests.post(text_url, json={
        "chat_id": chat_id,
        "text": "📐 正在掃描全市場 Fib 0.618 回踩點，約需 15~30 秒..."
    })

    assets, _ = get_all_okx_swap_assets()
    results   = []

    with ThreadPoolExecutor(max_workers=60) as ex:
        futs = {ex.submit(_fib618_check_asset, a): a for a in assets}
        for fut in as_completed(futs):
            try:
                hit = fut.result()
                if hit:
                    results.append(hit)
            except Exception:
                pass

    la_tz   = pytz.timezone('America/Los_Angeles')
    now_str = datetime.datetime.now(la_tz).strftime('%Y-%m-%d %H:%M')

    if not results:
        requests.post(text_url, json={
            "chat_id": chat_id,
            "text": f"📐 <b>Fib 0.618 掃描結果</b>  <code>{now_str} PT</code>\n\n目前無合約現價落在 Fib 0.618 ±2% 區間內。\n可稍後再試，或調大容差後重新掃描。",
            "parse_mode": "HTML"
        })
        return

    # 排序：MACD 確認優先，再按距 0.618 距離由近到遠
    results.sort(key=lambda x: (not x['macd_ok'], x['dist_pct']))

    _TG_LIMIT = 3800
    header = (f"📐 <b>Fib 0.618 回踩掃描</b>  <code>{now_str} PT</code>\n"
              f"共 <b>{len(results)}</b> 個合約現價在 Fib 0.618 ±2% 以內\n"
              f"（✅ = MACD 方向吻合，排序優先）\n"
              f"─────────────────────────\n")

    pages   = []
    cur     = header
    for hit in results:
        d    = "🟩多" if hit['is_bull'] else "🟥空"
        macd = "✅MACD" if hit['macd_ok'] else "⚠️MACD反"
        blk  = (f"{d}  <b>{hit['symbol']}</b>  現價 <code>{format_price(hit['price'])}</code>"
                f"  {macd}  距0.618 <b>{hit['dist_pct']:.1f}%</b>\n"
                f"<pre>"
                f"Fib 0.618  {format_price(hit['fib618'])}  ← 進場區\n"
                f"Fib 0.500  {format_price(hit['fib500'])}\n"
                f"Fib 0.382  {format_price(hit['fib382'])}  ← TP1\n"
                f"止損       {format_price(hit['sl'])}\n"
                f"波段高     {format_price(hit['swing_high'])}  波段低  {format_price(hit['swing_low'])}\n"
                f"</pre>")
        if len(cur) + len(blk) > _TG_LIMIT:
            pages.append(cur)
            cur = blk
        else:
            cur += blk

    footer = f"\n<i>⚠️ 僅供技術參考，請結合 K 線形態確認後再進場</i>"
    if cur.strip():
        cur += footer
        pages.append(cur)

    for page in pages:
        requests.post(text_url, json={"chat_id": chat_id, "text": page, "parse_mode": "HTML"})


# ==================== 📌 手動開倉/平倉指令 ====================
def handle_open_command(text, chat_id):
    """
    /open ETH        → 即時重算指標，以 MA8/EMA89 為進場錨點
    /open ETH 多     → 指定方向（若與指標方向相反會警告）
    每次都即時重算，保證點位穩定且基於當前市況。
    """
    send_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    parts = text.strip().split()
    if len(parts) < 2:
        requests.post(send_url, json={"chat_id": chat_id,
            "text": "❓ 用法：/open ETH 或 /open ETH 多"})
        return

    symbol    = parts[1].upper()
    direction = parts[2] if len(parts) >= 3 and parts[2] in ("多", "空") else None
    inst_id   = f"{symbol}-USDT-SWAP"
    now_ts    = time.time()

    # ── 24h 成交額硬性門檻檢查（與掃描過濾一致：≥50M USDT）──
    _MIN_VOL_USDT_OPEN = 50_000_000
    _coin_vol = _vol_cache.get(inst_id, None)
    if _coin_vol is None:
        # 快取未有此幣 → 即時查詢
        try:
            _tv = requests.get(
                f"{BASE_URL}/api/v5/market/ticker?instId={inst_id}", timeout=4
            ).json()
            if _tv.get('code') == '0' and _tv.get('data'):
                _coin_vol = float(_tv['data'][0].get('volCcy24h', 0))
                _vol_cache[inst_id] = _coin_vol
        except Exception:
            _coin_vol = None
    if _coin_vol is not None and _coin_vol < _MIN_VOL_USDT_OPEN:
        requests.post(send_url, json={"chat_id": chat_id, "parse_mode": "HTML",
            "text": (
                f"⛔ <b>{symbol}</b> 24h 成交額 <b>{_coin_vol/1_000_000:.1f}M USDT</b>，"
                f"未達流動性門檻（50M USDT）。\n\n"
                f"低流動性合約插針頻繁、實際難以成交，不建議操作。"
            )})
        return

    # ── 即時重算兩個時框，挑條件最多的 ──
    requests.post(send_url, json={"chat_id": chat_id,
        "text": f"🔍 正在分析 {symbol} 當前指標，請稍候..."})

    best = None
    for tf in ('15m', '30m', '1h', '4h', '1d'):
        st = fetch_coin_status(inst_id, tf)
        if st and (best is None or st['passed'] > best['passed']):
            best = st

    if not best:
        requests.post(send_url, json={"chat_id": chat_id,
            "text": f"❌ 無法取得 {symbol} 的市場數據，請確認幣種名稱"})
        return

    # ── 若未指定方向，採用指標判斷的方向 ──
    if direction is None:
        direction = best['dir']

    # ── 方向與指標相反 → 拒絕，避免逆勢開倉 ──
    if best['dir'] != direction:
        f = best['filters']
        filter_lines = (
            f"   ADX={best['adx']} {'✅' if f['adx_ok'] else '❌'}\n"
            f"   成交量={best['vol_pct']}%均量 {'✅' if f['vol_ok'] else '❌'}\n"
            f"   EMA89斜率 {'✅' if f['slope_ok'] else '❌'}\n"
            f"   RSI={best['rsi']} {'✅' if f['no_div'] else '❌ 背離'}"
        )
        requests.post(send_url, json={"chat_id": chat_id, "parse_mode": "HTML",
            "text": (
                f"⛔ <b>{symbol}</b> 當前技術方向為「<b>{best['dir']}</b>」，"
                f"你指定的「{direction}」與指標相反。\n\n"
                f"當前指標狀況：\n{filter_lines}\n\n"
                f"逆勢開倉風險極高，建議等待方向確認後再進場。\n"
                f"若確認方向改變，請重新執行 /scan 取得最新訊號。"
            )})
        return

    # ── 通過條件不足 → 拒絕 ──
    if best['passed'] < 2:
        f = best['filters']
        filter_lines = (
            f"   ADX={best['adx']} {'✅' if f['adx_ok'] else '❌'}\n"
            f"   成交量={best['vol_pct']}%均量 {'✅' if f['vol_ok'] else '❌'}\n"
            f"   EMA89斜率 {'✅' if f['slope_ok'] else '❌'}\n"
            f"   RSI={best['rsi']} {'✅' if f['no_div'] else '❌ 背離'}"
        )
        requests.post(send_url, json={"chat_id": chat_id, "parse_mode": "HTML",
            "text": (
                f"⚠️ <b>{symbol} {direction}</b> 當前僅通過 {best['passed']}/4 個條件，"
                f"訊號強度不足，不建議進場。\n\n"
                f"當前條件狀況：\n{filter_lines}\n\n"
                f"請等待訊號改善後再開倉（建議至少通過 2 個條件）。"
            )})
        return

    # ── 條件足夠 → 以 MA8/EMA89 為進場錨點，不用當下市價 ──
    entry = best['entry']   # 1H→MA8，4H→EMA89，穩定不跳動
    sl    = best['sl']
    tp1   = best['tp1']
    tp2   = best['tp2']
    tp3   = best['tp3']
    tf_label = best['tf']

    f = best['filters']
    bar = "■" * best['passed'] + "□" * (4 - best['passed'])
    cond_note = ""
    if best['passed'] < 4:
        missing = []
        if not f['adx_ok']:    missing.append(f"ADX={best['adx']:.0f}<20")
        if not f['vol_ok']:    missing.append(f"量能{best['vol_pct']}%均量")
        if not f['slope_ok']:  missing.append("EMA89斜率")
        if not f['no_div']:    missing.append("RSI背離")
        cond_note = f"\n<i>⚠️ 未滿足：{'、'.join(missing)}（{best['passed']}/4）</i>"

    dir_tag = "🟩多" if direction == "多" else "🟥空"
    anchor_label = "MA8" if "1H" in tf_label else "EMA89"
    confirm_text = (
        f"⏳ <b>{symbol} {dir_tag} 掛單監控中</b>  [{bar}]\n"
        f"進場錨點：{anchor_label}（{tf_label}），穩定不跳動\n"
        f"<pre>"
        f"進場  {format_price(entry)}\n"
        f"止損  {format_price(sl)}\n"
        f"TP1   {format_price(tp1)}\n"
        f"TP2   {format_price(tp2)}\n"
        f"TP3   {format_price(tp3)}\n"
        f"</pre>"
        f"現價 <code>{format_price(best['price'])}</code>  "
        f"距進場點 {abs(best['price'] - entry) / entry * 100:.2f}%"
        f"{cond_note}\n"
        f"📌 價格觸及進場點時自動推送確認通知\n"
        f"取消請輸入 /close {symbol}"
    )

    _is_mkt_open = best.get('is_market_entry', False)
    new_pos = {
        'asset':           symbol,
        'dir':             direction,
        'tf':              tf_label,
        'entry':           entry,
        'sl':              sl,
        'orig_sl':         sl,
        'tp1':             tp1,
        'tp2':             tp2,
        'tp3':             tp3,
        'tp_count':        best.get('tp_count', 3),
        'signal_price':    best.get('price', entry),
        'signal_type':     best.get('signal_type', 'trend'),
        'atr_trail':       best.get('atr_trail', 0),
        'adx':             best.get('adx', 0),
        'score':           best.get('score', 0),
        'win_rate':        best.get('win_rate', 0),
        'entry_fr':        best.get('entry_fr'),
        'reported_at':     now_ts,
        'last_checked_ts': now_ts,
        'filled':          _is_mkt_open,
        'fill_ts':         now_ts if _is_mkt_open else 0,
    }

    with active_positions_lock:
        active_positions[:] = [
            p for p in active_positions
            if not (p['asset'] == new_pos['asset'] and p['dir'] == new_pos['dir'])
        ]
        active_positions.append(new_pos)
        save_positions(active_positions)

    print(f"📌 /open {symbol} {direction} 已加入監控（{tf_label} {anchor_label}={format_price(entry)}），共 {len(active_positions)} 筆")
    requests.post(send_url, json={"chat_id": chat_id, "text": confirm_text, "parse_mode": "HTML"})


def handle_close_command(text, chat_id):
    """
    /close ETH     → 移除 ETH 所有持倉（計算損益並記錄統計）
    /close ETH 多  → 只移除多倉
    /close all     → 清空所有持倉
    """
    parts = text.strip().split()
    if len(parts) < 2:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": chat_id,
                  "text": "❓ 用法：/close ETH  或  /close ETH 多  或  /close all"}
        )
        return

    target    = parts[1].upper()
    direction = parts[2] if len(parts) >= 3 else None

    # 找出要關閉的倉位
    with active_positions_lock:
        if target == "ALL":
            closing = list(active_positions)
        elif direction:
            closing = [p for p in active_positions
                       if p['asset'] == target and p['dir'] == direction]
        else:
            closing = [p for p in active_positions if p['asset'] == target]

    if not closing:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": chat_id,
                  "text": f"❓ 找不到 {target} 的持倉記錄（可用 /holding 查看目前持倉）"}
        )
        return

    la_tz   = pytz.timezone('America/Los_Angeles')
    now_str = datetime.datetime.now(la_tz).strftime('%H:%M PT')
    lines   = [f"<b>【手動平倉確認】</b>  <code>{now_str}</code>\n"]

    for pos in closing:
        asset    = pos['asset']
        dir_tag  = "🟩多" if pos['dir'] == "多" else "🟥空"
        entry    = pos.get('entry', 0)
        inst_id  = asset + "-USDT-SWAP"

        # 取當前價格計算損益
        try:
            px_res = requests.get(
                f"{BASE_URL}/api/v5/market/ticker?instId={inst_id}", timeout=4
            ).json()
            current_price = float(px_res['data'][0]['last'])
        except Exception:
            current_price = entry  # 無法取價時視為平手

        if not pos.get('filled', False):
            # 掛單未成交 → 純撤單，不計入損益統計
            outcome_label = "撤單"
            outcome_icon  = "🚫"
            pnl_str       = "未成交，不計損益"
        else:
            if pos['dir'] == "多":
                pnl_pct = (current_price - entry) / entry * 100
            else:
                pnl_pct = (entry - current_price) / entry * 100

            if pos.get('tp1_hit') or pos.get('tp2_hit'):
                outcome       = "win"
                outcome_label = "手動平倉（TP後）"
                outcome_icon  = "🟣"
            else:
                outcome       = "loss"
                outcome_label = "策略改變關倉"
                outcome_icon  = "🔴"

            pnl_sign = "+" if pnl_pct >= 0 else ""
            pnl_str  = f"損益 {pnl_sign}{pnl_pct:.2f}%，現價 <code>{format_price(current_price)}</code>"
            record_trade_outcome(pos, outcome)

        lines.append(
            f"{outcome_icon} <b>{asset}</b>  {dir_tag}  {pos.get('tf','—')}\n"
            f"   進場 <code>{format_price(entry)}</code>  → {outcome_label}\n"
            f"   {pnl_str}\n"
        )

    # 移除倉位
    with active_positions_lock:
        for p in closing:
            if p in active_positions:
                active_positions.remove(p)
        save_positions(active_positions)

    # 平倉後清除推播冷卻，讓同幣種可以在下次掃描重新出現
    with recently_sent_lock:
        for p in closing:
            recently_sent_signals.pop((p['asset'], p['dir']), None)

    lines.append(f"<i>共移除 {len(closing)} 筆持倉監控</i>")
    msg = "\n".join(lines)
    print(f"🗑️ /close {target}: 移除 {len(closing)} 筆")
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"}
    )


def send_daily_journal(chat_id):
    """
    每日交易日誌（23:50 PT 觸發）。
    總結當天所有完整交易，列出勝敗明細、策略失效原因分析、優化建議。
    """
    _url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    _cid = str(chat_id)
    la_tz   = pytz.timezone('America/Los_Angeles')
    now_la  = datetime.datetime.now(la_tz)
    today   = now_la.date()

    # 今日 00:00 PT 的 Unix 時間戳
    day_start = la_tz.localize(datetime.datetime.combine(today, datetime.time.min)).timestamp()

    try:
        all_records = load_stats()
        today_records = [
            r for r in all_records
            if r.get('timestamp', 0) >= day_start
            and r.get('outcome') in ('win', 'loss')
        ]
    except Exception as e:
        requests.post(_url, json={"chat_id": _cid,
            "text": f"⚠️ 日誌讀取失敗：{e}"}, timeout=10)
        return

    date_str = today.strftime('%Y-%m-%d')
    header   = f"📔 <b>【每日交易日誌】{date_str} PT</b>\n"

    if not today_records:
        requests.post(_url, json={"chat_id": _cid,
            "text": header + "\n今日無完整交易記錄。\n持倉中或無訊號觸發，繼續監控中。",
            "parse_mode": "HTML"}, timeout=10)
        return

    wins   = [r for r in today_records if r['outcome'] == 'win']
    losses = [r for r in today_records if r['outcome'] == 'loss']
    total  = len(today_records)
    wr     = len(wins) / total * 100

    # ── 出場類型標籤 ──
    def _exit_tag(r):
        et = r.get('exit_type', '')
        return {"tp3": "TP3 🎯", "tp2": "TP2 ✅", "tp1": "TP1 ✅",
                "breakeven": "保本 🛡️", "sl": "SL ❌",
                "timeout": "逾時 ⏰", "manual": "手動 🖐️"}.get(et, et)

    # ── P&L 標籤 ──
    def _pnl_tag(r):
        v = r.get('pnl_pct')
        return f"{v:+.2f}%" if v is not None else "—"

    # ── 策略標籤 ──
    def _st_tag(r):
        return {"trend": "趨勢", "range": "區間", "divergence": "背離"}.get(
            r.get('signal_type', 'trend'), r.get('signal_type', ''))

    # ── 失效原因分析（單筆敗場）──
    def _failure_reason(r):
        st  = r.get('signal_type', 'trend')
        tf  = r.get('tf', '')
        adx = r.get('adx') or 0
        if st == 'trend':
            if '15M' in tf or '超短' in tf:
                return "超短線(15M)雜訊大，趨勢動能維持時間短，快速反轉"
            if adx < 30:
                return f"ADX偏低({adx:.0f})，趨勢方向性不足，進場後趨勢消失"
            if adx > 55:
                return "ADX過高，趨勢末段進場，動能耗盡後反轉"
            return "趨勢中途反轉，建議加強更高時框方向確認"
        if st == 'range':
            if adx > 35:
                return f"ADX過高({adx:.0f})，市場已由盤整轉趨勢，區間結構被突破"
            return "支撐/壓力位被有效突破，盤整結構失效"
        if st == 'divergence':
            if adx > 35:
                return f"強趨勢(ADX {adx:.0f})中的背離往往是假反轉，趨勢繼續延伸"
            if adx < 20:
                return f"ADX偏低({adx:.0f})，背離訊號可靠性低，方向性不明"
            return "背離反轉空間不足，趨勢未完全結束便再度延伸"
        return "策略條件在出場前發生變化"

    # ── 組合今日概要 ──
    tp_types   = [r.get('exit_type','') for r in wins]
    tp3_c = tp_types.count('tp3')
    tp2_c = tp_types.count('tp2')
    tp1_c = tp_types.count('tp1')
    be_c  = tp_types.count('breakeven')
    sl_c  = sum(1 for r in losses if r.get('exit_type') == 'sl')

    win_detail  = "  ".join(filter(None, [
        f"TP3×{tp3_c}" if tp3_c else "",
        f"TP2×{tp2_c}" if tp2_c else "",
        f"TP1×{tp1_c}" if tp1_c else "",
        f"保本×{be_c}" if be_c  else "",
    ])) or "—"
    loss_detail = f"SL×{sl_c}" if sl_c else "逾時/手動"

    msg  = header
    msg += "─────────────────────────\n"
    msg += f"📊 <b>今日概要</b>  共 {total} 筆完整交易\n"
    msg += f"🟢 勝  <b>{len(wins)}</b> 筆 ({wr:.0f}%)   {win_detail}\n"
    msg += f"🔴 敗  <b>{len(losses)}</b> 筆 ({100-wr:.0f}%)   {loss_detail}\n"

    # ── 勝場明細 ──
    if wins:
        msg += "\n🟢 <b>勝場明細</b>\n"
        for r in wins[:8]:   # 最多顯示 8 筆，避免訊息過長
            msg += (f"  ✅ <b>{r['asset']}</b> {r.get('dir','')} "
                    f"{r.get('tf','')} {_st_tag(r)}  "
                    f"{_exit_tag(r)}  {_pnl_tag(r)}  "
                    f"ADX{r.get('adx',0):.0f}  TA{r.get('win_rate',0)}\n")

    # ── 敗場明細 + 失效原因 ──
    if losses:
        msg += "\n🔴 <b>敗場明細</b>\n"
        for r in losses[:8]:
            msg += (f"  ❌ <b>{r['asset']}</b> {r.get('dir','')} "
                    f"{r.get('tf','')} {_st_tag(r)}  "
                    f"{_exit_tag(r)}  {_pnl_tag(r)}  "
                    f"ADX{r.get('adx',0):.0f}  TA{r.get('win_rate',0)}\n"
                    f"     ↳ {_failure_reason(r)}\n")

    # ── 策略分析（各策略勝敗拆解）──
    msg += "\n📌 <b>策略分析</b>\n"
    for stype, label in [('trend','趨勢'), ('range','區間'), ('divergence','背離')]:
        st_recs = [r for r in today_records if r.get('signal_type','trend') == stype]
        if not st_recs:
            continue
        st_w = sum(1 for r in st_recs if r['outcome'] == 'win')
        st_l = len(st_recs) - st_w
        st_wr = st_w / len(st_recs) * 100
        flag = "⚠️" if st_wr < 50 and len(st_recs) >= 2 else ("✅" if st_wr >= 70 else "")
        # 時框細分
        tf_notes = []
        for tf_key, tf_name in [('15M','15M'), ('30M','30M'), ('1H','1H'), ('4H','4H'), ('1D','1D')]:
            tf_recs = [r for r in st_recs if tf_key in r.get('tf','')]
            if len(tf_recs) >= 1:
                tw = sum(1 for r in tf_recs if r['outcome'] == 'win')
                tf_notes.append(f"{tf_name}:{tw}/{len(tf_recs)}")
        tf_str = "  ".join(tf_notes)
        msg += f"  {flag} {label}策略  {st_w}勝/{st_l}敗 ({st_wr:.0f}%)  {tf_str}\n"

    # ── 優化建議（規則引擎）──
    suggestions = []

    # 1. 15M 趨勢勝率分析
    trend_15m = [r for r in today_records
                 if r.get('signal_type','trend') == 'trend' and '15M' in r.get('tf','')]
    if len(trend_15m) >= 2:
        wr15 = sum(1 for r in trend_15m if r['outcome'] == 'win') / len(trend_15m) * 100
        if wr15 < 50:
            suggestions.append(f"15M趨勢今日勝率 {wr15:.0f}%（{len(trend_15m)}筆），建議提高15M的ADX/分數門檻")

    # 2. 背離策略分析
    div_recs = [r for r in today_records if r.get('signal_type','trend') == 'divergence']
    if len(div_recs) >= 2:
        div_wr = sum(1 for r in div_recs if r['outcome'] == 'win') / len(div_recs) * 100
        if div_wr < 40:
            avg_adx = sum(r.get('adx',0) for r in div_recs) / len(div_recs)
            suggestions.append(
                f"背離策略今日勝率 {div_wr:.0f}%（{len(div_recs)}筆），"
                f"均ADX={avg_adx:.0f}，強趨勢中背離失敗率高，建議ADX>35時跳過")

    # 3. 區間策略分析
    rng_recs = [r for r in today_records if r.get('signal_type','trend') == 'range']
    if len(rng_recs) >= 2:
        rng_wr = sum(1 for r in rng_recs if r['outcome'] == 'win') / len(rng_recs) * 100
        if rng_wr < 40:
            suggestions.append(
                f"區間策略今日勝率 {rng_wr:.0f}%（{len(rng_recs)}筆），今日波動性強非盤整市，"
                f"可考慮在BTC大幅波動時降低區間策略權重")

    # 4. 最佳策略+時框組合
    best_combo = None
    best_combo_wr = 0
    for stype in ('trend','range','divergence'):
        for tf_key in ('4H','1H','1D','30M','15M'):
            combo = [r for r in today_records
                     if r.get('signal_type','trend') == stype and tf_key in r.get('tf','')]
            if len(combo) >= 2:
                c_wr = sum(1 for r in combo if r['outcome'] == 'win') / len(combo) * 100
                if c_wr > best_combo_wr:
                    best_combo_wr = c_wr
                    best_combo = (tf_key, stype, len(combo), c_wr)
    if best_combo and best_combo_wr >= 75:
        label_map = {'trend':'趨勢','range':'區間','divergence':'背離'}
        suggestions.append(
            f"{best_combo[0]} {label_map[best_combo[1]]}今日表現最佳 "
            f"({best_combo[2]}筆勝率{best_combo[3]:.0f}%)，可優先追蹤此類訊號")

    # 5. 整體勝率
    if total >= 3 and wr < 50:
        suggestions.append("今日整體勝率偏低，建議檢視是否處於震盪市，考慮暫緩開倉等待更明確趨勢")

    if suggestions:
        msg += "\n💡 <b>優化建議</b>\n"
        for i, s in enumerate(suggestions[:4], 1):
            msg += f"  {i}. {s}\n"
    else:
        msg += "\n💡 今日策略運作正常，無特別優化建議。\n"

    msg += "\n<i>日誌基於當日已結案交易自動生成，P&L為預估值（依TP/SL目標推算）。</i>"

    if len(msg) > 4000:
        msg = msg[:3990] + "\n…（訊息過長已截斷）"

    try:
        requests.post(_url, json={"chat_id": _cid, "text": msg, "parse_mode": "HTML"}, timeout=15)
        print(f"📔 每日交易日誌已發送：{date_str}，共 {total} 筆，勝率 {wr:.0f}%")
    except Exception as e:
        print(f"❌ 每日交易日誌發送失敗：{e}")


def export_trade_data(chat_id):
    """
    將完整交易記錄以 JSON 檔案形式透過 Telegram 發送。
    供人工備份與日後策略優化分析用。
    """
    import io
    _url_doc = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    _url_msg = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    _cid   = str(chat_id)
    la_tz  = pytz.timezone('America/Los_Angeles')
    now_la = datetime.datetime.now(la_tz)
    try:
        records = load_stats()
        if not records:
            requests.post(_url_msg, json={"chat_id": _cid,
                "text": "📭 目前無交易記錄可匯出。"}, timeout=10)
            return
        wins   = sum(1 for r in records if r.get('outcome') == 'win')
        losses = sum(1 for r in records if r.get('outcome') == 'loss')
        wr_str = f"{wins/(wins+losses)*100:.0f}%" if (wins + losses) > 0 else "—"
        filename = f"trade_records_{now_la.strftime('%Y%m%d_%H%M')}.json"
        content  = json.dumps(records, ensure_ascii=False, indent=2).encode('utf-8')
        caption  = (
            f"📦 <b>交易記錄備份</b>\n"
            f"📅 {now_la.strftime('%Y-%m-%d %H:%M')} PT\n"
            f"📊 共 <b>{len(records)}</b> 筆  勝{wins} / 敗{losses}  整體勝率 {wr_str}\n"
            f"💡 含策略類型、ADX、出場類型、預估P&L等欄位\n"
            f"💾 JSON 格式，可直接用 Excel / Python 分析"
        )
        requests.post(
            _url_doc,
            data={"chat_id": _cid, "caption": caption, "parse_mode": "HTML"},
            files={"document": (filename, io.BytesIO(content), "application/json")},
            timeout=30
        )
        print(f"📦 交易記錄已匯出：{len(records)} 筆 → {filename}")
    except Exception as e:
        print(f"❌ 匯出交易記錄失敗：{e}")
        try:
            requests.post(_url_msg, json={"chat_id": _cid,
                "text": f"❌ 匯出失敗：{e}"}, timeout=10)
        except Exception:
            pass


def send_stats_report(chat_id):
    """勝率統計播報（近30天）"""
    _url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    _cid = str(chat_id)
    print(f"📊 send_stats_report 開始執行，chat_id={_cid}")

    # ── Step 1: 立刻送「收到」確認，確認 Telegram API 通 ──
    try:
        r0 = requests.post(_url, json={"chat_id": _cid, "text": "📊 查詢勝率中，請稍候…"}, timeout=10)
        print(f"📊 確認訊息送出，HTTP={r0.status_code}, ok={r0.json().get('ok')}")
    except Exception as ex:
        print(f"❌ send_stats_report 確認訊息失敗: {ex}")
        return   # 連這都送不出去，Telegram API 有問題，直接放棄

    # ── Step 2: 讀取統計資料 ──
    try:
        records = load_stats()
        cutoff  = time.time() - 30 * 86400
        recent  = [r for r in records if r.get('timestamp', 0) >= cutoff]
        valid   = [r for r in recent if r.get('outcome') in ('win', 'loss')]
        print(f"📊 統計資料：總={len(records)}, 近30天={len(recent)}, 有效={len(valid)}")
    except Exception as ex:
        print(f"❌ load_stats 失敗: {ex}")
        requests.post(_url, json={"chat_id": _cid, "text": f"⚠️ 讀取統計檔案失敗：{ex}"}, timeout=10)
        return

    # ── Step 3: 無資料情況 ──
    if not valid:
        requests.post(_url, json={
            "chat_id": _cid,
            "text": "📊 近30天尚無完整交易記錄\n繼續積累數據中，每次 TP/SL 觸發後自動更新。"
        }, timeout=10)
        print("📊 無有效記錄，已回覆提示")
        return

    # ── Step 4: 計算並組訊息 ──
    try:
        from collections import Counter
        wins   = [r for r in valid if r['outcome'] == 'win']
        losses = [r for r in valid if r['outcome'] == 'loss']
        total  = len(valid)
        win_rate = len(wins) / total * 100

        la_tz   = pytz.timezone('America/Los_Angeles')
        now_str = datetime.datetime.now(la_tz).strftime('%Y-%m-%d')

        msg  = f"<b>【勝率統計】</b>  <code>{now_str}</code>\n"
        msg += f"近30天共 <b>{total}</b> 筆完整交易\n"
        msg += "─────────────────────────\n"
        msg += f"🟢 勝（任何 TP 後出場）  <b>{len(wins)}</b> 筆  ({win_rate:.1f}%)\n"
        msg += f"🔴 敗（止損 / 策略改變）  <b>{len(losses)}</b> 筆  ({100-win_rate:.1f}%)\n"
        msg += "─────────────────────────\n"
        msg += f"勝率：<b>{win_rate:.1f}%</b>\n\n"

        # 資金費率分析
        fr_records = [r for r in valid if r.get('entry_fr') is not None]
        if len(fr_records) >= 3:
            msg += "\n<b>📊 資金費率 vs 結果：</b>\n"
            def _avg_fr(lst):
                vals = [r.get('entry_fr', 0) for r in lst if r.get('entry_fr') is not None]
                return sum(vals) / len(vals) if vals else 0.0
            def _fr_label(v):
                if v >  0.01: return f"+{v:.4f}%（偏多）"
                if v < -0.01: return f"{v:.4f}%（偏空）"
                return f"{v:+.4f}%（中性）"
            msg += f"  🟢 勝場均費率：{_fr_label(_avg_fr([r for r in fr_records if r['outcome']=='win']))}\n"
            msg += f"  🔴 敗場均費率：{_fr_label(_avg_fr([r for r in fr_records if r['outcome']=='loss']))}\n"

            high_fr = [r for r in fr_records if r.get('entry_fr', 0) >  0.03]
            low_fr  = [r for r in fr_records if r.get('entry_fr', 0) < -0.03]
            neut_fr = [r for r in fr_records if -0.03 <= r.get('entry_fr', 0) <= 0.03]
            def _zone_wr(lst):
                if not lst: return 0.0
                return sum(1 for r in lst if r['outcome'] == 'win') / len(lst) * 100
            if high_fr: msg += f"  費率>+0.03%（多頭過熱）：{len(high_fr)}筆  勝率{_zone_wr(high_fr):.0f}%\n"
            if neut_fr: msg += f"  費率中性（±0.03%）：{len(neut_fr)}筆  勝率{_zone_wr(neut_fr):.0f}%\n"
            if low_fr:  msg += f"  費率<-0.03%（空頭過熱）：{len(low_fr)}筆  勝率{_zone_wr(low_fr):.0f}%\n"

        msg += "\n<i>數據僅供參考，保持紀律為第一要務。</i>"
        if len(msg) > 4000:
            msg = msg[:3990] + "\n…（訊息過長已截斷）"

    except Exception as ex:
        print(f"❌ send_stats_report 組訊息失敗: {ex}")
        requests.post(_url, json={"chat_id": _cid, "text": f"⚠️ 勝率計算失敗：{ex}"}, timeout=10)
        return

    # ── Step 5: 送出正式統計訊息 ──
    try:
        r5 = requests.post(_url, json={"chat_id": _cid, "text": msg, "parse_mode": "HTML"}, timeout=10)
        print(f"📊 統計訊息送出，HTTP={r5.status_code}, ok={r5.json().get('ok')}, total={total}")
        if not r5.json().get('ok'):
            print(f"❌ Telegram 拒絕訊息: {r5.json().get('description')}")
            # 有可能是 HTML parse 問題，改純文字重送
            plain = msg.replace('<b>','').replace('</b>','').replace('<i>','').replace('</i>','').replace('<code>','').replace('</code>','')
            requests.post(_url, json={"chat_id": _cid, "text": plain}, timeout=10)
    except Exception as ex:
        print(f"❌ send_stats_report 最終發送失敗: {ex}")


def fetch_coin_status(inst_id, tf):
    """取得幣種當前完整指標狀態（不要求交叉）"""
    bar_param = _TF_BAR.get(tf, "1H")
    url = f"{BASE_URL}/api/v5/market/candles?instId={inst_id}&bar={bar_param}&limit=100"
    try:
        res = requests.get(url, timeout=2.0).json()
        if res.get('code') != '0' or len(res['data']) < 90:
            return None
        df = pd.DataFrame(res['data'], columns=['ts','open','high','low','close','vol','volCcy','volCcyQuote','state'])
        for col in ['open','high','low','close','vol']:
            df[col] = df[col].astype(float)
        df = df.iloc[::-1].reset_index(drop=True)
        df['MA8']   = df['close'].rolling(8).mean()
        df['EMA89'] = df['close'].ewm(span=89, adjust=False).mean()
        delta = df['close'].diff()
        gain  = delta.where(delta > 0, 0).rolling(14).mean()
        loss  = (-delta.where(delta < 0, 0)).rolling(14).mean()
        df['RSI']  = 100 - (100 / (1 + gain / (loss + 1e-10)))
        df['H-L']  = df['high'] - df['low']
        df['H-PC'] = (df['high'] - df['close'].shift(1)).abs()
        df['L-PC'] = (df['low']  - df['close'].shift(1)).abs()
        df['TR']   = df[['H-L','H-PC','L-PC']].max(axis=1)
        df['ATR14']= df['TR'].rolling(14).mean()
        plus_dm  = df['high'].diff().clip(lower=0)
        minus_dm = (-df['low'].diff()).clip(lower=0)
        mask = plus_dm >= minus_dm
        plus_dm_c = plus_dm.where(mask, 0)
        minus_dm_c= minus_dm.where(~mask, 0)
        s = df['ATR14'] + 1e-10
        plus_di  = 100 * plus_dm_c.rolling(14).mean() / s
        minus_di = 100 * minus_dm_c.rolling(14).mean() / s
        dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di + 1e-10)
        df['ADX'] = dx.rolling(14).mean()
        c = df.iloc[-1]; p = df.iloc[-2]
        direction  = "多" if c['MA8'] > c['EMA89'] else "空"
        crossed    = (p['MA8'] <= p['EMA89'] and c['MA8'] > c['EMA89']) or \
                     (p['MA8'] >= p['EMA89'] and c['MA8'] < c['EMA89'])
        avg_vol    = df['vol'].iloc[-22:-2].mean()
        slope      = c['EMA89'] - df['EMA89'].iloc[-4]
        price_5ago = df['close'].iloc[-6]
        rsi_5ago   = df['RSI'].iloc[-6]
        adx_ok     = c['ADX'] >= 20
        vol_ok     = c['vol'] > avg_vol
        slope_ok   = (direction == "多" and slope > 0) or (direction == "空" and slope < 0)
        no_div     = not ((direction == "多" and c['close'] > price_5ago and c['RSI'] < rsi_5ago) or
                          (direction == "空" and c['close'] < price_5ago and c['RSI'] > rsi_5ago))
        atr     = c['ATR14']
        entry_p = c['MA8']
        # 與主掃描一致：從 K 線擺動結構推導 SL/TP
        sl, tp1, tp2, tp3 = find_market_structure_levels(df, entry_p, direction, atr)
        passed = sum([adx_ok, vol_ok, slope_ok, no_div])
        return {
            'asset': inst_id.split('-')[0], 'dir': direction,
            'tf': _TF_LABEL.get(tf, "1H"),
            'price': c['close'], 'ma8': c['MA8'], 'ema89': c['EMA89'],
            'rsi': round(c['RSI'], 1), 'adx': round(c['ADX'], 1),
            'vol_pct': round(c['vol'] / avg_vol * 100),
            'crossed': crossed, 'passed': passed,
            'filters': {'adx_ok': adx_ok, 'vol_ok': vol_ok,
                        'slope_ok': slope_ok, 'no_div': no_div},
            'entry': round_to_tick(entry_p, inst_id),
            'sl':    round_to_tick(sl,      inst_id),
            'tp1':   round_to_tick(tp1,     inst_id),
            'tp2':   round_to_tick(tp2,     inst_id),
            'tp3':   round_to_tick(tp3,     inst_id),
        }
    except Exception:
        return None


def check_watched_coin(asset, chat_id, force_update=False):
    """
    檢查單一自選幣種當前狀態。
    通知邏輯：
      - 觸發完整訊號（全條件通過）→ 立即通知
      - 條件通過數比上次增加 → 通知（訊號在進步）
      - 距上次通知超過 30 分鐘（心跳）→ 通知
      - force_update=True（/watch 指令立即查看）→ 強制通知
    """
    try:
        inst_id = f"{asset}-USDT-SWAP"
        la_tz   = pytz.timezone('America/Los_Angeles')
        now_str = datetime.datetime.now(la_tz).strftime('%H:%M PT')
        now_ts  = time.time()
        url     = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

        # 持倉監控已接管時，不重複推送（但 force_update 仍允許）
        if not force_update:
            with active_positions_lock:
                in_pos = any(p['asset'] == asset for p in active_positions)
            if in_pos:
                return

        # ── 先試完整訊號 ──
        full_sig = None
        for tf in ('15m', '30m', '1h', '4h', '1d'):
            s = fetch_candle_sync(inst_id, tf, max_leverage=20)
            if s:
                full_sig = s
                break

        if full_sig:
            msg = (f"🚨 <b>自選監控觸發完整訊號！</b>  {now_str}\n\n"
                   f"⚡ <b>{full_sig['asset']}</b>  "
                   f"{'🟩多' if full_sig['dir']=='多' else '🟥空'}  "
                   f"{full_sig['tf']}  TA分<b>{full_sig['win_rate']}</b>\n"
                   f"<pre>進場  {format_price(full_sig['entry'])}\n"
                   f"止損  {format_price(full_sig['sl'])}\n"
                   f"TP1   {format_price(full_sig['tp1'])}\n"
                   f"TP2   {format_price(full_sig['tp2'])}\n"
                   f"TP3   {format_price(full_sig['tp3'])}</pre>"
                   f"▸ TP1達標 → 止損移至 <code>{format_price(full_sig['entry'])}</code>")
            with last_scan_lock:
                last_scan_cache[asset] = {**full_sig, 'cached_at': now_ts}
            markup = {"inline_keyboard": [[
                {"text": f"✅ 追蹤 {asset}{full_sig['dir']}",
                 "callback_data": f"open_{asset}_{full_sig['dir']}"},
                {"text": "🗑️ 停止監控", "callback_data": f"unwatch_{asset}"},
            ]]}
            requests.post(url, json={"chat_id": str(chat_id), "text": msg,
                                     "parse_mode": "HTML", "reply_markup": markup})
            # 更新快取的 passed 數和上次通知時間
            with watch_list_lock:
                for w in watch_list:
                    if w['asset'] == asset:
                        w['last_passed']   = 4
                        w['last_notified'] = now_ts
            return

        # ── 無完整訊號 → 先試近期交叉狀態，再退回趨勢狀態 ──
        best = None
        for tf in ('15m', '30m', '1h', '4h', '1d'):
            st = fetch_coin_status(inst_id, tf)
            if st and (best is None or st['passed'] > best['passed']):
                best = st

        # 若 fetch_coin_status 也找不到（交叉時間太久遠），用趨勢延續偵測
        if not best:
            for tf in ('15m', '30m', '1h', '4h', '1d'):
                st = fetch_trend_state(inst_id, tf)
                if st and (best is None or st['passed'] > best['passed']):
                    best = st

        if not best:
            print(f"⚠️ 自選監控 {asset}：所有偵測均失敗，跳過")
            return

        # ── 決定是否要推送 ──
        with watch_list_lock:
            entry = next((w for w in watch_list if w['asset'] == asset), None)
            prev_passed    = entry.get('last_passed', -1)    if entry else -1
        improved       = best['passed'] > prev_passed
        should_notify  = force_update or improved   # 趨勢未變化不推播

        if not should_notify:
            print(f"👁 {asset}：passed={best['passed']}（同上次），趨勢無變化，跳過推送")
            return

        d   = "🟩多" if best['dir'] == "多" else "🟥空"
        bar = "■" * best['passed'] + "□" * (4 - best['passed'])
        f   = best['filters']
        reason = ""
        if improved:      reason = "📈 條件進步！"
        elif force_update: reason = "🔍 即時查詢"

        # 趨勢來源標籤
        is_trend_mode = not best.get('crossed', False) and not best.get('recent_cross', False)
        if is_trend_mode:
            trend_label = f"趨勢延續（MA8{'>' if best['dir']=='多' else '<'}EMA89 差{best.get('gap_pct',0):.1f}%）"
            pullback_note = ""
            if best.get('is_pullback'):
                pullback_note = f"\n🎯 <b>回踩 MA8（{best.get('pullback_pct',0):.2f}%），潛在再入場機會</b>"
        else:
            trend_label = "近期交叉訊號"
            pullback_note = ""

        lines = (
            f"   ADX={best['adx']} {'✅' if f['adx_ok'] else '❌'}\n"
            f"   成交量={best['vol_pct']}%均量 {'✅' if f['vol_ok'] else '❌'}\n"
            f"   EMA89斜率 {'✅' if f['slope_ok'] else '❌'}\n"
            f"   RSI={best['rsi']} {'✅' if f['no_div'] else '❌ 背離'}"
        )
        msg = (f"👁 <b>{best['asset']} 自選監控</b>  {now_str}  {reason}\n"
               f"{d}  {best['tf']}  {trend_label}  [{bar}] {best['passed']}/4\n"
               f"現價 <code>{format_price(best['price'])}</code>\n"
               f"{lines}"
               f"{pullback_note}\n")
        if best['passed'] >= 2:
            entry_label = "MA8（回踩再入）" if is_trend_mode else "進場"
            msg += (f"\n<b>─ 當前建議點位（參考）─</b>\n"
                    f"<pre>{entry_label}  {format_price(best['entry'])}\n"
                    f"止損  {format_price(best['sl'])}\n"
                    f"TP1   {format_price(best['tp1'])}\n"
                    f"TP2   {format_price(best['tp2'])}\n"
                    f"TP3   {format_price(best['tp3'])}</pre>"
                    f"<i>{'趨勢延續回踩點位，非新交叉' if is_trend_mode else '尚未通過全部條件，點位僅供參考'}</i>")
        markup = {"inline_keyboard": [[
            {"text": "🗑️ 停止監控", "callback_data": f"unwatch_{asset}"}
        ]]}
        requests.post(url, json={"chat_id": str(chat_id), "text": msg,
                                 "parse_mode": "HTML", "reply_markup": markup})

        # 更新快取
        with watch_list_lock:
            for w in watch_list:
                if w['asset'] == asset:
                    w['last_passed']   = best['passed']
                    w['last_notified'] = now_ts
        save_watch_list(watch_list)

    except Exception as e:
        print(f"❌ check_watched_coin({asset}) 例外：{e}")
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": str(chat_id),
                      "text": f"⚠️ 自選監控 {asset} 發生錯誤：{e}"},
                timeout=5
            )
        except Exception:
            pass


def run_watchlist_check():
    """每 5 分鐘檢查所有自選監控幣種"""
    with watch_list_lock:
        items = list(watch_list)
    if not items:
        print("👁 自選監控：清單為空，跳過")
        return
    print(f"👁 自選監控：檢查 {len(items)} 顆幣：{[i['asset'] for i in items]}")
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(check_watched_coin, item['asset'], item['chat_id']): item['asset']
                   for item in items}
        for future, asset in futures.items():
            try:
                future.result(timeout=60)
            except Exception as e:
                print(f"❌ 自選監控 {asset} 執行錯誤：{e}")


def handle_telegram_updates():
    print("🤖 幣圈分析師【勝率精選 5 幣版 + 主力動向確認 + 持倉監控版】雷達正在開機...")
    offset = None
    la_tz = pytz.timezone('America/Los_Angeles')
    last_scan_slot           = None  # (hour, minute) 上次觸發的整點/半點
    last_monitor_time        = 0
    last_watchlist_check_time_local = 0
    last_stats_date          = None  # 每日勝率播報去重
    last_journal_date        = None  # 每日交易日誌去重
    last_weekly_backup_date  = None  # 每週備份去重

    while True:
        try:
            now_la   = datetime.datetime.now(la_tz)
            now_ts   = time.time()

            # A0. 自選監控（每 5 分鐘）
            if now_ts - last_watchlist_check_time_local >= WATCHLIST_INTERVAL_MINUTES * 60:
                t = threading.Thread(target=run_watchlist_check)
                t.daemon = True
                t.start()
                last_watchlist_check_time_local = now_ts

            # A. 固定整點/半點掃描（24小時全天，每 10 分鐘觸發一次）
            _h, _m = now_la.hour, now_la.minute
            _is_slot = _m in (0, 10, 20, 30, 40, 50)   # 每 10 分鐘一槽
            _cur_slot = (_h, _m)
            if _is_slot and _cur_slot != last_scan_slot:
                last_scan_slot = _cur_slot
                print(f"🔔 觸發定時掃描：{now_la.strftime('%H:%M')} PT")
                t = threading.Thread(target=scan_worker_thread, args=("定時自動速報", TELEGRAM_CHAT_ID, True, True, True))
                t.daemon = True
                t.start()

            # B0. 每週日自動備份（週日 23:55 PT）
            if now_la.weekday() == 6 and now_la.hour == 23 and 55 <= now_la.minute < 57 \
                    and last_weekly_backup_date != now_la.date():
                last_weekly_backup_date = now_la.date()
                print(f"📦 觸發每週交易記錄備份：{now_la.strftime('%Y-%m-%d')}")
                t = threading.Thread(target=export_trade_data, args=(TELEGRAM_CHAT_ID,))
                t.daemon = True
                t.start()

            # B1. 每日交易日誌（23:50 PT，在當天結束前總結）
            if now_la.hour == 23 and 50 <= now_la.minute < 52 and last_journal_date != now_la.date():
                last_journal_date = now_la.date()
                print(f"📔 觸發每日交易日誌：{now_la.strftime('%Y-%m-%d')}")
                t = threading.Thread(target=send_daily_journal, args=(TELEGRAM_CHAT_ID,))
                t.daemon = True
                t.start()

            # B2. 每日勝率播報（00:00 PT）
            if now_la.hour == 0 and now_la.minute < 2 and last_stats_date != now_la.date():
                last_stats_date = now_la.date()
                print(f"📊 觸發每日勝率播報：{now_la.strftime('%Y-%m-%d')}")
                t = threading.Thread(target=send_stats_report, args=(TELEGRAM_CHAT_ID,))
                t.daemon = True
                t.start()

            # B. 持倉監控（Plan B 動態間隔）
            # 掛單等待 / TP1-TP2 已命中 → 5 分鐘；已成交未到 TP1 → 10 分鐘
            with active_positions_lock:
                has_tp_hit   = any(p.get('tp1_hit') or p.get('tp2_hit') for p in active_positions)
                has_unfilled = any(not p.get('filled', False) for p in active_positions)
            monitor_interval = 180  # 每 3 分鐘監控一次（掛單等待 / 持倉均適用）
            if now_ts - last_monitor_time >= monitor_interval:
                print(f"🔍 觸發持倉監控：{now_la.strftime('%H:%M')}")
                t = threading.Thread(target=run_position_monitor)
                t.daemon = True
                t.start()
                last_monitor_time = now_ts

            # C. 手動指令監聽
            get_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
            params = {"timeout": 2}
            if offset:
                params["offset"] = offset

            res = requests.get(get_url, params=params, timeout=5).json()
            if res.get("ok") and res.get("result"):
                for update in res["result"]:
                    offset = update["update_id"] + 1
                    # ── Inline button 回調處理 ──
                    if "callback_query" in update:
                        cq      = update["callback_query"]
                        cb_id   = cq["id"]
                        cb_data = cq.get("data", "")
                        cb_chat = str(cq["message"]["chat"]["id"])

                        if cb_data.startswith("open_"):
                            parts = cb_data.split("_", 2)   # ["open","ETH","多"]
                            if len(parts) == 3:
                                asset_cb, dir_cb = parts[1], parts[2]
                                with active_positions_lock:
                                    already = any(p['asset'] == asset_cb and p['dir'] == dir_cb
                                                  for p in active_positions)
                                if already:
                                    answer_callback(cb_id, f"✅ {asset_cb}{dir_cb} 已在追蹤中", alert=True)
                                else:
                                    with last_scan_lock:
                                        sig_cb = last_scan_cache.get(asset_cb)
                                    # 若主訊號快取無資料，查接近訊號快取
                                    if not (sig_cb and sig_cb.get('dir') == dir_cb):
                                        with near_miss_lock:
                                            nm_cb = near_miss_cache.get(asset_cb)
                                        if nm_cb and nm_cb.get('dir') == dir_cb:
                                            sig_cb = nm_cb
                                    if sig_cb and sig_cb.get('dir') == dir_cb:
                                        now_ts_cb = time.time()
                                        _raw_entry = sig_cb['entry']
                                        _sig_type  = sig_cb.get('signal_type', 'trend')
                                        # 區間訊號：訊號在支撐/壓力帶觸發，價格立即反彈（或拒絕）
                                        # → 使用「突破模式」fill（high≥entry 或 low≤entry）
                                        #   避免「回調模式」讓正確反彈方向的倉位永遠無法確認成交
                                        if _sig_type == 'range':
                                            if dir_cb == "多":
                                                _sp = _raw_entry * 0.997  # entry > signal_price → 突破多
                                            else:
                                                _sp = _raw_entry * 1.003  # entry < signal_price → 突破空
                                        else:
                                            _sp = sig_cb.get('price', _raw_entry)
                                        _is_mkt_cb = sig_cb.get('is_market_entry', False)
                                        new_pos_cb = {
                                            'asset': sig_cb['asset'], 'dir': sig_cb['dir'],
                                            'tf': sig_cb['tf'], 'entry': _raw_entry,
                                            'sl': sig_cb['sl'],
                                            'orig_sl': sig_cb['sl'],
                                            'tp1': sig_cb['tp1'],
                                            'tp2': sig_cb['tp2'], 'tp3': sig_cb['tp3'],
                                            'tp_count':       sig_cb.get('tp_count', 3),
                                            'signal_price':   _sp,
                                            'signal_type':    _sig_type,
                                            'atr_trail':      sig_cb.get('atr_trail', 0),
                                            'reported_at': now_ts_cb,
                                            'last_checked_ts': now_ts_cb,
                                            'filled': _is_mkt_cb,  # 市價進場：立即視為已成交
                                            'fill_ts': now_ts_cb if _is_mkt_cb else 0,
                                            'entry_fr': sig_cb.get('entry_fr'),
                                        }
                                        with active_positions_lock:
                                            active_positions.append(new_pos_cb)
                                            save_positions(active_positions)
                                        answer_callback(cb_id, f"⏳ {asset_cb}{dir_cb} 掛單監控已開始！", alert=True)
                                        print(f"📌 按鈕追蹤：{asset_cb} {dir_cb}")
                                    else:
                                        answer_callback(cb_id, "⚠️ 訊號已過期，請重新 /scan", alert=True)

                        elif cb_data.startswith("cancel_pos_"):
                            parts_cp = cb_data.split("_", 3)  # ["cancel","pos","ETH","多"]
                            if len(parts_cp) == 4:
                                asset_cp, dir_cp = parts_cp[2], parts_cp[3]
                                with active_positions_lock:
                                    before_cp = len(active_positions)
                                    active_positions[:] = [p for p in active_positions
                                                           if not (p['asset'] == asset_cp and p['dir'] == dir_cp
                                                                   and not p.get('filled', False))]
                                    removed_cp = before_cp > len(active_positions)
                                    if removed_cp:
                                        save_positions(active_positions)
                                if removed_cp:
                                    answer_callback(cb_id, f"🗑️ {asset_cp}{dir_cp} 掛單已撤銷，停止監控", alert=True)
                                else:
                                    answer_callback(cb_id, f"⚠️ 找不到 {asset_cp}{dir_cp} 的掛單（可能已成交或已取消）", alert=True)

                        elif cb_data.startswith("unwatch_"):
                            asset_uw = cb_data.split("_", 1)[1]
                            with watch_list_lock:
                                watch_list[:] = [w for w in watch_list if w['asset'] != asset_uw]
                                save_watch_list(watch_list)
                            answer_callback(cb_id, f"🗑️ {asset_uw} 已停止監控", alert=True)

                        elif cb_data == "ack_monitor":
                            answer_callback(cb_id, "✅ 已收到", alert=False)

                        elif cb_data == "cmd_scan":
                            answer_callback(cb_id, "⚡ 開始掃描，請稍候約 15 秒...", alert=False)
                            t = threading.Thread(target=scan_worker_thread, args=("手動現場突擊播報", cb_chat), kwargs={"include_fib": True})
                            t.daemon = True
                            t.start()

                        elif cb_data == "cmd_stats":
                            answer_callback(cb_id, "📊 查詢中...", alert=False)
                            send_stats_report(cb_chat)

                    if "message" in update and "text" in update["message"]:
                        msg = update["message"]
                        text = msg["text"].strip()
                        chat_id = str(msg["chat"]["id"])

                        if text.lower().startswith("/help"):
                            help_text = (
                                "📖 <b>指令說明</b>\n"
                                "─────────────────────────\n"
                                "🔍 <b>掃描與訊號</b>\n"
                                "/scan — 全網掃描，精選最高技術分訊號（趨勢/區間/背離）\n"
                                "/[幣種] — 單幣分析，例如 /btc /eth /sol\n\n"
                                "📌 <b>持倉管理</b>\n"
                                "/open [幣種] [方向] — 確認開倉並加入 TP/SL 監控\n"
                                "　例：/open BTC 多　或　/open ETH 空\n"
                                "/close [幣種] [方向] — 平倉後解除監控\n"
                                "　例：/close BTC 多\n"
                                "/holding — 查看目前所有持倉的監控狀態\n\n"
                                "👁 <b>自選監控</b>\n"
                                "/watch [幣種] — 加入自選監控（定時掃描通知）\n"
                                "/unwatch [幣種] — 移除自選監控\n"
                                "/watching — 查看自選監控清單\n\n"
                                "📊 <b>統計與備份</b>\n"
                                "/stats — 查看近 30 天真實交易勝率統計\n"
                                "/export_data — 匯出完整交易記錄 JSON（人工備份）\n"
                                "/resetstats — 清空歷史勝率紀錄，重新開始累積\n\n"
                                "─────────────────────────\n"
                                "<i>TP/SL 觸發、市場惡化時系統自動推播\n"
                                "TP1 達標後止損自動移至成本價（保本）\n"
                                "槓桿建議僅供參考，請自行控管風險</i>"
                            )
                            requests.post(
                                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                json={"chat_id": chat_id, "text": help_text, "parse_mode": "HTML"}
                            )

                        elif text.startswith("/scan"):
                            print(f"⚡ 收到 /scan 指令")
                            confirm_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
                            requests.post(confirm_url, json={"chat_id": chat_id, "text": "⚡ 收到指令！正在進行全網掃描 + 主力動向確認，精選最高技術分訊號（趨勢/區間/背離），請稍候約 15 秒..."})
                            t = threading.Thread(target=scan_worker_thread, args=("手動現場突擊播報", chat_id), kwargs={"include_fib": True})
                            t.daemon = True
                            t.start()

                        elif text.lower().startswith("/fibcheck"):
                            print(f"📐 收到 /fibcheck 指令：{text}")
                            t = threading.Thread(target=send_fibcheck_report, args=(text, chat_id))
                            t.daemon = True
                            t.start()

                        elif text.startswith("/holding"):
                            print(f"📋 收到 /holding 指令")
                            t = threading.Thread(target=send_holding_summary, args=(chat_id,))
                            t.daemon = True
                            t.start()

                        elif text.lower().startswith("/open"):
                            print(f"📌 收到 /open 指令：{text}")
                            t = threading.Thread(target=handle_open_command, args=(text, chat_id))
                            t.daemon = True
                            t.start()

                        elif text.lower().startswith("/watch "):
                            coin_w = text.split()[1].upper().strip() if len(text.split()) > 1 else ""
                            if coin_w:
                                with watch_list_lock:
                                    already_w = any(w['asset'] == coin_w for w in watch_list)
                                    if not already_w:
                                        watch_list.append({'asset': coin_w, 'chat_id': chat_id, 'added_at': time.time()})
                                        save_watch_list(watch_list)
                                msg_w = f"👁 <b>{coin_w}</b> 已加入自選監控，每 {WATCHLIST_INTERVAL_MINUTES} 分鐘更新一次。\n發送 /unwatch {coin_w} 可停止。" if not already_w else f"⚠️ {coin_w} 已在監控清單中。"
                                requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                              json={"chat_id": chat_id, "text": msg_w, "parse_mode": "HTML"})
                                if not already_w:
                                    t = threading.Thread(target=check_watched_coin, args=(coin_w, chat_id, True))
                                    t.daemon = True; t.start()

                        elif text.lower().startswith("/unwatch"):
                            coin_uw = text.split()[1].upper().strip() if len(text.split()) > 1 else ""
                            if coin_uw:
                                with watch_list_lock:
                                    before = len(watch_list)
                                    watch_list[:] = [w for w in watch_list if w['asset'] != coin_uw]
                                    removed = before > len(watch_list)
                                    if removed:
                                        save_watch_list(watch_list)
                                msg_uw = f"🗑️ <b>{coin_uw}</b> 已從監控清單移除。" if removed else f"⚠️ {coin_uw} 不在監控清單中。"
                                requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                              json={"chat_id": chat_id, "text": msg_uw, "parse_mode": "HTML"})

                        elif text.lower().startswith("/watching"):
                            with watch_list_lock:
                                wl = list(watch_list)
                            if wl:
                                lines_wl = "\n".join(f"• {w['asset']}" for w in wl)
                                msg_wl = f"👁 <b>自選監控清單（{len(wl)} 顆）</b>\n{lines_wl}\n\n每 {WATCHLIST_INTERVAL_MINUTES} 分鐘更新一次\n輸入 /unwatch [幣種] 移除"
                            else:
                                msg_wl = "📭 監控清單為空。\n輸入 /watch BTC 開始監控。"
                            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                          json={"chat_id": chat_id, "text": msg_wl, "parse_mode": "HTML"})

                        elif text.lower().startswith("/close"):
                            print(f"🗑️ 收到 /close 指令：{text}")
                            t = threading.Thread(target=handle_close_command, args=(text, chat_id))
                            t.daemon = True
                            t.start()

                        elif text.lower().startswith("/stats"):
                            print(f"📊 收到 /stats 指令，同步執行")
                            send_stats_report(chat_id)

                        elif text.lower().startswith("/export_data") or text.lower().startswith("/exportdata"):
                            print(f"📦 收到 /export_data 指令")
                            t = threading.Thread(target=export_trade_data, args=(chat_id,))
                            t.daemon = True
                            t.start()

                        elif text.lower().startswith("/resetstats"):
                            print(f"🗑️ 收到 /resetstats 指令，清空勝率紀錄")
                            save_stats([])
                            requests.post(
                                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                json={"chat_id": chat_id,
                                      "text": "🗑️ <b>勝率紀錄已清空</b>\n\n從現在起依照最新策略重新統計。\n歷史資料已全部移除，後續的 TP/SL 結果將從零開始累積。",
                                      "parse_mode": "HTML"}
                            )

                        elif text.lower().startswith("/fib618") or text.lower().startswith("/fib"):
                            print(f"📐 收到 /fib618 指令")
                            t = threading.Thread(target=scan_fib618_setups, args=(chat_id,))
                            t.daemon = True
                            t.start()

                        elif text.startswith("/") and len(text) > 1:
                            # 通用幣種查詢：/eth /btc /sol /doge 等
                            coin_cmd = text.split()[0].lstrip("/").split("@")[0]
                            if coin_cmd and coin_cmd.isalpha() and coin_cmd.lower() not in ("open","close","scan","holding","watch","unwatch","watching","stats","resetstats","help","fib618","fib"):
                                print(f"🔍 收到幣種查詢指令：/{coin_cmd.upper()}")
                                requests.post(
                                    f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                    json={"chat_id": chat_id, "text": f"🔍 正在分析 {coin_cmd.upper()}，請稍候..."}
                                )
                                t = threading.Thread(target=send_coin_analysis, args=(coin_cmd, chat_id))
                                t.daemon = True
                                t.start()

        except Exception as e:
            print(f"⚠️ 監聽異常: {e}")
            time.sleep(1)
            continue
        time.sleep(1)

if __name__ == '__main__':
    print("🤖 幣圈分析師【勝率精選 5 幣版 + 主力動向確認版】雷達正在開機...")
    handle_telegram_updates()
