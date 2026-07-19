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

if not TELEGRAM_BOT_TOKEN:
    raise ValueError("❌ 未設置 TELEGRAM_BOT_TOKEN 環境變數，請在 Secrets 中添加。")
if not TELEGRAM_CHAT_ID:
    raise ValueError("❌ 未設置 TELEGRAM_CHAT_ID 環境變數，請在 Secrets 中添加。")

# ==================== 🗂️ 2. OKX 官方全量資產動態抓取 ====================
def get_all_okx_swap_assets():
    """回傳 (assets列表, {instId: max_leverage} 字典)"""
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

    if direction == "多":
        # SL：entry 下方（需有一定距離，≥ 0.1% 避免貼身）最近支撐
        below = [s for s in supports if s < entry * 0.999]
        sl    = max(below) if below else entry - atr * 2.0

        # TP：entry 上方壓力由近到遠
        above = sorted(r for r in resistances if r > entry * 1.001)
        tp1   = above[0] if len(above) >= 1 else entry + atr * 1.5
        tp2   = above[1] if len(above) >= 2 else tp1  + atr * 1.5
        tp3   = above[2] if len(above) >= 3 else tp2  + atr * 2.0
    else:
        # SL：entry 上方最近壓力
        above = [r for r in resistances if r > entry * 1.001]
        sl    = min(above) if above else entry + atr * 2.0

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


def get_candle_range_since(inst_id, since_ts, bar="1H", no_margin=False):
    """取得 since_ts 以來所有 K 線的最高價和最低價。
    用於偵測監控間隔中曾觸碰的價格極值，防止漏掉 TP/SL/填單事件。
    no_margin=True：嚴格只取 since_ts 之後開始的 K 棒（用於填單判定，避免納入舊棒）。
    回傳 (range_high, range_low) 或 (None, None)。
    """
    try:
        url = f"{BASE_URL}/api/v5/market/candles?instId={inst_id}&bar={bar}&limit=30"
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

            # ── 近中性容忍（±0.5%）──
            # MA8 與 EMA89 差距 ≤ 0.5% 時，1H 實際上是盤整轉折期，
            # 不應視為明確反向；此時 BTC/ETH 15m 的突破訊號往往先於 1H 切換，
            # 嚴格二元擋掉會錯過所有初段行情。
            if not aligned:
                gap_pct = abs(last['MA8'] - last['EMA89']) / (last['EMA89'] + 1e-10) * 100
                if gap_pct <= 0.5:
                    aligned = True   # 差距 ≤ 0.5%：視為中性，放行

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
    url = f"{BASE_URL}/api/v5/market/candles?instId={asset}&bar={bar_param}&limit=100"
    try:
        res = requests.get(url, timeout=1.5).json()
        if res.get('code') == '0' and len(res['data']) >= 90:
            df = pd.DataFrame(res['data'], columns=['ts','open','high','low','close','vol','volCcy','volCcyQuote','state'])
            for col in ['open','high','low','close','vol']:
                df[col] = df[col].astype(float)
            df = df.iloc[::-1].reset_index(drop=True)

            # ── 基礎指標 ──
            df['MA8']  = df['close'].rolling(8).mean()
            df['EMA89']= df['close'].ewm(span=89, adjust=False).mean()

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

            # ── 若無新交叉，補偵測「MA8 回踩反彈」（趨勢延伸進場）──
            # 趨勢已確立（MA8 持續 > EMA89）時，價格回踩 MA8 後反彈 = 有效再進場點。
            # 不需要新的交叉，捕捉趨勢中每一波的延伸機會。
            _is_pullback = False
            if not (is_cross_up or is_cross_down):
                _n_chk = min(8, len(df) - 2)
                _up_bars = sum(1 for i in range(1, _n_chk + 1)
                               if df.iloc[-i]['MA8'] > df.iloc[-i]['EMA89'])
                _dn_bars = sum(1 for i in range(1, _n_chk + 1)
                               if df.iloc[-i]['MA8'] < df.iloc[-i]['EMA89'])

                if _up_bars >= 6 and c_last['MA8'] > c_last['EMA89']:
                    # 上升趨勢：找最近3根中 low 回踩至 MA8 0.5% 內且 close > MA8 的根
                    for _i in range(1, 4):
                        _b = df.iloc[-_i]
                        if _b['low'] <= _b['MA8'] * 1.005 and _b['close'] > _b['MA8']:
                            is_cross_up  = True
                            cross_vol    = _b['vol']
                            _cross_i     = _i
                            _is_pullback = True
                            break

                elif _dn_bars >= 6 and c_last['MA8'] < c_last['EMA89']:
                    # 下降趨勢：找最近3根中 high 回踩至 MA8 0.5% 內且 close < MA8 的根
                    for _i in range(1, 4):
                        _b = df.iloc[-_i]
                        if _b['high'] >= _b['MA8'] * 0.995 and _b['close'] < _b['MA8']:
                            is_cross_down = True
                            cross_vol     = _b['vol']
                            _cross_i      = _i
                            _is_pullback  = True
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

            # ① ADX 過濾：< 20 表示盤整市場，MA 交叉失誤率高
            if current_adx < 20:
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
            # RSI 超買/超賣時進場，即使方向對也會被震倉。
            # 現有評分已降低極端RSI的分數，但高ADX仍可蓋過而通過門檻，需硬擋。
            if direction == "多" and current_rsi > 76:
                return None   # RSI > 76 = 過度超買，多方動能耗盡在即，不追頂
            if direction == "空" and current_rsi < 24:
                return None   # RSI < 24 = 過度超賣，空方動能耗盡在即，不追底

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
            _htf_adx_min = 20 if tf in ("15m", "30m") else 25
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

            score = base_score + sentiment_bonus + vol_bonus + btc_bonus + fr_bonus

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

            # ── 進場錨定點（所有時框統一用 MA8，交叉當下的訊號線）──
            entry_price  = current_ma8
            anchor_label = f"MA8={format_price(current_ma8)}"

            # ⑦ SL / TP：從 K 線擺動結構推導，不用固定倍數百分比
            #   SL  = entry 方向後方最近支撐/壓力（跌破代表訊號失效）
            #   TP1/2/3 = entry 方向前方歷史壓力/支撐，由近到遠
            #   找不到足夠水位時以 ATR 補全，保證永遠回傳有效數值
            sl_price, tp1, tp2, tp3 = find_market_structure_levels(
                df, entry_price, direction, current_atr)

            # ── 斐波那契回撤匯流：找最近擺動水位，判斷進場是否在 Fib 關鍵支撐/壓力 ──
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

            # ── 手續費最低保本距離：TP1 < 0.25% 時「TP1後移至開倉止損」必虧 ──
            # OKX taker fee ≈ 0.05%；開倉+TP1平倉+SL平倉 共 3 筆 = 0.15% 總費用
            # TP1 需 ≥ 0.25% 才能讓 50% TP1利潤 > 全部費用，否則保本止損實為虧損
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
            if direction == "多":
                gap_pct = (current_price - entry_price) / entry_price * 100
                if current_rsi > 65:
                    entry_type = f"⚠️ {tf_tag}RSI過熱({current_rsi:.1f})，宜等回踩支撐後再掛限價 {anchor_label}"
                elif current_price < entry_price:
                    entry_type = (f"🔴 {tf_tag}現價 {format_price(current_price)} 低於 {anchor_label}，"
                                  f"偏離 {abs(gap_pct):.2f}%｜確認大週期趨勢仍多，可市價進場")
                elif gap_pct <= 0.5:
                    entry_type = (f"⚡ {tf_tag}現價緊貼 {anchor_label}（差 {gap_pct:.2f}%），"
                                  f"建議<b>市價進場</b>或快速掛限價 {format_price(entry_price)}")
                else:
                    entry_type = f"📌 {tf_tag}掛限價 {anchor_label}，止損={format_price(sl_price)}"
            else:
                gap_pct = (entry_price - current_price) / entry_price * 100
                if current_rsi < 35:
                    entry_type = f"⚠️ {tf_tag}RSI超賣({current_rsi:.1f})，宜等反彈至壓力後再掛限價 {anchor_label}"
                elif current_price > entry_price:
                    entry_type = (f"🔴 {tf_tag}現價 {format_price(current_price)} 高於 {anchor_label}，"
                                  f"偏離 {abs(gap_pct):.2f}%｜確認大週期趨勢仍空，可市價進場")
                elif gap_pct <= 0.5:
                    entry_type = (f"⚡ {tf_tag}現價緊貼 {anchor_label}（差 {gap_pct:.2f}%），"
                                  f"建議<b>市價進場</b>或快速掛限價 {format_price(entry_price)}")
                else:
                    entry_type = f"📌 {tf_tag}掛限價 {anchor_label}，止損={format_price(sl_price)}"

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
                "entry_fr":       round(funding_rate * 100, 4),  # 進場時資金費率（%）
                "fib_level":      _fib_lbl,
                "fib_price":      _fib_px,
                "fib_near":       _fib_near,
                "fib_dist":       _fib_dist,
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
        tf_label = _TF_LABEL.get(tf, tf.upper())
        dir_tag  = "頂背離做空" if direction == "空" else "底背離做多"
        tf_tag   = f"{tf_label}{dir_tag}"

        entry_desc = (f"⚡ {tf_tag}  {div_desc}  |  "
                      f"現價 {format_price(price)}  RSI {rsi:.0f}  ADX {adx:.0f}  "
                      f"止損 {format_price(sl_price)}  TP {format_price(tp1)} / {format_price(tp2)}")

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
    print("📡 獲取市場環境指標...")
    with ThreadPoolExecutor(max_workers=2) as _env_ex:
        _f_ref = _env_ex.submit(get_ecosystem_ref_trends)
        _f_fr  = _env_ex.submit(get_market_avg_funding_rate)
    ref_trends = _f_ref.result()
    market_fr  = _f_fr.result()
    print(f"   生態鏈趨勢: { {k: v for k, v in ref_trends.items()} } | 市場費率均值: {market_fr*100:.4f}%")

    trend_signals = []
    range_signals = []
    div_signals   = []
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

    print(f"   趨勢候選 {len(trend_signals)} 組 | 區間候選 {len(range_signals)} 組 | 背離候選 {len(div_signals)} 組")

    # ── 組合：趨勢前 2 + 背離前 1 + 區間前 1（最多 4 個）──
    # 優先順序：趨勢 > 背離 > 區間
    combined = trend_signals[:2] + div_signals[:1] + range_signals[:1]

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

    # 去重：同幣種同方向只保留第一個（趨勢優先，已排在前面）
    seen = set()
    deduped = []
    for s in combined:
        key = (s['asset'], s['dir'])
        if key not in seen:
            seen.add(key)
            deduped.append(s)

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

def record_trade_outcome(pos, outcome):
    """記錄交易結果；outcome: win/loss"""
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
    records.append({
        "asset":    pos['asset'],
        "dir":      pos['dir'],
        "tf":       pos.get('tf', '—'),
        "entry":    pos.get('entry'),
        "entry_fr": round(entry_fr * 100, 4),   # 百分比，保留4位
        "outcome":  outcome,
        "timestamp": time.time(),
    })
    save_stats(records)
    print(f"📊 記錄交易結果：{pos['asset']} {pos['dir']} → {outcome}  費率{entry_fr*100:.4f}%")

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
    tp2   = pos['tp2']
    tp3   = pos['tp3']

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
        signal_price = pos.get('signal_price', entry)  # 無記錄時 fallback = entry（視為回調）
        if dir == "多":
            if entry > signal_price:   # 突破多：等高點達到進場
                filled_by_candle = fill_eff_high is not None and fill_eff_high >= entry
            else:                      # 回調多：等低點跌到進場
                filled_by_candle = fill_eff_low  is not None and fill_eff_low  <= entry
        else:  # 空
            if entry < signal_price:   # 突破空：等低點跌到進場
                filled_by_candle = fill_eff_low  is not None and fill_eff_low  <= entry
            else:                      # 回調空：等高點漲到進場
                filled_by_candle = fill_eff_high is not None and fill_eff_high >= entry
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
    # 以 1m K 線為主要 TP/SL 判定來源：精度最高，不受大時框 wick 誤差影響
    effective_high, effective_low = get_candle_range_since(inst_id, since_ts, '1m', no_margin=True)
    effective_high = max(effective_high, current_price) if effective_high is not None else current_price
    effective_low  = min(effective_low,  current_price) if effective_low  is not None else current_price
    # 原始時框補充：只納入 since_ts 之後「新開盤」的 K 棒（no_margin=True，不往前抓舊棒）
    # 例外：成交後第一個 bar 週期內強制也用 no_margin=True，防止成交那根 K 線
    # 在成交「前」的 wick 被誤算為 TP/SL 觸碰。
    _bar_secs = {"15m": 900, "30m": 1800, "1H": 3600, "4H": 14400, "1D": 86400}
    _bar_dur  = _bar_secs.get(bar, 3600)
    _rng_h, _rng_l = get_candle_range_since(inst_id, since_ts, bar, no_margin=True)
    if _rng_h is not None:
        effective_high = max(effective_high, _rng_h)
    if _rng_l is not None:
        effective_low  = min(effective_low,  _rng_l)
    pos['last_checked_ts'] = time.time()

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

    # TP 確認緩衝：要求超過 TP 0.03% 才算達標，避免 1-pip wick 誤觸
    # 0.03% 足以擋掉最小假 wick（原始 ZKP 案例：wick 只超 TP 0.022%）
    # 比舊值 0.05% 寬鬆：讓 wick 超 TP 0.03-0.05% 的真實觸碰正常觸發 TP1，
    # 防止 TP1 被錯過後 SL 仍在原位導致不必要虧損。
    # SL 不加緩衝（止損要快，只要 wick 碰到就觸發）
    TP_CONFIRM = 1.0003   # 多頭 TP：effective_high >= tp * TP_CONFIRM
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
                # TP2 已完成，追蹤止損繼續鎖利剩餘 20%
                trail_dist = pos.get('trail_dist', entry * 0.015)
                new_trail_sl = current_price - trail_dist
                if new_trail_sl > sl:
                    pos['sl'] = new_trail_sl
                    sl = new_trail_sl
                status = "🔵 TP2已完成"
                action = f"剩餘20%持倉中，等待TP3 <code>{format_price(tp3)}</code>，追蹤止損 {format_price(sl)}，現價 {format_price(current_price)}"
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
                pos['tp2_hit'] = True
                pos['sl'] = tp1   # SL 立即鎖至 TP1，剩餘倉位零風險
                sl = tp1
                status = "🔵 止盈2達標"
                if not pos.get('tp1_hit'):
                    # K線一根直接衝破 TP1→TP2，補記 TP1 並在訊息中說明
                    pos['tp1_hit'] = True
                    pos['trail_dist'] = entry - pos.get('orig_sl', entry - (tp1 - entry))
                    action = (f"✅ K線高點 {format_price(effective_high)} 同時穿越 TP1 <code>{format_price(tp1)}</code> 並達止盈2 <code>{format_price(tp2)}</code>，"
                              f"現價 {format_price(current_price)}\n"
                              f"🔒 TP1 段已自動確認，止損鎖至TP1（<code>{format_price(tp1)}</code>），建議平倉50%")
                else:
                    action = (f"✅ K線高點 {format_price(effective_high)} 已達止盈2 {format_price(tp2)}，"
                              f"現價 {format_price(current_price)}｜<b>建議將止損上移至TP1（<code>{format_price(tp1)}</code>）</b>")
                push = True
        elif effective_high >= tp1 * TP_CONFIRM:
            if pos.get('tp1_hit'):
                # 追蹤止損：TP1 後每次監控都把 SL 往上拉緊鎖利
                trail_dist = pos.get('trail_dist', entry * 0.015)
                new_trail_sl = current_price - trail_dist
                if new_trail_sl > sl:
                    pos['sl'] = new_trail_sl
                    sl = new_trail_sl
                # TP1 已在前次通知，本次靜默監控等待 TP2
                status = "🟢 TP1已完成"
                action = f"剩餘50%持倉中，等待TP2 <code>{format_price(tp2)}</code>，追蹤止損 {format_price(sl)}，現價 {format_price(current_price)}"
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
                status = "🟢 止盈1達標"
                # 計算費後真正保本點（含開倉+TP1+SL 三筆手續費）
                # SL_be = 2×E×(1+f)/(1-f) - TP1，保證移至此點後總P&L ≥ 0
                _f_r  = 0.0005
                _sl_be = max(entry, 2 * entry * (1 + _f_r) / (1 - _f_r) - tp1)
                action = (f"✅ K線高點 {format_price(effective_high)} 已達止盈1 {format_price(tp1)}，"
                          f"現價 {format_price(current_price)}｜止損已自動上移至保本點（<code>{format_price(_sl_be)}</code>）")
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
                # TP2 已完成，追蹤止損繼續鎖利剩餘 20%（空頭）
                trail_dist = pos.get('trail_dist', entry * 0.015)
                new_trail_sl = current_price + trail_dist
                if new_trail_sl < sl:
                    pos['sl'] = new_trail_sl
                    sl = new_trail_sl
                status = "🔵 TP2已完成"
                action = f"剩餘20%持倉中，等待TP3 <code>{format_price(tp3)}</code>，追蹤止損 {format_price(sl)}，現價 {format_price(current_price)}"
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
                pos['tp2_hit'] = True
                pos['sl'] = tp1   # SL 立即鎖至 TP1，剩餘倉位零風險
                sl = tp1
                status = "🔵 止盈2達標"
                if not pos.get('tp1_hit'):
                    # K線一根直接衝破 TP1→TP2，補記 TP1 並在訊息中說明（空頭）
                    pos['tp1_hit'] = True
                    pos['trail_dist'] = pos.get('orig_sl', entry + (entry - tp1)) - entry
                    action = (f"✅ K線低點 {format_price(effective_low)} 同時穿越 TP1 <code>{format_price(tp1)}</code> 並達止盈2 <code>{format_price(tp2)}</code>，"
                              f"現價 {format_price(current_price)}\n"
                              f"🔒 TP1 段已自動確認，止損鎖至TP1（<code>{format_price(tp1)}</code>），建議平倉50%")
                else:
                    action = (f"✅ K線低點 {format_price(effective_low)} 已達止盈2 {format_price(tp2)}，"
                              f"現價 {format_price(current_price)}｜<b>建議將止損下移至TP1（<code>{format_price(tp1)}</code>）</b>")
                push = True
        elif effective_low <= tp1 / TP_CONFIRM:
            if pos.get('tp1_hit'):
                # 追蹤止損：TP1 後每次監控都把 SL 往下拉緊鎖利（空頭）
                trail_dist = pos.get('trail_dist', entry * 0.015)
                new_trail_sl = current_price + trail_dist
                if new_trail_sl < sl:
                    pos['sl'] = new_trail_sl
                    sl = new_trail_sl
                # TP1 已在前次通知，本次靜默監控等待 TP2
                status = "🟢 TP1已完成"
                action = f"剩餘50%持倉中，等待TP2 <code>{format_price(tp2)}</code>，追蹤止損 {format_price(sl)}，現價 {format_price(current_price)}"
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
                status = "🟢 止盈1達標"
                # 空頭費後保本點：SL_be = 2×E×(1-f)/(1+f) - TP1
                # TP1 < entry（空頭），SL_be 若 > entry = 需要讓餘倉再跌才真正保本
                _f_r  = 0.0005
                _sl_be_s = min(entry, 2 * entry * (1 - _f_r) / (1 + _f_r) - tp1)
                action = (f"✅ K線低點 {format_price(effective_low)} 已達止盈1 {format_price(tp1)}，"
                          f"現價 {format_price(current_price)}｜止損已自動下移至保本點（<code>{format_price(_sl_be_s)}</code>）")
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
            to_remove.append(pos)
            continue

        status, action, push = analyze_position(pos)
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
            if status in OUTCOME_MAP and pos.get('filled', True):
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
        "🔵 止盈2達標":        ("平倉30%，持有剩餘", "📤"),
        "🟢 止盈1達標":        ("平倉50%，持有剩餘", "📤"),
        "⚠️ 即將觸及保本止損": ("主動出場保本",   "⚠️"),
        "⚠️ 接近止損":         ("考慮現價出場",   "⚠️"),
        "🚨 局勢惡化":         ("建議現價出場",   "🚪"),
        "🟢 TP1已完成":        ("繼續持有等TP2",  "⏳"),
        "🔄 持倉中":           ("繼續持有",       "✅"),
        "⏳ 等待進場":         ("等待限價單成交", "⏳"),
    }

    for asset, asset_alerts in alerts_by_asset.items():
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
            # 掛單取消：action 已說明真正原因，不再重複 REC 文字
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

    msg += "\n"
    msg += f"<i>🕐 分析時間：{now_la.strftime('%m/%d %H:%M')} PT</i>\n"
    msg += f"<i>⏰ 有效至：{expire_note}，超時請重新輸入 /{symbol} 更新</i>\n"

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
        type_badge = {"trend": "📈趨勢", "range": "↔️區間", "divergence": "🔄背離"}.get(sig_type, "")
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
        else:
            ctx_label = "趨勢"
            ctx_extra = item.get('tf_note', '')
        html_message += f"{ctx_label} {adx_bar} <b>{adx_level}</b>  {ctx_extra}  ｜  {sentiment_short}\n"
        # ── 分隔 ──
        html_message += "┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
        # ── 進場 / 止損 / TP（等寬對齊，CJK=2格 ASCII=1格，統一補至6格） ──
        html_message += "<pre>"
        html_message += f"進場  {format_price(item['entry'])}\n"
        html_message += f"止損  {format_price(item['sl'])}\n"
        html_message += f"TP1   {format_price(item['tp1'])}\n"
        html_message += f"TP2   {format_price(item['tp2'])}\n"
        html_message += f"TP3   {format_price(item['tp3'])}\n"
        html_message += "</pre>"
        # ── TP1後止損提示（費後真正保本點，非開倉原點）──
        _f_msg = 0.0005
        _e_msg, _t1_msg = item['entry'], item['tp1']
        if item['dir'] == '多':
            _sl_be_msg = max(_e_msg, 2 * _e_msg * (1 + _f_msg) / (1 - _f_msg) - _t1_msg)
        else:
            _sl_be_msg = min(_e_msg, 2 * _e_msg * (1 - _f_msg) / (1 + _f_msg) - _t1_msg)
        html_message += f"▸ TP1達標 → 止損移至 <code>{format_price(_sl_be_msg)}</code>（含手續費保本）\n"
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

# ==================== 📡 7. 原生無衝突監聽引擎 ====================
def scan_worker_thread(msg_title, target_chat_id, silent_on_empty=False, include_news=False, auto_track=False):
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
                            'signal_price':   sig.get('signal_price', sig['entry']),
                            'signal_type':    sig.get('signal_type', 'trend'),
                            'reported_at':    sent_at,
                            'last_checked_ts': sent_at,
                            'filled':         False,
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
        if silent_on_empty:
            print(f"📭 定時掃描無訊號，靜默略過（{msg_title}）")
        else:
            # 無主訊號 → 跑接近訊號掃描
            print("🔍 無主訊號，掃描接近訊號中...")
            near_misses = run_near_miss_scan()
            if near_misses:
                dir_map = {"多": "🟩多", "空": "🟥空"}
                lines = ["📭 <b>全網掃描完畢，暫無完整訊號</b>\n\n<b>── 接近訊號 Top 3（供參考）──</b>"]
                for nm in near_misses:
                    bar = "■" * nm['filters_passed'] + "□" * (4 - nm['filters_passed'])
                    d = dir_map.get(nm['dir'], nm['dir'])
                    lines.append(
                        f"⚡ <b>{nm['asset']}</b>  {d}  {nm['tf']}  "
                        f"通過 {nm['filters_passed']}/4 關  [{bar}]\n"
                        f"<pre>"
                        f"進場  {nm['entry_label'].split('=')[-1]}\n"
                        f"止損  {format_price(nm['sl'])}\n"
                        f"TP1   {format_price(nm['tp1'])}\n"
                        f"TP2   {format_price(nm['tp2'])}\n"
                        f"TP3   {format_price(nm['tp3'])}"
                        f"</pre>"
                        f"⚠️ 卡在：{nm['failed_at']}　ADX={nm['adx']}　RSI={nm['rsi']}"
                    )
                lines.append("\n<i>接近訊號未通過所有過濾條件，請自行評估進場風險。</i>")
                text = "\n\n".join(lines)
                # 快取接近訊號供按鈕回調查詢
                with near_miss_lock:
                    near_miss_cache.clear()
                    for nm in near_misses:
                        near_miss_cache[nm['asset']] = {**nm, 'cached_at': time.time()}
                # 每個接近訊號一個追蹤按鈕（標注「自選」提示風險）
                buttons = [
                    [{"text": f"👁 自選追蹤 {nm['asset']}{nm['dir']} ({nm['filters_passed']}/4)",
                      "callback_data": f"open_{nm['asset']}_{nm['dir']}"}]
                    for nm in near_misses
                ]
                reply_markup = {"inline_keyboard": buttons}
            else:
                text = "📭 全網通掃完畢，當前盤面極其冷靜，暫無達到技術篩選門檻的訊號（趨勢/區間/背離均未通過）。"
                reply_markup = None
            text_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = {"chat_id": str(target_chat_id), "text": text, "parse_mode": "HTML"}
            if reply_markup:
                payload["reply_markup"] = reply_markup
            resp = requests.post(text_url, json=payload)
            if resp.json().get("ok"):
                print(f"✅ 掃描結果發送成功（{'有' if near_misses else '無'}接近訊號）")

def send_holding_summary(chat_id):
    """發送目前所有活躍持倉的狀態總覽"""
    with active_positions_lock:
        positions = list(active_positions)

    text_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    if not positions:
        requests.post(text_url, json={"chat_id": chat_id, "text": "📭 目前沒有追蹤中的持倉。請先執行 /scan 產生訊號。"})
        return

    la_tz = pytz.timezone('America/Los_Angeles')
    now_str = datetime.datetime.now(la_tz).strftime('%Y-%m-%d %H:%M')

    # ── 先清除已取消 / 已結束的掛單，並推送即時通知 ──
    _TERMINAL = {"🚫 掛單已取消", "⏰ 掛單逾時取消",
                 "🔴 止損觸發", "🟣 全部止盈", "🛡️ 回調至保本止損"}
    to_remove   = []
    active_rows = []   # (pos, status, action) 真正需要顯示的
    for pos in positions:
        status, action, _ = analyze_position(pos)
        if status is None:
            status, action = "❓ 無法取得現價", "—"
        if status in _TERMINAL:
            to_remove.append(pos)
            # 與 run_position_monitor 相同邏輯：記錄交易結果
            _OUTCOME_MAP = {
                "🟣 全部止盈":        "win",
                "🛡️ 回調至保本止損":  "win",
                "🔴 止損觸發":        "loss",
            }
            if status in _OUTCOME_MAP and pos.get('filled', False):
                record_trade_outcome(pos, _OUTCOME_MAP[status])
            d_tag = "🟩多" if pos['dir'] == "多" else "🟥空"
            icon  = "🚫" if "取消" in status else ("🔴" if "止損" in status else "🟣")
            note  = (
                f"{icon} <b>{status}</b>  <code>{now_str} PT</code>\n\n"
                f"<b>{pos['asset']}</b>  {d_tag}  {pos['tf']}\n"
                f"▸ {action}\n\n"
                f"<i>（已自動從監控清單移除）</i>"
            )
            requests.post(text_url, json={"chat_id": str(TELEGRAM_CHAT_ID),
                                          "text": note, "parse_mode": "HTML"})
        else:
            active_rows.append((pos, status, action))

    with active_positions_lock:
        for p in to_remove:
            if p in active_positions:
                active_positions.remove(p)
        save_positions(active_positions)  # 無論有無移除都存，確保 tp1_hit/sl 等狀態變更不因重啟而遺失

    if not active_rows:
        requests.post(text_url, json={"chat_id": chat_id,
            "text": "📭 目前沒有持倉中的追蹤記錄。"})
        return

    # ── 只顯示真正持倉中的幣種 ──
    msg = f"<b>【持倉監控總覽】</b>  <code>{now_str} PT</code>\n"
    msg += f"共追蹤 <b>{len(active_rows)}</b> 筆持倉\n"
    msg += "─────────────────────────\n"

    by_asset = defaultdict(list)
    for pos, status, action in active_rows:
        by_asset[pos['asset']].append((pos, status, action))

    for asset, group in by_asset.items():
        inst_id = asset + '-USDT-SWAP'
        cp = get_current_price(inst_id)
        price_str = format_price(cp) if cp else "—"
        msg += f"<b>{asset}</b>  現價 <code>{price_str}</code>\n"

        for i, (pos, status, action) in enumerate(group, 1):
            age_h = (time.time() - pos['reported_at']) / 3600
            d     = "🟩<b>多</b>" if pos['dir'] == "多" else "🟥<b>空</b>"
            sub   = f"#{i} " if len(group) > 1 else ""
            msg += f"{sub}{d}  {pos['tf']}  {status}  <i>({age_h:.1f}h前)</i>\n"
            t1 = "✅" if pos.get('tp1_hit') else ""
            t2 = "✅" if pos.get('tp2_hit') else ""
            t3 = "✅" if pos.get('tp3_hit') else ""
            msg += "<pre>"
            msg += f"進場  {format_price(pos['entry'])}\n"
            msg += f"止損  {format_price(pos['sl'])}\n"
            msg += f"TP1   {format_price(pos['tp1'])} {t1}\n"
            msg += f"TP2   {format_price(pos['tp2'])} {t2}\n"
            msg += f"TP3   {format_price(pos['tp3'])} {t3}\n"
            msg += "</pre>"
            msg += f"▸ {action}\n"

        all_pos_in_asset = [r[0] for r in group]
        conclusion = build_coin_conclusion(all_pos_in_asset, cp)
        if conclusion:
            msg += f"{conclusion}\n"
        msg += "─────────────────────────\n"

    tracked = list(by_asset.keys())
    _eg = tracked[0] if tracked else "ETH"
    msg += f"<i>📌 /open {_eg}  確認開倉並加入監控\n🗑️ /close {_eg}  平倉後解除監控\n⚡ 警報僅在 TP/SL/市場惡化時推送</i>"
    # C. 快捷按鈕
    holding_markup = {"inline_keyboard": [[
        {"text": "🔄 重新掃描", "callback_data": "cmd_scan"},
        {"text": "📊 查看勝率", "callback_data": "cmd_stats"}
    ]]}
    text_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    requests.post(text_url, json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML", "reply_markup": holding_markup})

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

    new_pos = {
        'asset':           symbol,
        'dir':             direction,
        'tf':              tf_label,
        'entry':           entry,
        'sl':              sl,
        'orig_sl':         sl,    # 原始止損，供 trail_dist 計算使用（不隨追蹤止損改變）
        'tp1':             tp1,
        'tp2':             tp2,
        'tp3':             tp3,
        'signal_price':    best.get('price', entry),  # 訊號產生時的市價，用於填單方向判斷
        'reported_at':     now_ts,
        'last_checked_ts': now_ts,
        'filled':          False,
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

        # 各幣種細分
        asset_wins   = Counter(r['asset'] for r in wins)
        asset_losses = Counter(r['asset'] for r in losses)
        all_assets   = sorted(set(r['asset'] for r in valid))
        if all_assets:
            msg += "<b>各幣種：</b>\n"
            for a in all_assets:
                w = asset_wins.get(a, 0)
                l = asset_losses.get(a, 0)
                t = w + l
                pct = w / t * 100 if t else 0
                msg += f"  {a}：{w}勝 {l}敗  ({pct:.0f}%)\n"

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
                                        new_pos_cb = {
                                            'asset': sig_cb['asset'], 'dir': sig_cb['dir'],
                                            'tf': sig_cb['tf'], 'entry': _raw_entry,
                                            'sl': sig_cb['sl'],
                                            'orig_sl': sig_cb['sl'],  # 原始止損，trail_dist 基準
                                            'tp1': sig_cb['tp1'],
                                            'tp2': sig_cb['tp2'], 'tp3': sig_cb['tp3'],
                                            'signal_price':   _sp,
                                            'signal_type':    _sig_type,
                                            'reported_at': now_ts_cb,
                                            'last_checked_ts': now_ts_cb,
                                            'filled': False,
                                            'entry_fr': sig_cb.get('entry_fr'),  # 進場時資金費率
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
                            t = threading.Thread(target=scan_worker_thread, args=("手動現場突擊播報", cb_chat))
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
                                "📊 <b>統計</b>\n"
                                "/stats — 查看近 30 天真實交易勝率統計\n"
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
                            t = threading.Thread(target=scan_worker_thread, args=("手動現場突擊播報", chat_id))
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

                        elif text.lower().startswith("/resetstats"):
                            print(f"🗑️ 收到 /resetstats 指令，清空勝率紀錄")
                            save_stats([])
                            requests.post(
                                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                                json={"chat_id": chat_id,
                                      "text": "🗑️ <b>勝率紀錄已清空</b>\n\n從現在起依照最新策略重新統計。\n歷史資料已全部移除，後續的 TP/SL 結果將從零開始累積。",
                                      "parse_mode": "HTML"}
                            )

                        elif text.startswith("/") and len(text) > 1:
                            # 通用幣種查詢：/eth /btc /sol /doge 等
                            coin_cmd = text.split()[0].lstrip("/").split("@")[0]
                            if coin_cmd and coin_cmd.isalpha() and coin_cmd.lower() not in ("open","close","scan","holding","watch","unwatch","watching","stats","resetstats","help"):
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
