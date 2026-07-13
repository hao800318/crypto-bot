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

# ==================== 🔑 1. Telegram 設定 ====================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
SCHEDULE_HOURS = [8, 12, 18, 22]

BASE_URL = "https://www.okx.com"

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
    """
    funding_rate = 0.0
    ls_ratio = 1.0

    try:
        fr_url = f"{BASE_URL}/api/v5/public/funding-rate?instId={asset}"
        fr_res = requests.get(fr_url, timeout=2.0).json()
        if fr_res.get('code') == '0' and fr_res.get('data'):
            funding_rate = float(fr_res['data'][0]['fundingRate'])
    except:
        pass

    try:
        ls_url = f"{BASE_URL}/api/v5/rubik/stat/contracts/long-short-account-ratio?instId={asset}&period=5m"
        ls_res = requests.get(ls_url, timeout=2.0).json()
        if ls_res.get('code') == '0' and ls_res.get('data'):
            ls_ratio = float(ls_res['data'][0][1])
    except:
        pass

    return funding_rate, ls_ratio

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
    """BTC 1H MA8 vs EMA89，判斷大盤方向"""
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

def get_candle_range_since(inst_id, since_ts, bar="1H"):
    """取得 since_ts 以來所有 K 線的最高價和最低價。
    用於偵測監控間隔中曾觸碰的價格極值，防止漏掉 TP/SL/填單事件。
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
            # 包含 since_ts 之前 1 根 K 線（防止邊界漏算）
            margin = 3600 if bar == "1H" else 14400
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

    # ── 1. BTC 方向是否逆向 ──
    try:
        btc_trend = get_btc_trend()
        if direction == "多" and btc_trend == "bear":
            warnings.append("BTC 轉空頭，對多單形成壓制")
        elif direction == "空" and btc_trend == "bull":
            warnings.append("BTC 轉多頭，對空單形成頂托")
    except:
        pass

    # ── 2. ADX 跌破 20（持倉期間趨勢消失）──
    try:
        bar = "1H" if "1H" in tf else "4H"
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
    """檢查更高時框 MA8 vs EMA89 是否與訊號方向對齊
       higher_bar: '4H'（給1H訊號用）或 '1D'（給4H訊號用）
    """
    try:
        limit = 100 if higher_bar != "1D" else 100
        url = f"{BASE_URL}/api/v5/market/candles?instId={asset}&bar={higher_bar}&limit={limit}"
        res = requests.get(url, timeout=3.0).json()
        if res.get('code') == '0' and len(res['data']) >= 30:
            df = pd.DataFrame(res['data'], columns=['ts','open','high','low','close','vol','volCcy','volCcyQuote','state'])
            df['close'] = df['close'].astype(float)
            df = df.iloc[::-1].reset_index(drop=True)
            df['MA8']   = df['close'].rolling(8).mean()
            df['EMA89'] = df['close'].ewm(span=89, adjust=False).mean()
            last = df.iloc[-1]
            aligned = (direction == "多" and last['MA8'] > last['EMA89']) or \
                      (direction == "空" and last['MA8'] < last['EMA89'])
            return aligned
    except:
        pass
    return True  # 無法取得時不懲罰

# ==================== ⚙️ 5. 同步 K 線與勝率量化評分核心 ====================
def score_to_win_rate(score):
    """將內部評分（0-130）映射為 50%–98% 勝率顯示"""
    return min(98, max(50, int(50 + (score / 130) * 48)))

def score_to_leverage(win_rate, max_leverage):
    """依勝率取最大槓桿的比例，結果不超過交易所上限"""
    if win_rate >= 90:
        ratio = 1.00
    elif win_rate >= 85:
        ratio = 0.80
    elif win_rate >= 78:
        ratio = 0.60
    elif win_rate >= 68:
        ratio = 0.40
    else:
        ratio = 0.20
    lev = max(1, round(max_leverage * ratio))
    return f"{lev}x"

def fetch_candle_sync(asset, tf, max_leverage=20, btc_trend="neutral", market_fr=0.0):
    bar_param = "1H" if tf == "1h" else "4H"
    url = f"{BASE_URL}/api/v5/market/candles?instId={asset}&bar={bar_param}&limit=100"
    try:
        res = requests.get(url, timeout=2.0).json()
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

            p_last = df.iloc[-2]
            c_last = df.iloc[-1]

            is_cross_up   = (p_last['MA8'] <= p_last['EMA89']) and (c_last['MA8'] > c_last['EMA89'])
            is_cross_down = (p_last['MA8'] >= p_last['EMA89']) and (c_last['MA8'] < c_last['EMA89'])

            if not (is_cross_up or is_cross_down):
                return None

            direction     = "多" if is_cross_up else "空"
            current_price = c_last['close']
            current_rsi   = c_last['RSI']
            current_ema89 = c_last['EMA89']
            current_ma8   = c_last['MA8']
            current_adx   = c_last['ADX']
            current_atr   = c_last['ATR14']

            # ① ADX 過濾：< 20 表示盤整市場，MA 交叉失誤率高
            if current_adx < 20:
                return None

            # ② 成交量確認：交叉K棒量 > 過去5根均量 × 1.2
            avg_vol_5 = df['vol'].iloc[-7:-2].mean()
            cross_vol = df['vol'].iloc[-2]
            volume_confirmed = cross_vol > avg_vol_5 * 1.2

            # ── RSI 勝率基礎評分 ──
            base_score = (100 - abs(current_rsi - 56)) if direction == "多" else (100 - abs(current_rsi - 44))

            # ── 主力動向確認 ──
            funding_rate, ls_ratio = get_market_sentiment(asset)
            sentiment_note, sentiment_bonus = build_sentiment_note(direction, funding_rate, ls_ratio)

            # ③ 成交量加分
            vol_bonus = 8 if volume_confirmed else -10

            # ④ 多時框對齊（1H看4H，4H看1D）
            tf_bonus = 0
            tf_note  = ""
            if tf == "1h":
                aligned = get_higher_tf_alignment(asset, direction, higher_bar="4H")
                if aligned:
                    tf_bonus = 10
                    tf_note  = " ✅4H對齊"
                else:
                    tf_bonus = -20
                    tf_note  = " ⚠️4H逆向"
            else:  # 4h
                aligned = get_higher_tf_alignment(asset, direction, higher_bar="1D")
                if aligned:
                    tf_bonus = 10
                    tf_note  = " ✅1D對齊"
                else:
                    tf_bonus = -20
                    tf_note  = " ⚠️1D逆向"

            # ⑤ BTC 方向過濾
            btc_bonus = 0
            if btc_trend == "bear" and direction == "多":
                btc_bonus = -15
            elif btc_trend == "bull" and direction == "空":
                btc_bonus = -15

            # ⑥ 全市場資金費率過熱
            fr_bonus = 0
            if market_fr > 0.0005 and direction == "多":
                fr_bonus = -10   # 多頭過熱
            elif market_fr < -0.0003 and direction == "空":
                fr_bonus = -10   # 空頭過熱

            score = base_score + sentiment_bonus + vol_bonus + tf_bonus + btc_bonus + fr_bonus

            win_rate = score_to_win_rate(score)
            leverage = score_to_leverage(win_rate, max_leverage)

            order_type = "短線單" if tf == "1h" else "長線單"
            tf_tag = f"1H短線{tf_note}" if tf == "1h" else "4H長線"

            # ── 錨定進場點 ──
            if tf == "1h":
                anchor_entry = current_ma8
                anchor_label = f"MA8={format_price(current_ma8)}"
                atr_mult = 1.5
                tp_ratios_long  = (1.015, 1.030, 1.050)
                tp_ratios_short = (0.985, 0.970, 0.950)
            else:
                anchor_entry = current_ema89
                anchor_label = f"EMA89={format_price(current_ema89)}"
                atr_mult = 2.0
                tp_ratios_long  = (1.030, 1.060, 1.100)
                tp_ratios_short = (0.970, 0.940, 0.900)

            entry_price = anchor_entry

            # ⑦ ATR 動態止損
            atr_sl_dist = current_atr * atr_mult
            if direction == "多":
                sl_price = max(entry_price - atr_sl_dist, entry_price * 0.96)  # 最大止損 4%
                if current_rsi > 65:
                    entry_type = f"⚠️ {tf_tag}RSI過熱({current_rsi:.1f})，掛限價等回踩 {anchor_label}"
                else:
                    entry_type = f"📌 {tf_tag}掛限價 {anchor_label}，ATR止損={format_price(sl_price)}"
                tp1 = entry_price * tp_ratios_long[0]
                tp2 = entry_price * tp_ratios_long[1]
                tp3 = entry_price * tp_ratios_long[2]
            else:
                sl_price = min(entry_price + atr_sl_dist, entry_price * 1.04)  # 最大止損 4%
                if current_rsi < 35:
                    entry_type = f"⚠️ {tf_tag}RSI超賣({current_rsi:.1f})，掛限價等反彈至 {anchor_label}"
                else:
                    entry_type = f"📌 {tf_tag}掛限價 {anchor_label}，ATR止損={format_price(sl_price)}"
                tp1 = entry_price * tp_ratios_short[0]
                tp2 = entry_price * tp_ratios_short[1]
                tp3 = entry_price * tp_ratios_short[2]

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
                "adx":            round(current_adx, 1),
                "vol_confirmed":  volume_confirmed,
            }
    except:
        pass
    return None

def run_strategy_scan():
    all_assets, leverage_map = get_all_okx_swap_assets()

    # 市場環境指標（只取一次，傳給所有 worker）
    print("📡 獲取市場環境指標...")
    btc_trend = get_btc_trend()
    market_fr = get_market_avg_funding_rate()
    print(f"   BTC趨勢: {btc_trend} | 市場費率均值: {market_fr*100:.4f}%")

    all_signals = []
    tasks = [(asset, tf) for asset in all_assets for tf in ["1h", "4h"]]
    total = len(tasks)
    completed = 0
    lock = threading.Lock()

    scan_start = time.time()
    print(f"⏱️ 掃描開始時間：{datetime.datetime.now().strftime('%H:%M:%S')}（並行模式，共 {total} 項任務）")

    def scan_task(asset, tf):
        max_lev = leverage_map.get(asset, 20)
        return fetch_candle_sync(asset, tf, max_leverage=max_lev,
                                 btc_trend=btc_trend, market_fr=market_fr)

    with ThreadPoolExecutor(max_workers=25) as executor:
        futures = {executor.submit(scan_task, asset, tf): (asset, tf) for asset, tf in tasks}
        for future in as_completed(futures):
            with lock:
                completed += 1
                if completed % 50 == 0 or completed == total:
                    print(f"\r🔍 並行掃描進度：[{completed}/{total}]...", end="", flush=True)
            try:
                res = future.result()
                if res:
                    with lock:
                        all_signals.append(res)
            except Exception:
                pass

    elapsed = time.time() - scan_start
    print(f"\n✨ 全網掃描完畢！耗時：{elapsed:.1f} 秒（共 {len(all_assets)} 支幣種 × 2 時框）")

    all_signals.sort(key=lambda x: x['score'], reverse=True)
    top_5_signals = all_signals[:5]

    print(f"📊 掃描結果：共找到 {len(all_signals)} 組信號，精選前 {len(top_5_signals)} 名")
    return top_5_signals

# ==================== 📊 5. 持倉監控系統 ====================
POSITIONS_FILE = "active_positions.json"

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

    # ── 取得上次監控後的 K 線高低點（防止 TP/SL/填單事件在監控間隔中被漏掉）──
    since_ts = pos.get('last_checked_ts', pos.get('reported_at', time.time() - 3600))
    bar      = "1H" if "1H" in pos.get('tf', '1H') else "4H"
    rng_high, rng_low = get_candle_range_since(inst_id, since_ts, bar)
    # 回退到現價作為保底
    effective_high = max(rng_high, current_price) if rng_high else current_price
    effective_low  = min(rng_low,  current_price) if rng_low  else current_price
    # 更新本次監控時間戳
    pos['last_checked_ts'] = time.time()

    # ── 先確認限價單是否已被成交 ──
    # 用 K 線高低點判斷（比只看現價更準確，防止進場點在兩次監控間被觸碰後反彈）
    if not pos.get('filled', False):
        filled_by_candle = (
            (dir == "多" and effective_low  <= entry) or
            (dir == "空" and effective_high >= entry)
        )
        if filled_by_candle:
            pos['filled'] = True
            how = "K線低點" if dir == "多" else "K線高點"
            print(f"✅ {pos['asset']} {dir}單已由{how}觸碰進場點 {format_price(entry)}，開始監控 TP/SL")
        else:
            # 限價單尚未成交，僅顯示等待狀態，不觸發任何警報
            gap_pct = abs(current_price - entry) / entry * 100
            if dir == "多":
                note = f"⏳ 掛單等待中，現價 {format_price(current_price)}，距進場點還差 {gap_pct:.2f}%↓"
            else:
                note = f"⏳ 掛單等待中，現價 {format_price(current_price)}，距進場點還差 {gap_pct:.2f}%↑"
            return "⏳ 等待進場", note, False

    oi_change  = get_open_interest_change(inst_id)
    vol_spike  = get_volume_spike(inst_id)
    fr, ls_ratio = get_market_sentiment(inst_id)

    # ── 主力異常信號判斷 ──
    whale_warn = ""
    if vol_spike >= 2.5:
        whale_warn = f"⚡ 異常量能！成交量是均量 {vol_spike:.1f} 倍"
    if oi_change < -5:
        whale_warn += f"\n📉 OI 下降 {oi_change:.1f}%，主力正在撤退"
    elif oi_change > 5:
        whale_warn += f"\n📈 OI 上升 {oi_change:.1f}%，主力持續加倉"

    # ── 持倉狀態判定（用 K 線高低點而非現價，防止監控間隔中的事件被漏掉）──
    if dir == "多":
        dist_to_sl_pct = (current_price - sl) / entry * 100
        # 止損：用 K 線低點（只要低點碰過 SL 就算觸發）
        if effective_low <= sl:
            status = "🔴 止損觸發"
            action = (f"⛔ K線低點 {format_price(effective_low)} 已觸及止損位 {format_price(sl)}，"
                      f"現價 {format_price(current_price)}｜<b>建議立即平倉</b>")
            push = True
        # 止盈：用 K 線高點（只要高點碰過 TP 就算達標）
        elif effective_high >= tp3:
            status = "🟣 全部止盈"
            action = (f"🎯 K線高點 {format_price(effective_high)} 已達止盈3 {format_price(tp3)}，"
                      f"現價 {format_price(current_price)}｜<b>建議全數平倉</b>")
            push = True
        elif effective_high >= tp2:
            status = "🔵 止盈2達標"
            action = (f"✅ K線高點 {format_price(effective_high)} 已達止盈2 {format_price(tp2)}，"
                      f"現價 {format_price(current_price)}｜<b>建議再平倉30%</b>，止損上移至止盈1（{format_price(tp1)}）")
            push = True
        elif effective_high >= tp1:
            status = "🟢 止盈1達標"
            action = (f"✅ K線高點 {format_price(effective_high)} 已達止盈1 {format_price(tp1)}，"
                      f"現價 {format_price(current_price)}｜<b>建議平倉50%</b>，止損上移至成本（{format_price(entry)}）")
            push = True
        elif dist_to_sl_pct < 0.5:
            status = "⚠️ 接近止損"
            action = f"⚠️ 距止損僅 {dist_to_sl_pct:.2f}%，現價 {format_price(current_price)}｜<b>建議收緊止損或現價平倉</b>"
            push = True
        else:
            status = "🔄 持倉中"
            action = f"持倉正常，現價 {format_price(current_price)}，距止損 {dist_to_sl_pct:.1f}%"
            deteri = check_market_deterioration(inst_id, dir, pos.get('tf','1H'))
            if deteri:
                status = "🚨 局勢惡化"
                action = f"持倉正常，現價 {format_price(current_price)}，距止損 {dist_to_sl_pct:.1f}%\n{deteri}"
                push = True
            else:
                push = bool(whale_warn)
    else:  # 空
        dist_to_sl_pct = (sl - current_price) / entry * 100
        # 止損：用 K 線高點
        if effective_high >= sl:
            status = "🔴 止損觸發"
            action = (f"⛔ K線高點 {format_price(effective_high)} 已觸及止損位 {format_price(sl)}，"
                      f"現價 {format_price(current_price)}｜<b>建議立即平倉</b>")
            push = True
        # 止盈：用 K 線低點
        elif effective_low <= tp3:
            status = "🟣 全部止盈"
            action = (f"🎯 K線低點 {format_price(effective_low)} 已達止盈3 {format_price(tp3)}，"
                      f"現價 {format_price(current_price)}｜<b>建議全數平倉</b>")
            push = True
        elif effective_low <= tp2:
            status = "🔵 止盈2達標"
            action = (f"✅ K線低點 {format_price(effective_low)} 已達止盈2 {format_price(tp2)}，"
                      f"現價 {format_price(current_price)}｜<b>建議再平倉30%</b>，止損下移至止盈1（{format_price(tp1)}）")
            push = True
        elif effective_low <= tp1:
            status = "🟢 止盈1達標"
            action = (f"✅ K線低點 {format_price(effective_low)} 已達止盈1 {format_price(tp1)}，"
                      f"現價 {format_price(current_price)}｜<b>建議平倉50%</b>，止損下移至成本（{format_price(entry)}）")
            push = True
        elif dist_to_sl_pct < 0.5:
            status = "⚠️ 接近止損"
            action = f"⚠️ 距止損僅 {dist_to_sl_pct:.2f}%，現價 {format_price(current_price)}｜<b>建議收緊止損或現價平倉</b>"
            push = True
        else:
            status = "🔄 持倉中"
            action = f"持倉正常，現價 {format_price(current_price)}，距止損 {dist_to_sl_pct:.1f}%"
            deteri = check_market_deterioration(inst_id, dir, pos.get('tf','4H'))
            if deteri:
                status = "🚨 局勢惡化"
                action = f"持倉正常，現價 {format_price(current_price)}，距止損 {dist_to_sl_pct:.1f}%\n{deteri}"
                push = True
            else:
                push = bool(whale_warn)

    full_action = action
    if whale_warn:
        full_action += f"\n{whale_warn}"

    # 加入 OI/多空比 狀態補充
    full_action += f"\n📊 主力資料：OI變化{oi_change:+.1f}% | 多空比{ls_ratio:.2f} | 費率{fr*100:.4f}%"

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
    to_remove = []

    for pos in positions:
        age_hours = (time.time() - pos['reported_at']) / 3600

        # 未成交掛單過期（1H訊號4小時、4H訊號12小時）
        if not pos.get('filled', False):
            expiry_h = 4 if '1H' in pos.get('tf', '1H') else 12
            if age_hours > expiry_h:
                inst_id = pos['asset'] + '-USDT-SWAP'
                current_price = get_current_price(inst_id)
                price_str = format_price(current_price) if current_price else "無法取得"
                gap = ""
                if current_price:
                    gap_pct = abs(current_price - pos['entry']) / pos['entry'] * 100
                    arrow = "↑" if (pos['dir'] == "空") else "↓"
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
            continue  # 未成交的單不做 SL/TP 監控

        # 已成交且超過 24 小時 → 發出最後警告再移除
        if age_hours > 24:
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
            alerts.append((pos, status, action))

        # 止損觸發或全部止盈 → 移除追蹤
        if status in ("🔴 止損觸發", "🟣 全部止盈"):
            to_remove.append(pos)

    # 清理過期 / 已結束的持倉
    with active_positions_lock:
        for p in to_remove:
            if p in active_positions:
                active_positions.remove(p)
        if to_remove:
            save_positions(active_positions)

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

    msg = f"🔔 <b>【持倉監控警報 - {now_str} PT】</b>\n"
    msg += f"📋 <b>監控 {len(positions)} 筆持倉，{len(alerts_by_asset)} 幣種需注意：</b>\n"
    msg += "───────────────────────\n\n"

    for asset, asset_alerts in alerts_by_asset.items():
        inst_id = asset + '-USDT-SWAP'
        cp = get_current_price(inst_id)
        price_str = format_price(cp) if cp else "無法取得"
        msg += f"<b>📍 {asset}</b>  現價：<code>{price_str}</code>\n"

        for i, (pos, status, action) in enumerate(asset_alerts, 1):
            d   = "🟢 <b>做多</b>" if pos['dir'] == "多" else "🔴 <b>做空</b>"
            sub = f"【第{i}筆】 " if len(asset_alerts) > 1 else ""
            msg += f"  {sub}{d}  <b>{pos['tf']}</b>  {status}\n"
            msg += f"   進場：<code>{format_price(pos['entry'])}</code>  止損：<code>{format_price(pos['sl'])}</code>\n"
            msg += f"   {action}\n"

        # 綜合結論（基於此幣種全部持倉，不限於有警報的）
        conclusion = build_coin_conclusion(all_by_asset[asset], cp)
        if conclusion:
            msg += f"   {conclusion}\n"
        msg += "\n"

    msg += "───────────────────────\n⚠️ <i>以上為自動監控建議，請結合自身判斷操作。</i>"

    text_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    resp = requests.post(text_url, json={"chat_id": str(TELEGRAM_CHAT_ID), "text": msg, "parse_mode": "HTML"})
    if resp.json().get("ok"):
        print(f"✅ 持倉監控警報已發送（{len(alerts)} 筆）")
    else:
        print(f"❌ 持倉監控警報發送失敗：{resp.json()}")

# ==================== 🚀 6. 動態精度渲染發送引擎 ====================
def format_price(p):
    if p == 0: return "0.00"
    if p >= 1: return f"{p:,.2f}" if p >= 100 else f"{p:,.4f}"
    num_zeros = math.floor(-math.log10(abs(p)))
    precision = num_zeros + 4
    return f"{p:.{precision}f}"

def send_html_report_via_requests(valid_signals, mode_title="實時雷達速報", target_chat_id=None):
    if target_chat_id is None:
        target_chat_id = TELEGRAM_CHAT_ID

    la_tz = pytz.timezone('America/Los_Angeles')
    now_str = datetime.datetime.now(la_tz).strftime('%Y-%m-%d %H:%M')

    html_message = f"<b>【{mode_title}】</b>  <code>{now_str} PT</code>\n"
    html_message += "─────────────────────────\n"

    for idx, item in enumerate(valid_signals, 1):
        win_rate = item.get('win_rate', 70)
        if win_rate >= 88:   stars = "⭐⭐⭐⭐⭐"
        elif win_rate >= 82: stars = "⭐⭐⭐⭐"
        elif win_rate >= 75: stars = "⭐⭐⭐"
        elif win_rate >= 65: stars = "⭐⭐"
        else:                stars = "⭐"

        adx_val = item.get('adx', 0)
        if adx_val >= 50:   adx_level, adx_bar = "強", "▰▰▰▰"
        elif adx_val >= 30: adx_level, adx_bar = "中", "▰▰▰▱"
        elif adx_val >= 20: adx_level, adx_bar = "低", "▰▰▱▱"
        else:               adx_level, adx_bar = "弱", "▰▱▱▱"
        medal   = ["🥇","🥈","🥉","#4","#5"][idx-1]

        # 方向：用顏色+文字，不堆疊其他圖示
        dir_display = "🟢 <b>多</b>" if item['dir'] == "多" else "🔴 <b>空</b>"

        # 主力動向精簡（去掉長串說明，只保留關鍵數字）
        sentiment = item.get('sentiment_note','')
        fr_match  = next((p for p in sentiment.split('，') if '費率' in p), '')
        ls_match  = next((p for p in sentiment.split('，') if '多空比' in p), '')
        sentiment_short = f"{fr_match} {ls_match}".strip('，').strip()

        # ── 標題行 ──
        html_message += (f"{medal} <b>{item['asset']}</b>  {dir_display}  "
                         f"<b>{item['leverage']}</b>  {item['tf']}  "
                         f"<b>{win_rate}%</b> {stars}\n")
        # ── 趨勢 + 主力 ──
        html_message += f"趨勢 {adx_bar} <b>{adx_level}</b>  |  {sentiment_short}\n"
        # ── 進場 / 止損 ──
        html_message += (f"進場 <code>{format_price(item['entry'])}</code>   "
                         f"止損 <code>{format_price(item['sl'])}</code>\n")
        # ── TP ──
        html_message += f"TP1 <code>{format_price(item['tp1'])}</code>\n"
        html_message += f"TP2 <code>{format_price(item['tp2'])}</code>\n"
        html_message += f"TP3 <code>{format_price(item['tp3'])}</code>\n"
        # ── TP1後止損提示 ──
        html_message += f"▸ TP1達標後，止損移至開倉位 <code>{format_price(item['entry'])}</code>\n"
        html_message += "─────────────────────────\n"

    html_message += "<i>勝率 = RSI＋成交量＋多時框＋主力動向＋BTC方向  |  槓桿僅供參考</i>"

    text_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    resp = requests.post(text_url, json={"chat_id": str(target_chat_id), "text": html_message, "parse_mode": "HTML"})
    result = resp.json()
    if result.get("ok"):
        print(f"✅ 精選 5 強報告發送成功 → chat_id={target_chat_id}")
    else:
        print(f"❌ 報告發送失敗：{result}")

# ==================== 📡 7. 原生無衝突監聽引擎 ====================
def scan_worker_thread(msg_title, target_chat_id):
    valid_signals = run_strategy_scan()
    if valid_signals:
        send_html_report_via_requests(valid_signals, mode_title=msg_title, target_chat_id=target_chat_id)
        # 儲存已播報的訊號進持倉監控清單
        now_ts = time.time()
        with active_positions_lock:
            for sig in valid_signals:
                # 完全相同（幣種+方向+進場+止損+止盈點位）→ 視為同一筆，跳過
                def same_price(a, b):
                    return abs(a - b) / max(abs(b), 1e-9) < 0.0001  # 容差 0.01%
                exists = any(
                    p['asset'] == sig['asset'] and
                    p['dir']   == sig['dir']   and
                    same_price(p['entry'], sig['entry']) and
                    same_price(p['sl'],    sig['sl'])    and
                    same_price(p['tp1'],   sig['tp1'])
                    for p in active_positions
                )
                if not exists:
                    active_positions.append({
                        'asset':            sig['asset'],
                        'dir':              sig['dir'],
                        'tf':               sig['tf'],
                        'entry':            sig['entry'],
                        'sl':               sig['sl'],
                        'tp1':              sig['tp1'],
                        'tp2':              sig['tp2'],
                        'tp3':              sig['tp3'],
                        'reported_at':      now_ts,
                        'last_checked_ts':  now_ts,   # 用於 K 線高低點回顧
                        'filled':           False,    # 限價單尚未成交
                    })
        save_positions(active_positions)   # 新增後立即寫入磁碟
        print(f"📌 已加入持倉監控，目前追蹤 {len(active_positions)} 筆")
    else:
        text_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        resp = requests.post(text_url, json={"chat_id": str(target_chat_id), "text": "📭 全網通掃完畢，當前盤面極其冷靜，暫無符合勝率條件之信號。"})
        if resp.json().get("ok"):
            print(f"✅ 「盤面冷靜」訊息發送成功")

def send_holding_summary(chat_id):
    """發送目前所有活躍持倉的狀態總覽"""
    with active_positions_lock:
        positions = list(active_positions)

    if not positions:
        text_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(text_url, json={"chat_id": chat_id, "text": "📭 目前沒有追蹤中的持倉。請先執行 /scan 產生訊號。"})
        return

    la_tz = pytz.timezone('America/Los_Angeles')
    now_str = datetime.datetime.now(la_tz).strftime('%Y-%m-%d %H:%M')
    msg = f"📋 <b>【持倉監控總覽 - {now_str} PT】</b>\n"
    msg += f"共追蹤 <b>{len(positions)}</b> 筆持倉：\n"
    msg += "───────────────────────\n\n"

    # 按幣種分組顯示
    by_asset = defaultdict(list)
    for pos in positions:
        by_asset[pos['asset']].append(pos)

    for asset, group in by_asset.items():
        inst_id = asset + '-USDT-SWAP'
        cp = get_current_price(inst_id)
        price_str = format_price(cp) if cp else "無法取得"
        msg += f"<b>📍 {asset}</b>  現價：<code>{price_str}</code>\n"

        for i, pos in enumerate(group, 1):
            age_h  = (time.time() - pos['reported_at']) / 3600
            status, action, _ = analyze_position(pos)
            if status is None:
                status, action = "❓ 無法取得", "無法取得現價"
            d   = "🟢 <b>做多</b>" if pos['dir'] == "多" else "🔴 <b>做空</b>"
            sub = f"【第{i}筆】 " if len(group) > 1 else ""
            msg += f"  {sub}{d}  <b>{pos['tf']}</b>  {status}  <i>({age_h:.1f}h前)</i>\n"
            msg += f"   {action}\n"

        conclusion = build_coin_conclusion(group, cp)
        if conclusion:
            msg += f"   {conclusion}\n"
        msg += "\n"

    msg += "───────────────────────\n💡 <i>持倉監控每小時自動推送警報</i>"
    text_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    requests.post(text_url, json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"})

def handle_telegram_updates():
    print("🤖 幣圈分析師【勝率精選 5 幣版 + 主力動向確認 + 持倉監控版】雷達正在開機...")
    offset = None
    la_tz = pytz.timezone('America/Los_Angeles')
    last_reported_hour = -1
    last_monitor_time  = 0  # epoch seconds，用絕對時間確保真正 1 小時間隔

    while True:
        try:
            now_la   = datetime.datetime.now(la_tz)
            now_ts   = time.time()

            # A. 定時播報（整點）
            if now_la.hour in SCHEDULE_HOURS and now_la.minute == 0 and now_la.hour != last_reported_hour:
                print(f"🔔 觸發加州整點定時播報：{now_la.hour}:00")
                t = threading.Thread(target=scan_worker_thread, args=("設定節點定時速報", TELEGRAM_CHAT_ID))
                t.daemon = True
                t.start()
                last_reported_hour = now_la.hour

            # B. 持倉監控（每 60 分鐘執行一次，用絕對時間防止重啟重複觸發）
            if now_ts - last_monitor_time >= 3600:
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
                    if "message" in update and "text" in update["message"]:
                        msg = update["message"]
                        text = msg["text"].strip()
                        chat_id = str(msg["chat"]["id"])

                        if text.startswith("/scan"):
                            print(f"⚡ 收到 /scan 指令")
                            confirm_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
                            requests.post(confirm_url, json={"chat_id": chat_id, "text": "⚡ 收到指令！正在進行全網掃描 + 主力動向確認，精選勝率最高 5 標的，請稍候約 15 秒..."})
                            t = threading.Thread(target=scan_worker_thread, args=("手動現場突擊播報", chat_id))
                            t.daemon = True
                            t.start()

                        elif text.startswith("/holding"):
                            print(f"📋 收到 /holding 指令")
                            t = threading.Thread(target=send_holding_summary, args=(chat_id,))
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
