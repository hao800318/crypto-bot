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
SCHEDULE_HOURS = [6, 8, 10, 12, 14, 18, 22]

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
        ccy = asset.split('-')[0]   # ETH-USDT-SWAP → ETH；ETH → ETH
        ls_url = f"{BASE_URL}/api/v5/rubik/stat/contracts/long-short-account-ratio?ccy={ccy}&period=5m"
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

def get_higher_tf_ema89_slope(asset, current_bar):
    """取高一級時間框架的 EMA89 斜率：1H→4H，4H→1D。回傳正數=上行，負數=下行，None=取得失敗。"""
    htf_bar = "4H" if current_bar == "1H" else "1D"
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

            # ② 成交量確認：交叉K棒量必須 > 過去20根均量（硬性過濾無量假突破）
            avg_vol_20 = df['vol'].iloc[-22:-2].mean()
            cross_vol  = df['vol'].iloc[-2]
            if cross_vol <= avg_vol_20:
                return None  # 無量突破直接過濾，不進入評分

            # ③ EMA89 斜率過濾：EMA89 橫盤或逆向時交叉幾乎全是假突破
            ema89_slope = c_last['EMA89'] - df['EMA89'].iloc[-4]  # 最近3根的斜率
            if direction == "多" and ema89_slope <= 0:
                return None  # EMA89 仍在下行或橫盤，多頭訊號不可信
            if direction == "空" and ema89_slope >= 0:
                return None  # EMA89 仍在上行或橫盤，空頭訊號不可信

            # ④ RSI 背離過濾：價格方向與 RSI 動能背離 → 假突破機率高
            price_5ago = df['close'].iloc[-6]  # 交叉棒往前5根
            rsi_5ago   = df['RSI'].iloc[-6]
            if direction == "多" and current_price > price_5ago and current_rsi < rsi_5ago:
                return None  # 頂背離：價格新高但 RSI 沒跟上，多頭動能衰竭
            if direction == "空" and current_price < price_5ago and current_rsi > rsi_5ago:
                return None  # 底背離：價格新低但 RSI 沒跟上，空頭動能衰竭

            # ── RSI 勝率基礎評分 ──
            base_score = (100 - abs(current_rsi - 56)) if direction == "多" else (100 - abs(current_rsi - 44))

            # ── 主力動向確認 ──
            funding_rate, ls_ratio = get_market_sentiment(asset)
            sentiment_note, sentiment_bonus = build_sentiment_note(direction, funding_rate, ls_ratio)

            # ③ 成交量加分（已通過硬性過濾；量越大加分越多）
            vol_bonus = 8 if cross_vol > avg_vol_20 * 1.5 else 4

            # ④ 多時框對齊（1H看4H，4H看1D）
            tf_bonus = 0
            tf_note  = ""
            if tf == "1h":
                aligned = get_higher_tf_alignment(asset, direction, higher_bar="4H")
                if aligned:
                    tf_bonus = 20
                    tf_note  = " ✅4H對齊"
                else:
                    tf_bonus = 0
                    tf_note  = " ⚠️4H逆向"
            else:  # 4h
                aligned = get_higher_tf_alignment(asset, direction, higher_bar="1D")
                if aligned:
                    tf_bonus = 20
                    tf_note  = " ✅1D對齊"
                else:
                    tf_bonus = 0
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
                tp_mults = (1.5, 3.0, 5.0)   # ATR 倍數 TP1/TP2/TP3
            else:
                anchor_entry = current_ema89
                anchor_label = f"EMA89={format_price(current_ema89)}"
                atr_mult = 2.0
                tp_mults = (2.0, 4.0, 7.0)

            entry_price = anchor_entry

            # ⑦ ATR 動態止損 + 止盈（全部用 ATR 倍數，波動大時目標自動拉寬）
            atr_sl_dist = current_atr * atr_mult
            if direction == "多":
                sl_price = max(entry_price - atr_sl_dist, entry_price * 0.96)
                tp1 = entry_price + current_atr * tp_mults[0]
                tp2 = entry_price + current_atr * tp_mults[1]
                tp3 = entry_price + current_atr * tp_mults[2]
                if current_rsi > 65:
                    entry_type = f"⚠️ {tf_tag}RSI過熱({current_rsi:.1f})，掛限價等回踩 {anchor_label}"
                else:
                    entry_type = f"📌 {tf_tag}掛限價 {anchor_label}，ATR止損={format_price(sl_price)}"
            else:
                sl_price = min(entry_price + atr_sl_dist, entry_price * 1.04)
                tp1 = entry_price - current_atr * tp_mults[0]
                tp2 = entry_price - current_atr * tp_mults[1]
                tp3 = entry_price - current_atr * tp_mults[2]
                if current_rsi < 35:
                    entry_type = f"⚠️ {tf_tag}RSI超賣({current_rsi:.1f})，掛限價等反彈至 {anchor_label}"
                else:
                    entry_type = f"📌 {tf_tag}掛限價 {anchor_label}，ATR止損={format_price(sl_price)}"

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
    top_signals = all_signals[:3]

    print(f"📊 掃描結果：共找到 {len(all_signals)} 組信號，精選前 {len(top_signals)} 名")
    return top_signals

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

# ==================== 📈 勝率統計系統 ====================
STATS_FILE = "trade_stats.json"

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
    """記錄交易結果；outcome: win_tp3/win_tp2/breakeven/loss"""
    records = load_stats()
    records.append({
        "asset":     pos['asset'],
        "dir":       pos['dir'],
        "tf":        pos.get('tf', '—'),
        "entry":     pos.get('entry'),
        "outcome":   outcome,
        "timestamp": time.time(),
    })
    save_stats(records)
    print(f"📊 記錄交易結果：{pos['asset']} {pos['dir']} → {outcome}")

# 最近一次掃描結果快取（key = asset 如 "ETH"，value = signal dict）
# 供 /open 指令查詢，不自動加入持倉監控
last_scan_cache: dict = {}
last_scan_lock  = threading.Lock()

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
            action = (f"🎯 {how} {format_price(effective_low if dir == '多' else effective_high)} "
                      f"已觸及進場點 {format_price(entry)}，現價 {format_price(current_price)}\n"
                      f"<b>限價單應已成交，開始監控 TP/SL</b>")
            return "✅ 已觸及進場點", action, True  # 推送進場確認，下次監控再做 TP/SL
        else:
            # ── 掛單尚未成交 ──
            # 1. 止損點已被突破 → 訊號作廢，自動取消
            sl_breached = (
                (dir == "多" and effective_low  <= sl) or
                (dir == "空" and effective_high >= sl)
            )
            if sl_breached:
                note = (f"⛔ 現價 {format_price(current_price)} 已突破止損位 {format_price(sl)}，"
                        f"進場點 {format_price(entry)} 訊號作廢\n<b>已自動取消掛單追蹤</b>")
                return "🚫 掛單已取消", note, True

            # 2. 等待逾時（1H→2H、4H/手動→8H）→ 自動取消
            tf = pos.get('tf', '4H')
            timeout_sec = 7200 if '1H' in tf else 28800  # 2H or 8H
            pending_sec = time.time() - pos.get('reported_at', time.time())
            if pending_sec > timeout_sec:
                hours = int(pending_sec / 3600)
                note = (f"⏰ 掛單等待已超過 {hours} 小時，進場點 {format_price(entry)} 仍未觸及，"
                        f"現價 {format_price(current_price)}\n<b>訊號可能已失效，已自動取消掛單追蹤</b>")
                return "⏰ 掛單逾時取消", note, True

            # 3. 正常等待中，靜默監控不推送
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
        whale_warn = f"⚡ 異常量能！成交量是均量 {vol_spike:.1f} 倍，方向不確定，<b>建議收緊止損，暫勿加倉</b>"
    elif vol_spike < 0.5:
        whale_warn = f"📉 量能萎縮（僅均量 {vol_spike*100:.0f}%），趨勢動能減弱，<b>建議縮小倉位或收緊止損</b>"
    if oi_change < -5:
        whale_warn += f"\n📉 OI 下降 {oi_change:.1f}%，主力正在撤退，<b>建議部分平倉或收緊止損</b>"
    elif oi_change > 5:
        whale_warn += f"\n📈 OI 上升 {oi_change:.1f}%，主力持續加倉，<b>可繼續持倉並同步上移止損鎖利</b>"

    # ── 持倉狀態判定（用 K 線高低點而非現價，防止監控間隔中的事件被漏掉）──
    if dir == "多":
        dist_to_sl_pct = (current_price - sl) / entry * 100
        # 止損：用 K 線低點（只要低點碰過 SL 就算觸發）
        if effective_low <= sl:
            if pos.get('tp1_hit'):
                status = "🛡️ 回調至保本止損"
                action = (f"📉 K線低點 {format_price(effective_low)} 回調至保本止損 {format_price(sl)}，"
                          f"現價 {format_price(current_price)}｜<b>剩餘倉位建議出場，TP1利潤已鎖定</b>")
            else:
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
                    status = "🚨 局勢惡化"
                    action += f"\n{deteri}"
                    push = True
                else:
                    push = False
            else:
                pos['tp2_hit'] = True
                pos['sl'] = tp1   # SL 立即鎖至 TP1，剩餘倉位零風險
                sl = tp1
                status = "🔵 止盈2達標"
                action = (f"✅ K線高點 {format_price(effective_high)} 已達止盈2 {format_price(tp2)}，"
                          f"現價 {format_price(current_price)}｜<b>建議再平倉30%</b>，止損已鎖至TP1（{format_price(tp1)}）")
                push = True
        elif effective_high >= tp1:
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
                    status = "🚨 局勢惡化"
                    action += f"\n{deteri}"
                    push = True
                else:
                    push = False   # 鯨魚警報單獨不推送，只附在訊息裡
            else:
                status = "🟢 止盈1達標"
                action = (f"✅ K線高點 {format_price(effective_high)} 已達止盈1 {format_price(tp1)}，"
                          f"現價 {format_price(current_price)}｜<b>建議平倉50%</b>，止損上移至成本（{format_price(entry)}）")
                pos['trail_dist'] = entry - sl  # 保存 ATR 距離供追蹤止損使用
                pos['sl'] = entry  # 立即把 SL 移至進場點（保本止損）
                sl = entry
                push = True
        elif dist_to_sl_pct < 0.5:
            if pos.get('tp1_hit'):
                status = "⚠️ 即將觸及保本止損"
                action = f"⚠️ 距保本止損（{format_price(sl)}）僅 {dist_to_sl_pct:.2f}%，現價 {format_price(current_price)}｜<b>考慮主動出場鎖利</b>"
            else:
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
                push = False   # 鯨魚警報單獨不推送
    else:  # 空
        dist_to_sl_pct = (sl - current_price) / entry * 100
        # 止損：用 K 線高點
        if effective_high >= sl:
            if pos.get('tp1_hit'):
                status = "🛡️ 回調至保本止損"
                action = (f"📈 K線高點 {format_price(effective_high)} 回調至保本止損 {format_price(sl)}，"
                          f"現價 {format_price(current_price)}｜<b>剩餘倉位建議出場，TP1利潤已鎖定</b>")
            else:
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
                    status = "🚨 局勢惡化"
                    action += f"\n{deteri}"
                    push = True
                else:
                    push = False
            else:
                pos['tp2_hit'] = True
                pos['sl'] = tp1   # SL 立即鎖至 TP1，剩餘倉位零風險
                sl = tp1
                status = "🔵 止盈2達標"
                action = (f"✅ K線低點 {format_price(effective_low)} 已達止盈2 {format_price(tp2)}，"
                          f"現價 {format_price(current_price)}｜<b>建議再平倉30%</b>，止損已鎖至TP1（{format_price(tp1)}）")
                push = True
        elif effective_low <= tp1:
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
                    status = "🚨 局勢惡化"
                    action += f"\n{deteri}"
                    push = True
                else:
                    push = False   # 鯨魚警報單獨不推送
            else:
                status = "🟢 止盈1達標"
                action = (f"✅ K線低點 {format_price(effective_low)} 已達止盈1 {format_price(tp1)}，"
                          f"現價 {format_price(current_price)}｜<b>建議平倉50%</b>，止損下移至成本（{format_price(entry)}）")
                pos['trail_dist'] = sl - entry  # 保存 ATR 距離供追蹤止損使用
                pos['sl'] = entry  # 立即把 SL 移至進場點（保本止損）
                sl = entry
                push = True
        elif dist_to_sl_pct < 0.5:
            if pos.get('tp1_hit'):
                status = "⚠️ 即將觸及保本止損"
                action = f"⚠️ 距保本止損（{format_price(sl)}）僅 {dist_to_sl_pct:.2f}%，現價 {format_price(current_price)}｜<b>考慮主動出場鎖利</b>"
            else:
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
                push = False   # 鯨魚警報單獨不推送

    full_action = action
    if whale_warn:
        full_action += f"\n{whale_warn}"

    # 加入 OI/多空比 狀態補充
    long_pct  = round(ls_ratio / (ls_ratio + 1) * 100)
    short_pct = 100 - long_pct
    full_action += f"\n📊 主力資料：OI變化{oi_change:+.1f}% | 多{long_pct}%:空{short_pct}% | 費率{fr*100:.4f}%"

    # ── 累計記錄 TP 達標旗標（只增不減，跨監控週期保留）──
    if pos.get('filled'):
        if dir == "多":
            if effective_high >= tp1: pos['tp1_hit'] = True
            if effective_high >= tp2: pos['tp2_hit'] = True
            if effective_high >= tp3: pos['tp3_hit'] = True
        else:
            if effective_low <= tp1: pos['tp1_hit'] = True
            if effective_low <= tp2: pos['tp2_hit'] = True
            if effective_low <= tp3: pos['tp3_hit'] = True

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

        # 止損觸發 / 全部止盈 / 保本回調 / 掛單取消 → 移除追蹤 + 記錄結果
        OUTCOME_MAP = {
            "🟣 全部止盈":        "win_tp3",
            "🛡️ 回調至保本止損":  "breakeven",
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
            rec_text, rec_icon = REC.get(status, ("自行判斷", "❓"))
            msg += f"{sub}{d}  {pos['tf']}  {status}\n"
            msg += "<pre>"
            msg += f"進場  {format_price(pos['entry'])}\n"
            msg += f"止損  {format_price(pos['sl'])}\n"
            msg += "</pre>"
            msg += f"▸ {action}\n"
            msg += f"<b>【持倉建議】{rec_icon} {rec_text}</b>\n"
            # 保本出場 / 止損 / 全止盈 → 標註自動移除
            if status in ("🔴 止損觸發", "🛡️ 回調至保本止損", "🟣 全部止盈"):
                msg += f"<i>（此持倉已自動移除追蹤）</i>\n"

        conclusion = build_coin_conclusion(all_by_asset[asset], cp)
        if conclusion:
            msg += f"{conclusion}\n"
        msg += "─────────────────────────\n"

    msg += "<i>以上為自動監控建議，請結合自身判斷操作。</i>"

    text_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    resp = requests.post(text_url, json={"chat_id": str(TELEGRAM_CHAT_ID), "text": msg, "parse_mode": "HTML"})
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
        if s is None or s['adx'] < 18:
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
            sl = s1['ma8'] - s1['atr'] * 1.5
            lines.append(f"  止損：<b>{format_price(sl)}</b>  TP參考：<b>{format_price(s1['ma8'] * 1.03)}</b> / <b>{format_price(s1['ma8'] * 1.06)}</b>")
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
            sl = s1['ma8'] + s1['atr'] * 1.5
            lines.append(f"  止損：<b>{format_price(sl)}</b>  TP參考：<b>{format_price(s1['ma8'] * 0.97)}</b> / <b>{format_price(s1['ma8'] * 0.94)}</b>")
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
        return s is not None and s['adx'] >= 18 and (s['ma_above'] == bull_conclusion)

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
        html_message += (f"{medal} <b>{item['asset']}</b>  {dir_display}  "
                         f"⚡<b>{item['leverage']}</b>  {item['tf']}  "
                         f"<b>{win_rate}%</b> {stars}\n")
        # ── 趨勢 + 主力 ──
        html_message += f"趨勢 {adx_bar} <b>{adx_level}</b>  ｜  {sentiment_short}\n"
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
        # ── TP1後止損提示 ──
        html_message += f"▸ TP1達標 → 止損移至 <code>{format_price(item['entry'])}</code>\n"
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
def scan_worker_thread(msg_title, target_chat_id, silent_on_empty=False):
    valid_signals = run_strategy_scan()
    if valid_signals:
        send_html_report_via_requests(valid_signals, mode_title=msg_title, target_chat_id=target_chat_id)
        # 快取最新訊號供 /open 指令查詢（不自動加入持倉監控，等用戶確認開倉）
        with last_scan_lock:
            last_scan_cache.clear()
            for sig in valid_signals:
                last_scan_cache[sig['asset']] = {**sig, 'cached_at': time.time()}
        print(f"📦 訊號已快取 {len(valid_signals)} 筆，等待 /open 指令確認")
    else:
        if silent_on_empty:
            print(f"📭 定時掃描無訊號，靜默略過（{msg_title}）")
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
    msg = f"<b>【持倉監控總覽】</b>  <code>{now_str} PT</code>\n"
    msg += f"共追蹤 <b>{len(positions)}</b> 筆持倉\n"
    msg += "─────────────────────────\n"

    by_asset = defaultdict(list)
    for pos in positions:
        by_asset[pos['asset']].append(pos)

    for asset, group in by_asset.items():
        inst_id = asset + '-USDT-SWAP'
        cp = get_current_price(inst_id)
        price_str = format_price(cp) if cp else "—"
        msg += f"<b>{asset}</b>  現價 <code>{price_str}</code>\n"

        for i, pos in enumerate(group, 1):
            age_h  = (time.time() - pos['reported_at']) / 3600
            status, action, _ = analyze_position(pos)
            if status is None:
                status, action = "❓ 無法取得現價", "—"
            d   = "🟩<b>多</b>" if pos['dir'] == "多" else "🟥<b>空</b>"
            sub = f"#{i} " if len(group) > 1 else ""
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

        conclusion = build_coin_conclusion(group, cp)
        if conclusion:
            msg += f"{conclusion}\n"
        msg += "─────────────────────────\n"

    msg += "<i>📌 /open ETH  確認開倉並加入監控\n🗑️ /close ETH  平倉後解除監控\n⚡ 警報僅在 TP/SL/市場惡化時推送</i>"
    text_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    requests.post(text_url, json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"})

# ==================== 📌 手動開倉/平倉指令 ====================
def handle_open_command(text, chat_id):
    """
    /open ETH        → 用最近快取訊號的進場參數開倉
    /open ETH 多     → 指定方向（快取中有多空兩筆時）
    """
    parts = text.strip().split()
    if len(parts) < 2:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": chat_id,
                  "text": "❓ 用法：/open ETH 或 /open ETH 多\n如尚未掃描請先執行 /scan 取得訊號"}
        )
        return

    symbol    = parts[1].upper()
    direction = parts[2] if len(parts) >= 3 else None  # "多" or "空"

    with last_scan_lock:
        sig = last_scan_cache.get(symbol)

    now_ts = time.time()

    # ── 有快取訊號 ──
    if sig:
        cache_age_min = (now_ts - sig.get('cached_at', 0)) / 60
        if cache_age_min > 120:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": chat_id,
                      "text": f"⚠️ {symbol} 的訊號已超過 2 小時，請重新執行 /scan 或 /{symbol.lower()} 後再開倉"}
            )
            return

        if direction and sig['dir'] != direction:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": chat_id,
                      "text": f"⚠️ 快取中 {symbol} 的訊號方向為「{sig['dir']}」，與你指定的「{direction}」不符\n如確認開倉請輸入 /open {symbol} {sig['dir']}"}
            )
            return

        new_pos = {
            'asset':           sig['asset'],
            'dir':             sig['dir'],
            'tf':              sig['tf'],
            'entry':           sig['entry'],
            'sl':              sig['sl'],
            'tp1':             sig['tp1'],
            'tp2':             sig['tp2'],
            'tp3':             sig['tp3'],
            'reported_at':     now_ts,
            'last_checked_ts': now_ts,
            'filled':          False,  # 掛單等待模式：等價格觸碰進場點後才開始 TP/SL 監控
        }
        dir_tag = "🟩多" if sig['dir'] == "多" else "🟥空"
        confirm_text = (
            f"⏳ <b>{symbol} {dir_tag} 掛單監控中</b>\n"
            f"<pre>"
            f"進場  {format_price(sig['entry'])}\n"
            f"止損  {format_price(sig['sl'])}\n"
            f"TP1   {format_price(sig['tp1'])}\n"
            f"TP2   {format_price(sig['tp2'])}\n"
            f"TP3   {format_price(sig['tp3'])}\n"
            f"</pre>"
            f"📌 價格觸及進場點時自動推送確認通知\n"
            f"確認成交後開始監控 TP/SL/市場惡化\n"
            f"取消掛單請輸入 /close {symbol}"
        )

    else:
        # ── 無快取 → 用當前市價建立簡易監控 ──
        inst_id = f"{symbol}-USDT-SWAP"
        current_price = get_current_price(inst_id)
        if not current_price:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": chat_id,
                      "text": f"❌ 找不到 {symbol} 的市場數據，請確認幣種名稱"}
            )
            return

        dir_use = direction if direction in ("多", "空") else "多"
        # 用當前價作為進場，ATR 估算止損止盈
        s1 = analyze_coin_snapshot(inst_id, "1H")
        atr = s1['atr'] if s1 else current_price * 0.02
        if dir_use == "多":
            sl  = current_price - atr * 1.5
            tp1 = current_price * 1.015
            tp2 = current_price * 1.030
            tp3 = current_price * 1.050
        else:
            sl  = current_price + atr * 1.5
            tp1 = current_price * 0.985
            tp2 = current_price * 0.970
            tp3 = current_price * 0.950

        new_pos = {
            'asset':           symbol,
            'dir':             dir_use,
            'tf':              '手動',
            'entry':           current_price,
            'sl':              sl,
            'tp1':             tp1,
            'tp2':             tp2,
            'tp3':             tp3,
            'reported_at':     now_ts,
            'last_checked_ts': now_ts,
            'filled':          True,
        }
        dir_tag = "🟩多" if dir_use == "多" else "🟥空"
        confirm_text = (
            f"📌 <b>{symbol} {dir_tag}（手動建倉）已加入監控</b>\n"
            f"<pre>"
            f"進場  {format_price(current_price)}（市價）\n"
            f"止損  {format_price(sl)}\n"
            f"TP1   {format_price(tp1)}\n"
            f"TP2   {format_price(tp2)}\n"
            f"TP3   {format_price(tp3)}\n"
            f"</pre>"
            f"<i>⚠️ 止損/止盈由 ATR 自動估算，可用 /holding 查看</i>\n"
            f"平倉後請輸入 /close {symbol} 解除監控"
        )

    with active_positions_lock:
        # 同幣種+同方向若已存在則覆蓋
        active_positions[:] = [
            p for p in active_positions
            if not (p['asset'] == new_pos['asset'] and p['dir'] == new_pos['dir'])
        ]
        active_positions.append(new_pos)
        save_positions(active_positions)

    print(f"📌 /open {symbol} {new_pos['dir']} 已加入監控，共 {len(active_positions)} 筆")
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        json={"chat_id": chat_id, "text": confirm_text, "parse_mode": "HTML"}
    )


def handle_close_command(text, chat_id):
    """
    /close ETH     → 移除 ETH 所有持倉
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

    target  = parts[1].upper()
    direction = parts[2] if len(parts) >= 3 else None

    with active_positions_lock:
        before = len(active_positions)
        if target == "ALL":
            active_positions.clear()
            removed = before
        else:
            if direction:
                active_positions[:] = [
                    p for p in active_positions
                    if not (p['asset'] == target and p['dir'] == direction)
                ]
            else:
                active_positions[:] = [
                    p for p in active_positions
                    if p['asset'] != target
                ]
            removed = before - len(active_positions)
        save_positions(active_positions)

    if removed == 0:
        msg = f"❓ 找不到 {target} 的持倉記錄（可用 /holding 查看目前持倉）"
    elif target == "ALL":
        msg = f"✅ 已清空所有持倉監控（共移除 {removed} 筆）"
    else:
        dir_note = f" {direction}" if direction else ""
        msg = f"✅ {target}{dir_note} 已移除持倉監控（移除 {removed} 筆）"

    print(f"🗑️ /close {target}: 移除 {removed} 筆")
    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        json={"chat_id": chat_id, "text": msg}
    )


def send_stats_report(chat_id):
    """每日勝率播報（近30天）"""
    records = load_stats()
    cutoff  = time.time() - 30 * 86400
    recent  = [r for r in records if r.get('timestamp', 0) >= cutoff]
    valid   = [r for r in recent if r['outcome'] in ('win_tp3', 'breakeven', 'loss')]

    if not valid:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": str(chat_id),
                  "text": "📊 近30天尚無完整交易記錄，繼續積累數據中。"}
        )
        return

    wins       = [r for r in valid if r['outcome'] == 'win_tp3']
    breakevens = [r for r in valid if r['outcome'] == 'breakeven']
    losses     = [r for r in valid if r['outcome'] == 'loss']
    total      = len(valid)
    win_rate   = len(wins) / total * 100
    be_rate    = len(breakevens) / total * 100

    la_tz   = pytz.timezone('America/Los_Angeles')
    now_str = datetime.datetime.now(la_tz).strftime('%Y-%m-%d')

    msg  = f"<b>【每日勝率統計】</b>  <code>{now_str}</code>\n"
    msg += f"近30天共 <b>{total}</b> 筆完整交易\n"
    msg += "─────────────────────────\n"
    msg += f"🟣 全止盈    <b>{len(wins)}</b> 筆  ({win_rate:.1f}%)\n"
    msg += f"🛡️ 保本出場  <b>{len(breakevens)}</b> 筆  ({be_rate:.1f}%)\n"
    msg += f"🔴 止損      <b>{len(losses)}</b> 筆  ({100-win_rate-be_rate:.1f}%)\n"
    msg += "─────────────────────────\n"
    msg += f"整體勝率（止盈+保本）：<b>{win_rate+be_rate:.1f}%</b>\n\n"

    # 各幣種細分
    from collections import Counter
    asset_wins   = Counter(r['asset'] for r in wins)
    asset_losses = Counter(r['asset'] for r in losses)
    all_assets   = sorted(set(r['asset'] for r in valid))
    if all_assets:
        msg += "<b>各幣種：</b>\n"
        for a in all_assets:
            w = asset_wins.get(a, 0)
            l = asset_losses.get(a, 0)
            b = sum(1 for r in breakevens if r['asset'] == a)
            t = w + l + b
            pct = (w + b) / t * 100 if t else 0
            msg += f"  {a}：{w}W {b}BE {l}L  ({pct:.0f}%)\n"

    msg += "\n<i>數據僅供參考，保持紀律為第一要務。</i>"

    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        json={"chat_id": str(chat_id), "text": msg, "parse_mode": "HTML"}
    )
    print(f"📊 每日勝率播報完成（{total} 筆）")


def handle_telegram_updates():
    print("🤖 幣圈分析師【勝率精選 5 幣版 + 主力動向確認 + 持倉監控版】雷達正在開機...")
    offset = None
    la_tz = pytz.timezone('America/Los_Angeles')
    last_reported_hour = -1
    last_monitor_time  = 0
    last_stats_date    = None  # 每日勝率播報去重

    while True:
        try:
            now_la   = datetime.datetime.now(la_tz)
            now_ts   = time.time()

            # A. 定時播報（整點）
            if now_la.hour in SCHEDULE_HOURS and now_la.minute == 0 and now_la.hour != last_reported_hour:
                print(f"🔔 觸發加州整點定時播報：{now_la.hour}:00")
                t = threading.Thread(target=scan_worker_thread, args=("設定節點定時速報", TELEGRAM_CHAT_ID, True))
                t.daemon = True
                t.start()
                last_reported_hour = now_la.hour

            # B2. 每日勝率播報（00:00 PT）
            if now_la.hour == 0 and now_la.minute < 2 and last_stats_date != now_la.date():
                last_stats_date = now_la.date()
                print(f"📊 觸發每日勝率播報：{now_la.strftime('%Y-%m-%d')}")
                t = threading.Thread(target=send_stats_report, args=(TELEGRAM_CHAT_ID,))
                t.daemon = True
                t.start()

            # B. 持倉監控（有 TP1/TP2 命中的倉位縮短至 15 分鐘，其餘 30 分鐘）
            with active_positions_lock:
                has_tp_hit = any(p.get('tp1_hit') or p.get('tp2_hit') for p in active_positions)
            monitor_interval = 900 if has_tp_hit else 1800
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

                        elif text.lower().startswith("/open"):
                            print(f"📌 收到 /open 指令：{text}")
                            t = threading.Thread(target=handle_open_command, args=(text, chat_id))
                            t.daemon = True
                            t.start()

                        elif text.lower().startswith("/close"):
                            print(f"🗑️ 收到 /close 指令：{text}")
                            t = threading.Thread(target=handle_close_command, args=(text, chat_id))
                            t.daemon = True
                            t.start()

                        elif text.lower().startswith("/stats"):
                            print(f"📊 收到 /stats 指令")
                            t = threading.Thread(target=send_stats_report, args=(chat_id,))
                            t.daemon = True
                            t.start()

                        elif text.startswith("/") and len(text) > 1:
                            # 通用幣種查詢：/eth /btc /sol /doge 等
                            coin_cmd = text.split()[0].lstrip("/").split("@")[0]
                            if coin_cmd and coin_cmd.isalpha() and coin_cmd.lower() not in ("open","close","scan","holding"):
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
