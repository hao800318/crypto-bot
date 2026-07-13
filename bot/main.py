import requests
import pandas as pd
import datetime
import pytz
import os
import time
import math
import threading
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

def get_higher_tf_alignment(asset, direction):
    """1H訊號：取4H MA8 vs EMA89 判斷是否與訊號方向對齊"""
    try:
        url = f"{BASE_URL}/api/v5/market/candles?instId={asset}&bar=4H&limit=100"
        res = requests.get(url, timeout=2.0).json()
        if res.get('code') == '0' and len(res['data']) >= 90:
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

            # ④ 多時框對齊（1H訊號需4H方向一致）
            tf_bonus = 0
            tf_note  = ""
            if tf == "1h":
                aligned = get_higher_tf_alignment(asset, direction)
                if aligned:
                    tf_bonus = 10
                    tf_note  = " ✅4H對齊"
                else:
                    tf_bonus = -20
                    tf_note  = " ⚠️4H逆向"

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
active_positions = []          # 儲存已播報的訊號
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

    # ── 先確認限價單是否已被成交 ──
    # 多單：需等價格回踩到 entry（低於進場點）才算成交
    # 空單：需等價格反彈到 entry（高於進場點）才算成交
    if not pos.get('filled', False):
        if dir == "多" and current_price <= entry:
            pos['filled'] = True
            print(f"✅ {pos['asset']} 多單已觸碰進場點 {format_price(entry)}，開始監控 TP/SL")
        elif dir == "空" and current_price >= entry:
            pos['filled'] = True
            print(f"✅ {pos['asset']} 空單已觸碰進場點 {format_price(entry)}，開始監控 TP/SL")
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

    # ── 持倉狀態判定 ──
    if dir == "多":
        dist_to_sl_pct = (current_price - sl) / entry * 100
        if current_price <= sl:
            status = "🔴 止損觸發"
            action = f"⛔ 已觸及止損位 {format_price(sl)}，<b>建議立即平倉</b>"
            push = True
        elif current_price >= tp3:
            status = "🟣 全部止盈"
            action = f"🎯 已達止盈3 {format_price(tp3)}，<b>建議全數平倉</b>"
            push = True
        elif current_price >= tp2:
            status = "🔵 止盈2達標"
            action = f"✅ 已達止盈2，<b>建議再平倉30%</b>，止損上移至止盈1（{format_price(tp1)}）"
            push = True
        elif current_price >= tp1:
            status = "🟢 止盈1達標"
            action = f"✅ 已達止盈1，<b>建議平倉50%</b>，止損上移至成本（{format_price(entry)}）"
            push = True
        elif dist_to_sl_pct < 0.5:
            status = "⚠️ 接近止損"
            action = f"⚠️ 距止損僅 {dist_to_sl_pct:.2f}%，<b>建議收緊止損或現價平倉</b>"
            push = True
        else:
            # 無重大事件，只在有巨鯨異常時推送
            status = "🔄 持倉中"
            action = f"持倉正常，現價 {format_price(current_price)}，距止損 {dist_to_sl_pct:.1f}%"
            push = bool(whale_warn)
    else:  # 空
        dist_to_sl_pct = (sl - current_price) / entry * 100
        if current_price >= sl:
            status = "🔴 止損觸發"
            action = f"⛔ 已觸及止損位 {format_price(sl)}，<b>建議立即平倉</b>"
            push = True
        elif current_price <= tp3:
            status = "🟣 全部止盈"
            action = f"🎯 已達止盈3 {format_price(tp3)}，<b>建議全數平倉</b>"
            push = True
        elif current_price <= tp2:
            status = "🔵 止盈2達標"
            action = f"✅ 已達止盈2，<b>建議再平倉30%</b>，止損下移至止盈1（{format_price(tp1)}）"
            push = True
        elif current_price <= tp1:
            status = "🟢 止盈1達標"
            action = f"✅ 已達止盈1，<b>建議平倉50%</b>，止損下移至成本（{format_price(entry)}）"
            push = True
        elif dist_to_sl_pct < 0.5:
            status = "⚠️ 接近止損"
            action = f"⚠️ 距止損僅 {dist_to_sl_pct:.2f}%，<b>建議收緊止損或現價平倉</b>"
            push = True
        else:
            status = "🔄 持倉中"
            action = f"持倉正常，現價 {format_price(current_price)}，距止損 {dist_to_sl_pct:.1f}%"
            push = bool(whale_warn)

    full_action = action
    if whale_warn:
        full_action += f"\n{whale_warn}"

    # 加入 OI/多空比 狀態補充
    full_action += f"\n📊 主力資料：OI變化{oi_change:+.1f}% | 多空比{ls_ratio:.2f} | 費率{fr*100:.4f}%"

    return status, full_action, push

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
                    gap = f"，距進場點 {gap_pct:.2f}%{arrow}"
                alerts.append((pos, "⏰ 掛單已過期",
                    f"掛單超過 {expiry_h}H 未成交，現價 <code>{price_str}</code>{gap}\n"
                    f"   ❌ <b>建議取消此掛單</b>，等待下次訊號重新進場"))
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

    if not alerts:
        print(f"✅ 持倉監控完成，{len(positions)} 筆持倉狀態正常")
        return

    # 發送警報
    msg = f"🔔 <b>【持倉監控警報 - {now_str} PT】</b>\n"
    msg += f"📋 <b>共監控 {len(positions)} 筆持倉，{len(alerts)} 筆需注意：</b>\n"
    msg += "───────────────────────\n\n"

    for pos, status, action in alerts:
        msg += f"<b>{pos['asset']} ({pos['dir']}) {pos['tf']}</b>  {status}\n"
        msg += f"   進場：<code>{format_price(pos['entry'])}</code>  止損：<code>{format_price(pos['sl'])}</code>\n"
        msg += f"   {action}\n\n"

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

    html_message = f"🎯 <b>【幣圈分析師 終極精選版 - {mode_title}】</b>\n"
    html_message += f"🔥 <b>戰術核心</b>：<code>MA8/EMA89交叉 ＋ RSI勝率 ＋ 主力動向確認</code>\n"
    html_message += f"⏰ <b>監控時間</b>：<code>{now_str}</code> (加州太平洋時間 PT)\n"
    html_message += "───────────────────────\n\n"
    html_message += f"📋 <b>當前全網勝率最高【精選 5 強方案】（依勝率由高到低排序）：</b>\n\n"

    for idx, item in enumerate(valid_signals, 1):
        win_rate = item.get('win_rate', 70)
        # 勝率星級
        if win_rate >= 88:
            stars = "⭐⭐⭐⭐⭐"
        elif win_rate >= 82:
            stars = "⭐⭐⭐⭐"
        elif win_rate >= 75:
            stars = "⭐⭐⭐"
        elif win_rate >= 65:
            stars = "⭐⭐"
        else:
            stars = "⭐"

        adx_val = item.get('adx', 0)
        vol_ok  = item.get('vol_confirmed', False)
        vol_tag = "✅量能確認" if vol_ok else "⚠️量能偏低"
        adx_tag = f"ADX {adx_val}"

        html_message += f"{'🥇' if idx==1 else '🥈' if idx==2 else '🥉' if idx==3 else '🔹'} <b>#{idx} {item['asset']} ({item['dir']}) {item['leverage']} 【{item['order_type']} | {item['tf']}】</b>\n"
        html_message += f"   • 🏆 <b>預估勝率</b>：<code>{win_rate}%</code>  {stars}\n"
        html_message += f"   • 📈 <b>趨勢強度</b>：<code>{adx_tag}</code>  <code>{vol_tag}</code>\n"
        html_message += f"   • 📡 <b>主力動向</b>：<code>{item['sentiment_note']}</code>\n"
        html_message += f"   • 📊 <b>進場策略判定</b>：<code>{item['entry_type']}</code>\n"
        html_message += f"   • 📝 <b>建議進場點位</b>：<code>{format_price(item['entry'])}</code>\n"
        html_message += f"   • 🟢 <b>止盈 1 (平倉 50%)</b>：<code>{format_price(item['tp1'])}</code>\n"
        html_message += f"   • 🔵 <b>止盈 2 (平倉 30%)</b>：<code>{format_price(item['tp2'])}</code>\n"
        html_message += f"   • 🟣 <b>止盈 3 (平倉 20%)</b>：<code>{format_price(item['tp3'])}</code>\n"
        html_message += f"   • 🔴 <b>ATR動態止損位</b>：<code>{format_price(item['sl'])}</code>\n\n"

    html_message += "───────────────────────\n"
    html_message += "💡 <i>勝率 = RSI ＋ 成交量 ＋ 多時框 ＋ 主力動向 ＋ BTC方向 綜合計算</i>\n"
    html_message += "⚠️ <i>槓桿僅供參考，請依個人風險承受能力調整。</i>"

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
                # 避免重複加入同幣同方向
                exists = any(p['asset'] == sig['asset'] and p['dir'] == sig['dir'] for p in active_positions)
                if not exists:
                    active_positions.append({
                        'asset':       sig['asset'],
                        'dir':         sig['dir'],
                        'tf':          sig['tf'],
                        'entry':       sig['entry'],
                        'sl':          sig['sl'],
                        'tp1':         sig['tp1'],
                        'tp2':         sig['tp2'],
                        'tp3':         sig['tp3'],
                        'reported_at': now_ts,
                        'filled':      False,   # 限價單尚未成交
                    })
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

    for pos in positions:
        age_h = (time.time() - pos['reported_at']) / 3600
        status, action, _ = analyze_position(pos)
        if status is None:
            status, action = "❓ 無法取得", "無法取得現價"
        msg += f"<b>{pos['asset']} ({pos['dir']}) {pos['tf']}</b>  {status}  <i>({age_h:.1f}h前播報)</i>\n"
        msg += f"   {action}\n\n"

    msg += "───────────────────────\n💡 <i>持倉監控每小時 xx:30 自動推送警報</i>"
    text_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    requests.post(text_url, json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"})

def handle_telegram_updates():
    print("🤖 幣圈分析師【勝率精選 5 幣版 + 主力動向確認 + 持倉監控版】雷達正在開機...")
    offset = None
    la_tz = pytz.timezone('America/Los_Angeles')
    last_reported_hour = -1
    last_monitor_hour  = -1

    while True:
        try:
            now_la = datetime.datetime.now(la_tz)

            # A. 定時播報（整點）
            if now_la.hour in SCHEDULE_HOURS and now_la.minute == 0 and now_la.hour != last_reported_hour:
                print(f"🔔 觸發加州整點定時播報：{now_la.hour}:00")
                t = threading.Thread(target=scan_worker_thread, args=("設定節點定時速報", TELEGRAM_CHAT_ID))
                t.daemon = True
                t.start()
                last_reported_hour = now_la.hour

            # B. 持倉監控（每小時 xx:30）
            if now_la.minute == 30 and now_la.hour != last_monitor_hour:
                print(f"🔍 觸發持倉監控：{now_la.hour}:30")
                t = threading.Thread(target=run_position_monitor)
                t.daemon = True
                t.start()
                last_monitor_hour = now_la.hour

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
