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

# ==================== ⚙️ 4. 同步 K 線與勝率量化評分核心 ====================
def score_to_win_rate(score):
    """將內部評分（0-112）映射為 50%–98% 勝率顯示"""
    return min(98, max(50, int(50 + (score / 112) * 48)))

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
    label = "🔥滿槓桿" if ratio == 1.00 else f"上限{max_leverage}x的{int(ratio*100)}%"
    return f"{lev}x（{label}）"

def fetch_candle_sync(asset, tf, max_leverage=20):
    bar_param = "1H" if tf == "1h" else "4H"
    url = f"{BASE_URL}/api/v5/market/candles?instId={asset}&bar={bar_param}&limit=100"
    try:
        res = requests.get(url, timeout=2.0).json()
        if res.get('code') == '0' and len(res['data']) >= 90:
            df = pd.DataFrame(res['data'], columns=['ts', 'open', 'high', 'low', 'close', 'vol', 'volCcy', 'volCcyQuote', 'state'])
            df['close'] = df['close'].astype(float)
            df = df.iloc[::-1].reset_index(drop=True)

            df['MA8'] = df['close'].rolling(window=8).mean()
            df['EMA89'] = df['close'].ewm(span=89, adjust=False).mean()

            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / (loss + 1e-10)
            df['RSI'] = 100 - (100 / (1 + rs))

            p_last = df.iloc[-2]
            c_last = df.iloc[-1]

            is_cross_up = (p_last['MA8'] <= p_last['EMA89']) and (c_last['MA8'] > c_last['EMA89'])
            is_cross_down = (p_last['MA8'] >= p_last['EMA89']) and (c_last['MA8'] < c_last['EMA89'])

            if is_cross_up or is_cross_down:
                current_price = c_last['close']
                current_rsi = c_last['RSI']
                current_ema89 = c_last['EMA89']
                direction = "多" if is_cross_up else "空"

                # 🎯 RSI 勝率基礎評分
                if direction == "多":
                    base_score = 100 - abs(current_rsi - 56)
                else:
                    base_score = 100 - abs(current_rsi - 44)

                # 📡 主力動向確認加分
                funding_rate, ls_ratio = get_market_sentiment(asset)
                sentiment_note, sentiment_bonus = build_sentiment_note(direction, funding_rate, ls_ratio)
                score = base_score + sentiment_bonus

                # 🏷️ 勝率 % 與動態槓桿
                win_rate = score_to_win_rate(score)
                leverage = score_to_leverage(win_rate, max_leverage)

                order_type = "短線單" if tf == "1h" else "長線單"
                tf_tag = "1H短線" if tf == "1h" else "4H長線"

                # ─── 錨定進場點邏輯 ───
                # 1H：交叉後短線回踩 MA8 是最佳掛單位（指標值穩定，不隨 tick 跳動）
                # 4H：交叉後大週期通常回踩 EMA89 才是真正安全進場位
                current_ma8 = c_last['MA8']

                if tf == "1h":
                    anchor_entry = current_ma8       # 1H 掛 MA8
                    anchor_label = f"MA8={format_price(current_ma8)}"
                    sl_pct_long  = 0.985             # 止損在 MA8 下方 1.5%
                    sl_pct_short = 1.015
                    tp_ratios_long  = (1.015, 1.030, 1.050)
                    tp_ratios_short = (0.985, 0.970, 0.950)
                else:
                    anchor_entry = current_ema89     # 4H 掛 EMA89
                    anchor_label = f"EMA89={format_price(current_ema89)}"
                    sl_pct_long  = 0.982             # 止損在 EMA89 下方 1.8%
                    sl_pct_short = 1.018
                    tp_ratios_long  = (1.030, 1.060, 1.100)
                    tp_ratios_short = (0.970, 0.940, 0.900)

                if direction == "多":
                    entry_price = anchor_entry
                    if current_rsi > 65:
                        entry_type = f"⚠️ {tf_tag}RSI過熱({current_rsi:.1f})，掛限價等回踩 {anchor_label}"
                    else:
                        entry_type = f"📌 {tf_tag}掛限價單於 {anchor_label}（交叉後回踩最佳位）"
                    sl_price = entry_price * sl_pct_long
                    tp1 = entry_price * tp_ratios_long[0]
                    tp2 = entry_price * tp_ratios_long[1]
                    tp3 = entry_price * tp_ratios_long[2]
                else:
                    entry_price = anchor_entry
                    if current_rsi < 35:
                        entry_type = f"⚠️ {tf_tag}RSI超賣({current_rsi:.1f})，掛限價等反彈至 {anchor_label}"
                    else:
                        entry_type = f"📌 {tf_tag}掛限價單於 {anchor_label}（交叉後反彈最佳位）"
                    sl_price = entry_price * sl_pct_short
                    tp1 = entry_price * tp_ratios_short[0]
                    tp2 = entry_price * tp_ratios_short[1]
                    tp3 = entry_price * tp_ratios_short[2]

                return {
                    "asset": asset.split('-')[0],
                    "dir": direction,
                    "leverage": leverage,
                    "win_rate": win_rate,
                    "tf": tf_tag,
                    "order_type": order_type,
                    "score": score,
                    "entry": entry_price,
                    "sl": sl_price,
                    "tp1": tp1,
                    "tp2": tp2,
                    "tp3": tp3,
                    "entry_type": entry_type,
                    "sentiment_note": sentiment_note,
                }
    except:
        pass
    return None

def run_strategy_scan():
    all_assets, leverage_map = get_all_okx_swap_assets()
    all_signals = []

    tasks = [(asset, tf) for asset in all_assets for tf in ["1h", "4h"]]
    total = len(tasks)
    completed = 0
    lock = threading.Lock()

    scan_start = time.time()
    print(f"⏱️ 掃描開始時間：{datetime.datetime.now().strftime('%H:%M:%S')}（並行模式，共 {total} 項任務）")

    def scan_task(asset, tf):
        max_lev = leverage_map.get(asset, 20)
        return fetch_candle_sync(asset, tf, max_leverage=max_lev)

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

# ==================== 🚀 5. 動態精度渲染發送引擎 ====================
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

        html_message += f"{'🥇' if idx==1 else '🥈' if idx==2 else '🥉' if idx==3 else '🔹'} <b>#{idx} {item['asset']} ({item['dir']}) {item['leverage']} 【{item['order_type']} | {item['tf']}】</b>\n"
        html_message += f"   • 🏆 <b>預估勝率</b>：<code>{win_rate}%</code>  {stars}\n"
        html_message += f"   • 📡 <b>主力動向</b>：<code>{item['sentiment_note']}</code>\n"
        html_message += f"   • 📊 <b>進場策略判定</b>：<code>{item['entry_type']}</code>\n"
        html_message += f"   • 📝 <b>建議進場點位</b>：<code>{format_price(item['entry'])}</code>\n"
        html_message += f"   • 🟢 <b>止盈 1 (平倉 50%)</b>：<code>{format_price(item['tp1'])}</code>\n"
        html_message += f"   • 🔵 <b>止盈 2 (平倉 30%)</b>：<code>{format_price(item['tp2'])}</code>\n"
        html_message += f"   • 🟣 <b>止盈 3 (平倉 20%)</b>：<code>{format_price(item['tp3'])}</code>\n"
        html_message += f"   • 🔴 <b>硬防守止損位置</b>：<code>{format_price(item['sl'])}</code>\n\n"

    html_message += "───────────────────────\n"
    html_message += "💡 <i>勝率 = RSI動能評分 ＋ 主力資金費率 ＋ 多空持倉比 綜合計算</i>\n"
    html_message += "⚠️ <i>槓桿僅供參考，請依個人風險承受能力調整。</i>"

    text_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    resp = requests.post(text_url, json={"chat_id": str(target_chat_id), "text": html_message, "parse_mode": "HTML"})
    result = resp.json()
    if result.get("ok"):
        print(f"✅ 精選 5 強報告發送成功 → chat_id={target_chat_id}")
    else:
        print(f"❌ 報告發送失敗：{result}")

# ==================== 📡 6. 原生無衝突監聽引擎 ====================
def scan_worker_thread(msg_title, target_chat_id):
    valid_signals = run_strategy_scan()
    if valid_signals:
        send_html_report_via_requests(valid_signals, mode_title=msg_title, target_chat_id=target_chat_id)
    else:
        text_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        resp = requests.post(text_url, json={"chat_id": str(target_chat_id), "text": "📭 全網通掃完畢，當前盤面極其冷靜，暫無符合勝率條件之信號。"})
        result = resp.json()
        if result.get("ok"):
            print(f"✅ 「盤面冷靜」訊息發送成功")
        else:
            print(f"❌ 「盤面冷靜」訊息發送失敗：{result}")

def handle_telegram_updates():
    print("🤖 幣圈分析師【勝率精選 5 幣版 + 主力動向確認版】雷達正在開機...")
    offset = None
    la_tz = pytz.timezone('America/Los_Angeles')
    last_reported_hour = -1

    while True:
        try:
            now_la = datetime.datetime.now(la_tz)
            if now_la.hour in SCHEDULE_HOURS and now_la.minute == 0 and now_la.hour != last_reported_hour:
                print(f"🔔 觸發加州整點定時播報：{now_la.hour}:00")
                t = threading.Thread(target=scan_worker_thread, args=("設定節點定時速報", TELEGRAM_CHAT_ID))
                t.daemon = True
                t.start()
                last_reported_hour = now_la.hour

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
                            print(f"⚡ 收到手動口令！啟動勝率精選獨立執行緒...")
                            confirm_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
                            requests.post(confirm_url, json={"chat_id": chat_id, "text": "⚡ 收到指令！正在進行全網掃描 + 主力動向確認，精選勝率最高 5 標的，請稍候約 15 秒..."})
                            t = threading.Thread(target=scan_worker_thread, args=("手動現場突擊播報", chat_id))
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
