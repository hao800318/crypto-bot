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
    """動態獲取 OKX 當前在線的所有 USDT 永續合約，實現真正的全幣種掃描"""
    url = f"{BASE_URL}/api/v5/public/instruments?instType=SWAP"
    try:
        res = requests.get(url, timeout=5).json()
        if res.get('code') == '0':
            assets = [item['instId'] for item in res['data'] if item['instId'].endswith('-USDT-SWAP')]
            print(f"📡 成功獲取全網合約資產庫，共計: {len(assets)} 支幣種")
            return assets
    except Exception as e:
        print(f"⚠️ 動態獲取資產庫失敗: {e}")
    return ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "TON-USDT-SWAP", "ARB-USDT-SWAP"]

# ==================== ⚙️ 3. 同步 K 線與勝率量化評分核心 ====================
def fetch_candle_sync(asset, tf):
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

                # 🎯 計算黃金勝率權重評分（越接近健康動能區，權重分越高）
                score = 0
                if direction == "多":
                    # RSI 在 50 到 62 之間是最佳追多動能區，勝率最高
                    score = 100 - abs(current_rsi - 56)
                else:
                    # RSI 在 38 到 50 之間是最佳追空動能區，勝率最高
                    score = 100 - abs(current_rsi - 44)

                order_type = "短線單" if tf == "1h" else "長線單"
                tf_tag = "1H短線" if tf == "1h" else "4H長線"
                leverage = "10x"

                if direction == "多":
                    if current_rsi > 65:
                        entry_price = (current_price + current_ema89) / 2
                        entry_type = f"⚠️ {tf_tag}過熱，建議等回調多單掛接"
                        sl_price = current_ema89 * 0.985
                        tp1 = entry_price * 1.030
                        tp2 = entry_price * 1.060
                        tp3 = entry_price * 1.100
                    else:
                        entry_price = current_price
                        entry_type = f"🚀 {tf_tag}動能健康，回調機率小，可現價追多"
                        sl_price = entry_price * 0.985
                        tp1 = entry_price * 1.015
                        tp2 = entry_price * 1.030
                        tp3 = entry_price * 1.050
                else:
                    if current_rsi < 35:
                        entry_price = (current_price + current_ema89) / 2
                        entry_type = f"⚠️ {tf_tag}超賣，建議等反彈高空掛接"
                        sl_price = current_ema89 * 1.015
                        tp1 = entry_price * 0.970
                        tp2 = entry_price * 0.940
                        tp3 = entry_price * 0.900
                    else:
                        entry_price = current_price
                        entry_type = f"🚀 {tf_tag}空頭動能強，回彈機率小，可現價空"
                        sl_price = entry_price * 1.015
                        tp1 = entry_price * 0.985
                        tp2 = entry_price * 0.970
                        tp3 = entry_price * 0.950

                return {
                    "asset": asset.split('-')[0], "dir": direction, "leverage": leverage,
                    "tf": tf_tag, "order_type": order_type, "score": score,
                    "entry": entry_price, "sl": sl_price, "tp1": tp1, "tp2": tp2, "tp3": tp3,
                    "entry_type": entry_type
                }
    except:
        pass
    return None

def run_strategy_scan():
    """並行掃描全網合約，精選勝率最高的 5 個訊號"""
    all_assets = get_all_okx_swap_assets()
    all_signals = []

    # 展開成 (asset, tf) 任務列表
    tasks = [(asset, tf) for asset in all_assets for tf in ["1h", "4h"]]
    total = len(tasks)
    completed = 0
    lock = threading.Lock()

    scan_start = time.time()
    print(f"⏱️ 掃描開始時間：{datetime.datetime.now().strftime('%H:%M:%S')}（並行模式，共 {total} 項任務）")

    def scan_task(asset, tf):
        return fetch_candle_sync(asset, tf)

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

    # 🎯 核心過濾：根據量化勝率評分（score）從大到小排序，只取前 5 名
    all_signals.sort(key=lambda x: x['score'], reverse=True)
    top_5_signals = all_signals[:5]

    print(f"📊 掃描結果：共找到 {len(all_signals)} 組信號，精選前 {len(top_5_signals)} 名")
    return top_5_signals

# ==================== 🚀 4. 動態精度渲染發送引擎 ====================
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
    html_message += f"🔥 <b>戰術核心</b>：<code>MA8/EMA89交叉 ＋ RSI勝率核心精選 5 🚀</code>\n"
    html_message += f"⏰ <b>監控時間</b>：<code>{now_str}</code> (加州太平洋時間 PT)\n"
    html_message += "───────────────────────\n\n"
    html_message += f"📋 <b>當前全網勝率最高【精選 5 強方案】：</b>\n\n"

    for idx, item in enumerate(valid_signals, 1):
        html_message += f"🔥 <b>{idx}. {item['asset']} ({item['dir']}) {item['leverage']} 【{item['order_type']} | {item['tf']}】</b>\n"
        html_message += f"   • 📊 <b>進場策略判定</b>：<code>{item['entry_type']}</code>\n"
        html_message += f"   • 📝 <b>建議進場點位</b>：<code>{format_price(item['entry'])}</code>\n"
        html_message += f"   • 🟢 <b>止盈 1 (平倉 50%)</b>：<code>{format_price(item['tp1'])}</code>\n"
        html_message += f"   • 🔵 <b>止盈 2 (平倉 30%)</b>：<code>{format_price(item['tp2'])}</code>\n"
        html_message += f"   • 🟣 <b>止盈 3 (平倉 20%)</b>：<code>{format_price(item['tp3'])}</code>\n"
        html_message += f"   • 🔴 <b>硬防守止損位置</b>：<code>{format_price(item['sl'])}</code>\n\n"

    html_message += "───────────────────────\n💡 <i>提示：所有數字皆已加上代碼塊，手機上「輕點數字」即可自動複製。</i>"

    text_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    resp = requests.post(text_url, json={"chat_id": str(target_chat_id), "text": html_message, "parse_mode": "HTML"})
    result = resp.json()
    if result.get("ok"):
        print(f"✅ 精選 5 強報告發送成功 → chat_id={target_chat_id}")
    else:
        print(f"❌ 報告發送失敗：{result}")

# ==================== 📡 5. 原生無衝突監聽引擎 ====================
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
    print("🟢 最初代原生監聽引擎已滿血回歸！正在安全監聽指令...")
    offset = None
    la_tz = pytz.timezone('America/Los_Angeles')
    last_reported_hour = -1

    while True:
        try:
            # A. 定時播報判定
            now_la = datetime.datetime.now(la_tz)
            if now_la.hour in SCHEDULE_HOURS and now_la.minute == 0 and now_la.hour != last_reported_hour:
                print(f"🔔 觸發加州整點定時播報：{now_la.hour}:00")
                t = threading.Thread(target=scan_worker_thread, args=("設定節點定時速報", TELEGRAM_CHAT_ID))
                t.daemon = True
                t.start()
                last_reported_hour = now_la.hour

            # B. 手動口令拉取
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
                            requests.post(confirm_url, json={"chat_id": chat_id, "text": "⚡ 收到長短線全網交叉指令！正在為您挑選勝率最高的 5 種標的，請稍候約 15 秒..."})

                            t = threading.Thread(target=scan_worker_thread, args=("手動現場突擊播報", chat_id))
                            t.daemon = True
                            t.start()

        except Exception as e:
            print(f"⚠️ 監聽異常: {e}")
            time.sleep(1)
            continue
        time.sleep(1)

if __name__ == '__main__':
    print("🤖 幣圈分析師【Replit專屬·勝率精選 5 幣版】雷達正在開機...")
    handle_telegram_updates()
