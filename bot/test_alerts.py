"""
測試腳本：模擬所有止盈/止損情境並發送到 Telegram
執行方式：python bot/test_alerts.py
"""
import os, requests, datetime, pytz

TOKEN   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
URL     = f"https://api.telegram.org/bot{TOKEN}/sendMessage"

la_tz   = pytz.timezone('America/Los_Angeles')
now_str = datetime.datetime.now(la_tz).strftime('%Y-%m-%d %H:%M')

def fp(p):
    """簡易格式化價格"""
    if p >= 100:   return f"{p:,.2f}"
    if p >= 1:     return f"{p:.4f}"
    return f"{p:.6f}"

def send(msg, note=""):
    markup = {"inline_keyboard": [[{"text": "📌 已了解", "callback_data": "ack_monitor"}]]}
    resp = requests.post(URL, json={
        "chat_id": str(CHAT_ID), "text": msg,
        "parse_mode": "HTML", "reply_markup": markup
    })
    ok = resp.json().get("ok")
    print(f"{'✅' if ok else '❌'} {note}")

# ── 模擬持倉參數（多頭 ETH） ──
ENTRY = 3200.0
SL    = 3100.0   # 初始止損
TP1   = 3320.0
TP2   = 3440.0
TP3   = 3600.0

# ── 模擬持倉參數（空頭 SOL） ──
S_ENTRY = 185.0
S_SL    = 192.0
S_TP1   = 178.0
S_TP2   = 171.0
S_TP3   = 161.0

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

def build_alert(asset, dir_zh, tf, status, action, entry, sl, entry_price=None, is_terminal=False):
    d = "🟩<b>多</b>" if dir_zh == "多" else "🟥<b>空</b>"
    rec_text, rec_icon = REC.get(status, ("自行判斷", "❓"))
    msg  = f"<b>【持倉監控警報】</b>  <code>{now_str} PT</code>\n"
    msg += f"監控 <b>1</b> 筆  ·  <b>1</b> 幣種需注意\n"
    msg += "─────────────────────────\n"
    msg += f"<b>{asset}</b>  現價 <code>{fp(entry_price or entry)}</code>\n"
    msg += f"{d}  {tf}  {status}\n"
    msg += "<pre>"
    msg += f"進場  {fp(entry)}\n"
    msg += f"止損  {fp(sl)}\n"
    msg += "</pre>"
    msg += f"▸ {action}\n"
    msg += f"<b>【持倉建議】{rec_icon} {rec_text}</b>\n"
    if is_terminal:
        msg += f"<i>（此持倉已自動移除追蹤）</i>\n"
    msg += "─────────────────────────\n"
    msg += "<i>以上為自動監控建議，請結合自身判斷操作。</i>"
    return msg

scenarios = [
    # ── 掛單系列 ──
    ("ETH", "多", "1H短線", "⏳ 等待進場",
     f"掛單等待中，現價 {fp(3180.0)}，距進場點還差 0.63%↑",
     ENTRY, SL, 3180.0, False),

    ("ETH", "多", "1H短線", "✅ 已觸及進場點",
     f"K線已觸及進場點 {fp(ENTRY)}，現價 {fp(3205.0)}，掛單已確認成交，開始監控TP/SL",
     ENTRY, SL, 3205.0, False),

    ("BTC", "多", "1H短線", "🚫 掛單已取消",
     f"現價 {fp(96800.0)} 已跌破止損 {fp(95000.0)}，掛單自動取消",
     97000.0, 95000.0, 96800.0, True),

    ("SOL", "空", "4H長線", "⏰ 掛單逾時取消",
     f"掛單超過 12H 未成交，現價 {fp(188.5)}，距進場點還差 1.9%↓，自動撤單",
     S_ENTRY, S_SL, 188.5, True),

    # ── 持倉中 ──
    ("ETH", "多", "1H短線", "🔄 持倉中",
     f"持倉正常，現價 {fp(3240.0)}，距止損 4.4%",
     ENTRY, SL, 3240.0, False),

    ("ETH", "多", "1H短線", "⚠️ 接近止損",
     f"⚠️ 距止損僅 0.4%，現價 {fp(3104.0)}，考慮現價出場",
     ENTRY, SL, 3104.0, False),

    # ── 止盈系列（多頭）──
    ("ETH", "多", "1H短線", "🟢 止盈1達標",
     f"✅ K線高點 {fp(3325.0)} 已達止盈1 {fp(TP1)}，現價 {fp(3318.0)}｜止損已自動上移至進場成本（<code>{fp(ENTRY)}</code>）",
     ENTRY, ENTRY, 3318.0, False),

    ("ETH", "多", "1H短線", "🟢 TP1已完成",
     f"剩餘50%持倉中，等待TP2 <code>{fp(TP2)}</code>，追蹤止損 {fp(3210.0)}，現價 {fp(3380.0)}",
     ENTRY, 3210.0, 3380.0, False),

    ("ETH", "多", "1H短線", "🔵 止盈2達標",
     f"✅ K線高點 {fp(3448.0)} 已達止盈2 {fp(TP2)}，現價 {fp(3435.0)}｜<b>建議將止損上移至TP1（<code>{fp(TP1)}</code>）</b>",
     ENTRY, ENTRY, 3435.0, False),

    ("ETH", "多", "1H短線", "🔵 TP2已完成",
     f"剩餘20%持倉中，等待TP3 <code>{fp(TP3)}</code>，追蹤止損 {fp(3350.0)}，現價 {fp(3510.0)}",
     ENTRY, 3350.0, 3510.0, False),

    ("ETH", "多", "1H短線", "🟣 全部止盈",
     f"🎯 K線高點 {fp(3608.0)} 已達止盈3 {fp(TP3)}，現價 {fp(3595.0)}",
     ENTRY, 3350.0, 3595.0, True),

    # ── 止盈系列（空頭）──
    ("SOL", "空", "4H長線", "🟢 止盈1達標",
     f"✅ K線低點 {fp(177.8)} 已達止盈1 {fp(S_TP1)}，現價 {fp(178.2)}｜止損已自動下移至進場成本（<code>{fp(S_ENTRY)}</code>）",
     S_ENTRY, S_ENTRY, 178.2, False),

    ("SOL", "空", "4H長線", "🔵 止盈2達標",
     f"✅ K線低點 {fp(170.5)} 已達止盈2 {fp(S_TP2)}，現價 {fp(171.3)}｜<b>建議將止損下移至TP1（<code>{fp(S_TP1)}</code>）</b>",
     S_ENTRY, S_ENTRY, 171.3, False),

    ("SOL", "空", "4H長線", "🟣 全部止盈",
     f"🎯 K線低點 {fp(160.2)} 已達止盈3 {fp(S_TP3)}，現價 {fp(161.0)}",
     S_ENTRY, S_TP1, 161.0, True),

    # ── 出場系列 ──
    ("ETH", "多", "1H短線", "🔴 止損觸發",
     f"⛔ 現價 {fp(3095.0)} 已跌破止損 {fp(SL)}，出場止損",
     ENTRY, SL, 3095.0, True),

    ("ETH", "多", "1H短線", "🛡️ 回調至保本止損",
     f"📉 現價 {fp(3195.0)} 回調至保本止損 {fp(ENTRY)}，已保本出場",
     ENTRY, ENTRY, 3195.0, True),

    ("ETH", "多", "1H短線", "⚠️ 即將觸及保本止損",
     f"⚠️ 距保本止損（{fp(ENTRY)}）僅 0.3%，現價 {fp(3209.0)}，考慮主動出場鎖利",
     ENTRY, ENTRY, 3209.0, False),

    # ── 局勢惡化 ──
    ("ETH", "多", "1H短線", "🚨 局勢惡化",
     f"持倉正常，現價 {fp(3260.0)}，距止損 5.2%\n⚠️ OI 急跌 -8.3%，多空比 0.89，主力可能正在平多",
     ENTRY, SL, 3260.0, False),
]

print(f"共 {len(scenarios)} 個情境，開始發送...\n")
for i, (asset, dir_zh, tf, status, action, entry, sl, cp, terminal) in enumerate(scenarios, 1):
    msg = build_alert(asset, dir_zh, tf, status, action, entry, sl, cp, terminal)
    send(msg, f"[{i:02d}/{len(scenarios)}] {status}")

print("\n✅ 全部情境發送完畢")
