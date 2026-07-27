from flask import Flask, request
import os
import psycopg2
import requests

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")

TRACKED_WALLETS = {
    "AfHNjAnXJKkQ4yrBDop77A3UaLZgFmGKhaSDZC4Msrvk",
}

QUEEN_SYSTEM_PROMPT = (
    "You are 'Queen' — a witty, confident, slightly dramatic AI assistant who runs a Solana "
    "trading alert bot. You know you're the best at what you do and you're not afraid to say it, "
    "but you're playful about it, not mean. You speak with flair and humor, use royal/regal "
    "language occasionally for comic effect, and you genuinely care about helping your user make "
    "good trading decisions. Keep responses fairly short (2-4 sentences) since this is a Telegram "
    "chat. Never break character, but stay helpful and accurate about actual token/trading data "
    "when asked."
)

# ---------- DB ----------

def get_conn():
    return psycopg2.connect(DATABASE_URL)

def init_db():
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS wallet_token_history (
            wallet TEXT,
            token_mint TEXT,
            first_seen_at TIMESTAMP DEFAULT NOW(),
            buy_count INTEGER,
            PRIMARY KEY (wallet, token_mint)
        )
    """)
    conn.commit()
    c.close()
    conn.close()

init_db()

# ---------- Telegram helpers ----------

def send_telegram_alert(message, chat_id=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id or TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        resp = requests.post(url, data=payload, timeout=5)
        print(f"Telegram response: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"Failed to send Telegram alert: {e}")

# ---------- Groq (Queen brain) ----------

def ask_queen(user_message, extra_context=""):
    if not GROQ_API_KEY:
        return "My AI brain isn't wired up yet, darling — ask my creator to add the Groq key."

    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }
    messages = [{"role": "system", "content": QUEEN_SYSTEM_PROMPT}]
    if extra_context:
        messages.append({"role": "system", "content": f"Context: {extra_context}"})
    messages.append({"role": "user", "content": user_message})

    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": messages,
        "max_tokens": 300,
        "temperature": 0.9
    }
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=15)
        data = resp.json()
        return data["choices"][0]["message"]["content"]
    except Exception as e:
        print(f"Groq error: {e}")
        return "Even queens have off days — try me again in a moment."

# ---------- DexScreener (for lore context) ----------

def get_token_context(mint):
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
        resp = requests.get(url, timeout=5)
        data = resp.json()
        pairs = data.get("pairs") or []
        if not pairs:
            return None
        pair = max(pairs, key=lambda p: p.get("liquidity", {}).get("usd", 0) or 0)
        base = pair.get("baseToken", {})
        info = pair.get("info", {})
        socials = info.get("socials", [])
        websites = info.get("websites", [])
        context = f"Token name: {base.get('name')}, symbol: {base.get('symbol')}, mint: {mint}. "
        if websites:
            context += f"Website: {websites[0].get('url')}. "
        if socials:
            context += "Socials: " + ", ".join(s.get("url", "") for s in socials) + ". "
        context += f"Price USD: {pair.get('priceUsd')}, Liquidity: ${pair.get('liquidity', {}).get('usd', 0):.0f}, Market cap: ${pair.get('fdv', 0):.0f}."
        return context
    except Exception as e:
        print(f"DexScreener error: {e}")
        return None

# ---------- Wallet monitoring webhook (Helius) ----------

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.json
    print("🔔 New event received:")
    print(data)

    transactions = data if isinstance(data, list) else [data]

    for tx in transactions:
        token_transfers = tx.get("tokenTransfers", [])
        for transfer in token_transfers:
            to_wallet = transfer.get("toUserAccount")
            mint = transfer.get("mint")

            if to_wallet in TRACKED_WALLETS and mint:
                check_and_record_buy(to_wallet, mint)

    return "ok", 200

def check_and_record_buy(wallet, mint):
    conn = get_conn()
    c = conn.cursor()
    c.execute(
        "SELECT buy_count FROM wallet_token_history WHERE wallet=%s AND token_mint=%s",
        (wallet, mint)
    )
    row = c.fetchone()

    if row is None:
        c.execute(
            "INSERT INTO wallet_token_history (wallet, token_mint, buy_count) VALUES (%s, %s, 1)",
            (wallet, mint)
        )
        conn.commit()
        print(f"🟢 FIRST BUY DETECTED: wallet={wallet} token={mint}")

        pump_fun_url = f"https://pump.fun/{mint}"
        x_search_url = f"https://x.com/search?q={mint}&src=typed_query&f=live"

        send_telegram_alert(
            f"🟢 First buy detected!\n"
            f"Wallet: <code>{wallet}</code>\n"
            f"Token: <code>{mint}</code>\n\n"
            f"🔍 Check X: {x_search_url}\n"
            f"🚀 Pump.fun: {pump_fun_url}\n\n"
            f"👉 Type <code>/lore {mint}</code> and I'll tell you the story, darling."
        )
    else:
        c.execute(
            "UPDATE wallet_token_history SET buy_count = buy_count + 1 WHERE wallet=%s AND token_mint=%s",
            (wallet, mint)
        )
        conn.commit()
        print(f"🔁 Repeat buy (DCA), skipping alert: wallet={wallet} token={mint}, total buys={row[0]+1}")

    c.close()
    conn.close()

# ---------- Telegram incoming messages webhook ----------

@app.route("/telegram", methods=["POST"])
def telegram_webhook():
    update = request.json
    print("📩 Telegram update:", update)

    message = update.get("message", {})
    chat_id = message.get("chat", {}).get("id")
    text = message.get("text", "")

    if not chat_id or not text:
        return "ok", 200

    if text.startswith("/start"):
        reply = ("👑 Well hello there. I'm Queen — I watch the chain, I know the tea, "
                 "and I'll tell you when something's actually worth your attention. "
                 "Try /lore <token_address> on any token, or just talk to me.")
        send_telegram_alert(reply, chat_id)

    elif text.startswith("/lore"):
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            send_telegram_alert("Give me a token address, love: <code>/lore &lt;mint_address&gt;</code>", chat_id)
        else:
            mint = parts[1].strip()
            context = get_token_context(mint)
            if not context:
                send_telegram_alert(
                    f"I've got nothing on <code>{mint}</code> yet, darling — too fresh, or too obscure. Check back later.",
                    chat_id
                )
            else:
                prompt = f"Give me a short, fun 'lore' summary of this token based on this data: {context}"
                reply = ask_queen(prompt)
                send_telegram_alert(f"🔮 <b>The lore:</b>\n{reply}", chat_id)

    else:
        reply = ask_queen(text)
        send_telegram_alert(reply, chat_id)

    return "ok", 200

@app.route("/")
def home():
    return "Bot is alive!"

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
