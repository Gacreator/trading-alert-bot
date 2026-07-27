from flask import Flask, request
import os
import psycopg2
import requests

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

TRACKED_WALLETS = {
    "AfHNjAnXJKkQ4yrBDop77A3UaLZgFmGKhaSDZC4Msrvk",
}

def get_conn():
    return psycopg2.connect(DATABASE_URL)

def send_telegram_alert(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    try:
        requests.post(url, data=payload, timeout=5)
    except Exception as e:
        print(f"Failed to send Telegram alert: {e}")

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
            f"Wallet: `{wallet}`\n"
            f"Token: `{mint}`\n\n"
            f"🔍 Check X: {x_search_url}\n"
            f"🚀 Pump.fun: {pump_fun_url}"
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

@app.route("/")
def home():
    return "Bot is alive!"

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
