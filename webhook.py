from flask import Flask, request
import os
import sqlite3

app = Flask(__name__)

DB_PATH = "wallet_history.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS wallet_token_history (
            wallet TEXT,
            token_mint TEXT,
            first_seen_at TEXT,
            buy_count INTEGER,
            PRIMARY KEY (wallet, token_mint)
        )
    """)
    conn.commit()
    conn.close()

init_db()

TRACKED_WALLETS = {
    "AfHNjAnXJKkQ4yrBDop77A3UaLZgFmGKhaSDZC4Msrvk",
}

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
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "SELECT buy_count FROM wallet_token_history WHERE wallet=? AND token_mint=?",
        (wallet, mint)
    )
    row = c.fetchone()

    if row is None:
        c.execute(
            "INSERT INTO wallet_token_history (wallet, token_mint, first_seen_at, buy_count) VALUES (?, ?, datetime('now'), 1)",
            (wallet, mint)
        )
        conn.commit()
        print(f"🟢 FIRST BUY DETECTED: wallet={wallet} token={mint}")
    else:
        c.execute(
            "UPDATE wallet_token_history SET buy_count = buy_count + 1 WHERE wallet=? AND token_mint=?",
            (wallet, mint)
        )
        conn.commit()
        print(f"🔁 Repeat buy (DCA), skipping alert: wallet={wallet} token={mint}, total buys={row[0]+1}")

    conn.close()

@app.route("/")
def home():
    return "Bot is alive!"

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
