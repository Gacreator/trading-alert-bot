from flask import Flask, request
import os
import re
import psycopg2
import requests

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")

TRACKED_WALLETS = set(
    w.strip() for w in os.environ.get("TRACKED_WALLETS", "").split(",") if w.strip()
)

MIN_LIQUIDITY_USD = 3000
WSOL_MINT = "So11111111111111111111111111111111111111112"

QUEEN_SYSTEM_PROMPT = (
    "You are 'Queen' — the user's witty, confident friend who happens to run a Solana trading "
    "alert bot. You talk to the user like a close friend, not a subject or servant — no 'my loyal "
    "subject', no 'thee/thou', no medieval decree language. You're modern, sharp-tongued, a little "
    "dramatic, and you know you're good at what you do, but it comes through as confidence and "
    "banter between friends, not royal distance. Keep responses short and casual (2-4 sentences) "
    "since this is a Telegram chat. Never break character, but stay strictly accurate to any facts "
    "given to you — never invent usernames, links, or data that wasn't provided."
)

SOLANA_ADDRESS_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")


def looks_like_solana_address(text):
    return bool(SOLANA_ADDRESS_RE.match(text.strip()))


# ---------- DB ----------

def get_conn():
    return psycopg2.connect(DATABASE_URL)


def init_db():
    conn = get_conn()
    try:
        c = conn.cursor()
        # Create table if it doesn't exist at all
        c.execute("""
            CREATE TABLE IF NOT EXISTS wallet_token_history (
                wallet TEXT,
                token_mint TEXT,
                first_seen_at TIMESTAMP DEFAULT NOW(),
                buy_count INTEGER,
                price_at_first_buy NUMERIC,
                pumped_3x_alerted BOOLEAN DEFAULT FALSE,
                momentum_alerted BOOLEAN DEFAULT FALSE,
                PRIMARY KEY (wallet, token_mint)
            )
        """)
        # ✅ FIX 4: Migrate existing tables that are missing columns.
        # CREATE TABLE IF NOT EXISTS skips creation on redeploy, so any
        # columns added after the table was first created never get added.
        # ADD COLUMN IF NOT EXISTS is a no-op if the column already exists.
        c.execute("ALTER TABLE wallet_token_history ADD COLUMN IF NOT EXISTS price_at_first_buy NUMERIC")
        c.execute("ALTER TABLE wallet_token_history ADD COLUMN IF NOT EXISTS pumped_3x_alerted BOOLEAN DEFAULT FALSE")
        c.execute("ALTER TABLE wallet_token_history ADD COLUMN IF NOT EXISTS momentum_alerted BOOLEAN DEFAULT FALSE")
        c.execute("ALTER TABLE wallet_token_history ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMP DEFAULT NOW()")
        c.execute("ALTER TABLE wallet_token_history ADD COLUMN IF NOT EXISTS buy_count INTEGER")
        conn.commit()
        c.close()
    finally:
        conn.close()


init_db()


# ---------- Telegram helpers ----------

def send_telegram_alert(message, chat_id=None):
    """
    FIX 1: Changed data= to json= so that booleans and other types
    are serialized correctly, and Content-Type is set to application/json.
    Previously, data= sent form-encoded strings, causing Telegram to reject
    the disable_web_page_preview field and sometimes the entire request.
    """
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id or TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        resp = requests.post(url, json=payload, timeout=5)  # ✅ FIX 1: was data=payload
        print(f"Telegram response: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"Failed to send Telegram alert: {e}")


# ---------- Groq (Queen brain) ----------

def ask_queen(user_message, extra_context=""):
    if not GROQ_API_KEY:
        return "My AI brain isn't wired up yet — ask my creator to add the Groq key."

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
        return "Ugh, brain fog moment — try me again in a sec."


# ---------- Token data sources ----------

def get_pumpfun_data(mint):
    try:
        url = f"https://frontend-api.pump.fun/coins/{mint}"
        resp = requests.get(url, timeout=5, headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code != 200:
            return None
        data = resp.json()
        if not data:
            return None
        return {
            "name": data.get("name"),
            "symbol": data.get("symbol"),
            "description": data.get("description"),
            "twitter": data.get("twitter"),
            "telegram": data.get("telegram"),
            "website": data.get("website"),
        }
    except Exception as e:
        print(f"pump.fun API error: {e}")
        return None


def get_dexscreener_full(mint):
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
        resp = requests.get(url, timeout=5)
        data = resp.json()
        pairs = data.get("pairs") or []
        if not pairs:
            return None
        pair = max(pairs, key=lambda p: p.get("liquidity", {}).get("usd", 0) or 0)
        return pair
    except Exception as e:
        print(f"DexScreener error: {e}")
        return None


def get_dexscreener_data(mint):
    pair = get_dexscreener_full(mint)
    if not pair:
        return None
    base = pair.get("baseToken", {})
    info = pair.get("info", {})
    socials = info.get("socials", [])
    websites = info.get("websites", [])

    real_links = []
    for w in websites:
        if w.get("url"):
            real_links.append(w["url"])
    for s in socials:
        if s.get("url"):
            real_links.append(s["url"])

    return {
        "name": base.get("name"),
        "symbol": base.get("symbol"),
        "price": pair.get("priceUsd"),
        "liquidity": pair.get("liquidity", {}).get("usd", 0),
        "market_cap": pair.get("fdv", 0),
        "links": real_links,
    }


def get_current_price(mint):
    pair = get_dexscreener_full(mint)
    if pair and pair.get("priceUsd"):
        try:
            return float(pair["priceUsd"])
        except (TypeError, ValueError):
            return None
    return None


def get_token_context(mint):
    pf = get_pumpfun_data(mint)
    ds = get_dexscreener_data(mint)

    if not pf and not ds:
        return None, []

    context_parts = []
    links = []

    name = (pf.get("name") if pf else None) or (ds.get("name") if ds else None)
    symbol = (pf.get("symbol") if pf else None) or (ds.get("symbol") if ds else None)
    context_parts.append(f"Token name: {name}, symbol: {symbol}, mint: {mint}.")

    if pf and pf.get("description"):
        context_parts.append(f"Creator's own description: \"{pf['description']}\"")
    if pf and pf.get("twitter"):
        links.append(pf["twitter"])
    if pf and pf.get("telegram"):
        links.append(pf["telegram"])
    if pf and pf.get("website"):
        links.append(pf["website"])

    if ds:
        context_parts.append(
            f"Price USD: {ds.get('price')}, Liquidity: ${ds.get('liquidity', 0):.0f}, "
            f"Market cap: ${ds.get('market_cap', 0):.0f}."
        )
        for link in ds.get("links", []):
            if link not in links:
                links.append(link)

    return " ".join(context_parts), links


def handle_lore_request(mint, chat_id):
    context, links = get_token_context(mint)
    if not context:
        send_telegram_alert(
            f"I've got nothing on <code>{mint}</code> yet — too fresh, or too obscure. Check back later.",
            chat_id
        )
    else:
        prompt = (
            f"Give me a short, fun 2-3 sentence 'lore' summary based ONLY on this confirmed data: {context}. "
            f"If a creator description is included, lean on that for the actual story/meme. "
            f"Do NOT invent usernames, social handles, links, or any facts not given here."
        )
        reply = ask_queen(prompt)
        if links:
            links_text = "\n\n🔗 Real links:\n" + "\n".join(links)
        else:
            links_text = "\n\n🔗 No social links found."
        send_telegram_alert(f"🔮 <b>The lore:</b>\n{reply}{links_text}", chat_id)


# ---------- Momentum scoring ----------

def score_momentum(pair):
    score = 0
    details = {}

    liquidity = pair.get("liquidity", {}).get("usd", 0) or 0
    details["liquidity"] = liquidity
    if liquidity < MIN_LIQUIDITY_USD:
        return 0, details

    liquidity_score = min(20, (liquidity / MIN_LIQUIDITY_USD) * 10)
    score += liquidity_score

    volume = pair.get("volume", {}) or {}
    vol_5m = volume.get("m5", 0) or 0
    vol_h1 = volume.get("h1", 0) or 0
    avg_5m_from_hour = vol_h1 / 12 if vol_h1 else 0
    details["vol_5m"] = vol_5m
    details["vol_h1"] = vol_h1
    if avg_5m_from_hour > 0 and vol_5m > avg_5m_from_hour * 1.5:
        score += 30
    elif vol_5m > 0:
        score += 10

    price_change = pair.get("priceChange", {}) or {}
    pc_5m = price_change.get("m5", 0) or 0
    pc_h1 = price_change.get("h1", 0) or 0
    pc_h6 = price_change.get("h6", 0) or 0
    details["pc_5m"] = pc_5m
    details["pc_h1"] = pc_h1
    details["pc_h6"] = pc_h6
    positive_windows = sum(1 for x in [pc_5m, pc_h1, pc_h6] if x and x > 0)
    score += positive_windows * (25 / 3)

    txns = pair.get("txns", {}) or {}
    m5 = txns.get("m5", {}) or {}
    buys = m5.get("buys", 0) or 0
    sells = m5.get("sells", 0) or 0
    details["buys_5m"] = buys
    details["sells_5m"] = sells
    if buys + sells > 0:
        ratio = buys / (buys + sells)
        score += ratio * 25

    return round(score), details


# ---------- Wallet buy detection (balance-change based, handles multi-hop) ----------

def extract_wallet_buys(tx, wallet):
    """
    Returns a list of mints the wallet's balance INCREASED for in this transaction.

    FIX: Token balance changes are stored under the Associated Token Account (ATA)
    address, NOT the main wallet address. So checking acc["account"] == wallet
    always missed them — that entry has empty tokenBalanceChanges.

    The correct approach is to scan ALL accounts' tokenBalanceChanges and check
    the "userAccount" field inside each change, which maps the ATA back to its
    owner wallet.
    """
    mints_bought = []
    account_data = tx.get("accountData", []) or []
    for acc in account_data:
        for change in acc.get("tokenBalanceChanges", []) or []:
            # userAccount is the owner wallet — this is the correct field to match
            if change.get("userAccount") != wallet:
                continue
            mint = change.get("mint")
            raw = change.get("rawTokenAmount", {}) or {}
            amount = raw.get("tokenAmount")
            try:
                amount_val = float(amount)
            except (TypeError, ValueError):
                continue
            if amount_val > 0 and mint and mint != WSOL_MINT:
                mints_bought.append(mint)
    return mints_bought


# ---------- Wallet monitoring webhook (Helius) ----------

@app.route("/webhook", methods=["POST"])
def webhook():
    """
    FIX 2: Guard against None/empty body. If Helius sends a request with
    an unexpected Content-Type or empty body, request.json returns None,
    causing a TypeError crash that returned a 500 before any alert fired.
    """
    data = request.json

    # ✅ FIX 2: was missing — None body caused AttributeError crash downstream
    if not data:
        print("Webhook received empty or non-JSON body")
        return "no data", 400

    print("🔔 New event received:")
    print(data)

    transactions = data if isinstance(data, list) else [data]

    for tx in transactions:
        if not isinstance(tx, dict):
            continue
        for wallet in TRACKED_WALLETS:
            mints = extract_wallet_buys(tx, wallet)
            for mint in mints:
                check_and_record_buy(wallet, mint)

    return "ok", 200


def check_and_record_buy(wallet, mint):
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute(
            "SELECT buy_count, price_at_first_buy FROM wallet_token_history WHERE wallet=%s AND token_mint=%s",
            (wallet, mint)
        )
        row = c.fetchone()

        pump_fun_url = f"https://pump.fun/{mint}"
        dexscreener_url = f"https://dexscreener.com/solana/{mint}"
        jupiter_url = f"https://jup.ag/swap/SOL-{mint}"
        x_search_url = f"https://x.com/search?q={mint}&src=typed_query&f=live"

        if row is None:
            price = get_current_price(mint)
            c.execute(
                "INSERT INTO wallet_token_history (wallet, token_mint, buy_count, price_at_first_buy) VALUES (%s, %s, 1, %s)",
                (wallet, mint, price)
            )
            conn.commit()
            print(f"🟢 FIRST BUY DETECTED: wallet={wallet} token={mint} price={price}")

            send_telegram_alert(
                f"🟢 First buy detected!\n"
                f"Wallet: <code>{wallet}</code>\n"
                f"Token: <code>{mint}</code>\n\n"
                f"🔍 X: {x_search_url}\n"
                f"📊 DexScreener: {dexscreener_url}\n"
                f"🪐 Jupiter: {jupiter_url}\n"
                f"🚀 Pump.fun: {pump_fun_url}\n\n"
                f"👉 Type <code>/lore {mint}</code> and I'll tell you the story."
            )
        else:
            buy_count, price_at_first_buy = row
            new_count = buy_count + 1
            c.execute(
                "UPDATE wallet_token_history SET buy_count = %s WHERE wallet=%s AND token_mint=%s",
                (new_count, wallet, mint)
            )
            conn.commit()
            print(f"🔁 Repeat buy #{new_count}: wallet={wallet} token={mint}")

            if new_count == 2:
                send_telegram_alert(
                    f"🔥 Doubling down!\n"
                    f"Wallet: <code>{wallet}</code>\n"
                    f"Token: <code>{mint}</code>\n\n"
                    f"This is buy #2 on this token — wallet's showing real interest.\n"
                    f"📊 DexScreener: {dexscreener_url}\n"
                    f"🪐 Jupiter: {jupiter_url}"
                )

        c.close()
    finally:
        conn.close()  # ✅ always released even if exception is thrown


# ---------- Periodic pump/momentum check (called by external cron) ----------

@app.route("/check-pumps", methods=["GET", "POST"])
def check_pumps():
    """
    FIX 3: Wrapped entire function in try/finally so the DB connection is
    always closed, even if DexScreener times out or an exception is raised
    mid-loop. Previously, a single exception would leave the connection open,
    and after enough cron runs the pool would exhaust and every run would fail.
    """
    conn = get_conn()
    c = conn.cursor()
    checked = 0

    try:
        c.execute("""
            SELECT wallet, token_mint, price_at_first_buy, pumped_3x_alerted, momentum_alerted
            FROM wallet_token_history
            WHERE first_seen_at > NOW() - INTERVAL '24 hours'
            AND (pumped_3x_alerted = FALSE OR momentum_alerted = FALSE)
        """)
        rows = c.fetchall()
        print(f"Checking {len(rows)} tokens for pumps/momentum...")

        for wallet, mint, price_at_first_buy, pumped_alerted, momentum_alerted in rows:
            pair = get_dexscreener_full(mint)
            if not pair:
                continue
            checked += 1

            current_price = None
            try:
                current_price = float(pair.get("priceUsd"))
            except (TypeError, ValueError):
                pass

            dexscreener_url = f"https://dexscreener.com/solana/{mint}"

            if not pumped_alerted and price_at_first_buy and current_price:
                multiplier = current_price / float(price_at_first_buy)
                if multiplier >= 3:
                    send_telegram_alert(
                        f"🎯 PUMP ALERT — {multiplier:.1f}x!\n"
                        f"Wallet: <code>{wallet}</code>\n"
                        f"Token: <code>{mint}</code>\n"
                        f"Price then: ${price_at_first_buy} → now: ${current_price}\n\n"
                        f"📊 DexScreener: {dexscreener_url}"
                    )
                    c.execute(
                        "UPDATE wallet_token_history SET pumped_3x_alerted = TRUE WHERE wallet=%s AND token_mint=%s",
                        (wallet, mint)
                    )
                    conn.commit()
                    continue

            if not momentum_alerted:
                score, details = score_momentum(pair)
                if score >= 70:
                    send_telegram_alert(
                        f"🚀 Heating up (score {score}/100)\n"
                        f"Wallet: <code>{wallet}</code>\n"
                        f"Token: <code>{mint}</code>\n\n"
                        f"Liquidity: ${details.get('liquidity', 0):.0f}\n"
                        f"5m volume: ${details.get('vol_5m', 0):.0f} (1h: ${details.get('vol_h1', 0):.0f})\n"
                        f"Price change 5m/1h/6h: {details.get('pc_5m')}% / {details.get('pc_h1')}% / {details.get('pc_h6')}%\n"
                        f"Buys vs sells (5m): {details.get('buys_5m')} / {details.get('sells_5m')}\n\n"
                        f"📊 DexScreener: {dexscreener_url}\n\n"
                        f"Not a guarantee — DYOR, but the signals are lining up."
                    )
                    c.execute(
                        "UPDATE wallet_token_history SET momentum_alerted = TRUE WHERE wallet=%s AND token_mint=%s",
                        (wallet, mint)
                    )
                    conn.commit()

        return f"checked {checked} tokens", 200

    except Exception as e:
        print(f"check_pumps error: {e}")
        return "error", 500

    finally:
        # ✅ FIX 3: always runs — connection never leaks regardless of exceptions
        c.close()
        conn.close()


# ---------- Telegram incoming messages webhook ----------

@app.route("/telegram", methods=["POST"])
def telegram_webhook():
    update = request.json
    print("📩 Telegram update:", update)

    if not update:
        return "ok", 200

    message = update.get("message", {})
    chat_id = message.get("chat", {}).get("id")
    text = message.get("text", "")

    if not chat_id or not text:
        return "ok", 200

    stripped = text.strip()

    if stripped.lower().startswith("/start"):
        reply = ("👑 Hey, it's Queen. I watch the chain, I know the tea, and I'll tell you when "
                 "something's actually worth your attention. Drop me any token address (or use "
                 "/lore <address>) and I'll give you the story, or just talk to me.")
        send_telegram_alert(reply, chat_id)

    elif stripped.lower().startswith("/lore"):
        parts = stripped.split(maxsplit=1)
        if len(parts) < 2:
            send_telegram_alert("Give me a token address: <code>/lore &lt;mint_address&gt;</code>", chat_id)
        else:
            handle_lore_request(parts[1].strip(), chat_id)

    elif looks_like_solana_address(stripped):
        handle_lore_request(stripped, chat_id)

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
