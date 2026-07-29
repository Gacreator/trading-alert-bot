from flask import Flask, request
import os
import re
import psycopg2
import requests
import threading

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

SCAN_WINDOW_HOURS = int(os.environ.get("SCAN_WINDOW_HOURS", "168"))  # 7 days default

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

_check_pumps_lock = threading.Lock()


def looks_like_solana_address(text):
    return bool(SOLANA_ADDRESS_RE.match(text.strip()))


# ---------- DB ----------

def get_conn():
    return psycopg2.connect(DATABASE_URL)


def init_db():
    conn = get_conn()
    try:
        c = conn.cursor()
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
        c.execute("ALTER TABLE wallet_token_history ADD COLUMN IF NOT EXISTS price_at_first_buy NUMERIC")
        c.execute("ALTER TABLE wallet_token_history ADD COLUMN IF NOT EXISTS pumped_3x_alerted BOOLEAN DEFAULT FALSE")
        c.execute("ALTER TABLE wallet_token_history ADD COLUMN IF NOT EXISTS momentum_alerted BOOLEAN DEFAULT FALSE")
        c.execute("ALTER TABLE wallet_token_history ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMP DEFAULT NOW()")
        c.execute("ALTER TABLE wallet_token_history ADD COLUMN IF NOT EXISTS buy_count INTEGER")

        c.execute("ALTER TABLE wallet_token_history ADD COLUMN IF NOT EXISTS max_price_seen NUMERIC")
        c.execute("ALTER TABLE wallet_token_history ADD COLUMN IF NOT EXISTS max_multiplier_seen NUMERIC")
        c.execute("ALTER TABLE wallet_token_history ADD COLUMN IF NOT EXISTS last_checked_at TIMESTAMP")

        c.execute("ALTER TABLE wallet_token_history ADD COLUMN IF NOT EXISTS min_price_seen NUMERIC")
        c.execute("ALTER TABLE wallet_token_history ADD COLUMN IF NOT EXISTS max_drawdown_seen NUMERIC")
        c.execute("ALTER TABLE wallet_token_history ADD COLUMN IF NOT EXISTS last_liquidity NUMERIC")

        c.execute("""
            CREATE TABLE IF NOT EXISTS token_scan_log (
                id SERIAL PRIMARY KEY,
                wallet TEXT,
                token_mint TEXT,
                scanned_at TIMESTAMP DEFAULT NOW(),
                price NUMERIC,
                liquidity NUMERIC,
                vol_5m NUMERIC,
                vol_h1 NUMERIC,
                pc_5m NUMERIC,
                pc_h1 NUMERIC,
                pc_h6 NUMERIC,
                buys_5m INTEGER,
                sells_5m INTEGER,
                momentum_score NUMERIC,
                multiplier_from_first_buy NUMERIC,
                drawdown_from_first_buy NUMERIC,
                liquidity_delta_pct NUMERIC,
                momentum_alert_fired BOOLEAN DEFAULT FALSE,
                pump_alert_fired BOOLEAN DEFAULT FALSE
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_scan_log_mint ON token_scan_log (token_mint)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_scan_log_scanned_at ON token_scan_log (scanned_at)")
        c.execute("ALTER TABLE token_scan_log ADD COLUMN IF NOT EXISTS drawdown_from_first_buy NUMERIC")
        c.execute("ALTER TABLE token_scan_log ADD COLUMN IF NOT EXISTS liquidity_delta_pct NUMERIC")

        conn.commit()
        c.close()
    finally:
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
        resp = requests.post(url, json=payload, timeout=5)
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


# ---------- Wallet buy detection ----------

def extract_wallet_buys(tx, wallet):
    mints_bought = []
    seen = set()

    for transfer in tx.get("tokenTransfers", []) or []:
        if transfer.get("toUserAccount") != wallet:
            continue
        mint = transfer.get("mint")
        amount = transfer.get("tokenAmount", 0) or 0
        try:
            amount_val = float(amount)
        except (TypeError, ValueError):
            continue
        if amount_val > 0 and mint and mint != WSOL_MINT and mint not in seen:
            mints_bought.append(mint)
            seen.add(mint)

    for acc in tx.get("accountData", []) or []:
        for change in acc.get("tokenBalanceChanges", []) or []:
            if change.get("userAccount") != wallet:
                continue
            mint = change.get("mint")
            raw = change.get("rawTokenAmount", {}) or {}
            amount = raw.get("tokenAmount")
            try:
                amount_val = float(amount)
            except (TypeError, ValueError):
                continue
            if amount_val > 0 and mint and mint != WSOL_MINT and mint not in seen:
                mints_bought.append(mint)
                seen.add(mint)

    return mints_bought


# ---------- Wallet monitoring webhook (Helius) ----------

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.json

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
        conn.close()


# ---------- Periodic pump/momentum check ----------

def run_pump_check():
    conn = get_conn()
    c = conn.cursor()
    checked = 0

    try:
        c.execute("""
            SELECT wallet, token_mint, price_at_first_buy, pumped_3x_alerted, momentum_alerted, last_liquidity
            FROM wallet_token_history
            WHERE first_seen_at > NOW() - (INTERVAL '1 hour' * %s)
            AND (pumped_3x_alerted = FALSE OR momentum_alerted = FALSE)
        """, (SCAN_WINDOW_HOURS,))
        rows = c.fetchall()
        print(f"Checking {len(rows)} tokens for pumps/momentum...")

        for wallet, mint, price_at_first_buy, pumped_alerted, momentum_alerted, prev_liquidity in rows:
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

            score, details = score_momentum(pair)

            multiplier = None
            if price_at_first_buy and current_price:
                try:
                    multiplier = current_price / float(price_at_first_buy)
                except (TypeError, ValueError, ZeroDivisionError):
                    multiplier = None

            drawdown = None
            if price_at_first_buy and current_price:
                try:
                    drawdown = max(0.0, 1 - (current_price / float(price_at_first_buy)))
                except (TypeError, ValueError, ZeroDivisionError):
                    drawdown = None

            current_liquidity = details.get("liquidity")
            liquidity_delta_pct = None
            if current_liquidity is not None and prev_liquidity:
                try:
                    liquidity_delta_pct = (float(current_liquidity) - float(prev_liquidity)) / float(prev_liquidity)
                except (TypeError, ValueError, ZeroDivisionError):
                    liquidity_delta_pct = None

            if current_price is not None:
                c.execute(
                    """
                    UPDATE wallet_token_history
                    SET last_checked_at = NOW(),
                        max_price_seen = GREATEST(COALESCE(max_price_seen, 0), %s),
                        max_multiplier_seen = GREATEST(COALESCE(max_multiplier_seen, 0), COALESCE(%s, 0)),
                        min_price_seen = LEAST(COALESCE(min_price_seen, %s), %s),
                        max_drawdown_seen = GREATEST(COALESCE(max_drawdown_seen, 0), COALESCE(%s, 0)),
                        last_liquidity = COALESCE(%s, last_liquidity)
                    WHERE wallet=%s AND token_mint=%s
                    """,
                    (current_price, multiplier, current_price, current_price,
                     drawdown, current_liquidity, wallet, mint)
                )

            momentum_alert_fired = False
            pump_alert_fired = False

            if not pumped_alerted and multiplier is not None and multiplier >= 3:
                pump_alert_fired = True
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

            elif not momentum_alerted and score >= 70:
                momentum_alert_fired = True
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

            c.execute(
                """
                INSERT INTO token_scan_log
                    (wallet, token_mint, price, liquidity, vol_5m, vol_h1,
                     pc_5m, pc_h1, pc_h6, buys_5m, sells_5m, momentum_score,
                     multiplier_from_first_buy, drawdown_from_first_buy,
                     liquidity_delta_pct, momentum_alert_fired, pump_alert_fired)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    wallet, mint, current_price, details.get("liquidity"),
                    details.get("vol_5m"), details.get("vol_h1"),
                    details.get("pc_5m"), details.get("pc_h1"), details.get("pc_h6"),
                    details.get("buys_5m"), details.get("sells_5m"), score,
                    multiplier, drawdown, liquidity_delta_pct,
                    momentum_alert_fired, pump_alert_fired
                )
            )

            conn.commit()

        print(f"check_pumps finished — checked {checked} tokens")

    except Exception as e:
        print(f"check_pumps error: {e}")

    finally:
        c.close()
        conn.close()
        _check_pumps_lock.release()


@app.route("/check-pumps", methods=["GET", "POST"])
def check_pumps():
    if not _check_pumps_lock.acquire(blocking=False):
        print("check-pumps already running, skipping this trigger")
        return "already running", 200

    threading.Thread(target=run_pump_check, daemon=True).start()
    return "started", 200


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


@app.route("/stats")
def stats():
    conn = get_conn()
    try:
        c = conn.cursor()

        c.execute("SELECT COUNT(*) FROM wallet_token_history")
        total_tokens = c.fetchone()[0]

        c.execute("SELECT COUNT(*) FROM wallet_token_history WHERE max_multiplier_seen >= 3")
        pumped_3x = c.fetchone()[0]

        c.execute("SELECT COUNT(*) FROM wallet_token_history WHERE max_multiplier_seen >= 10")
        pumped_10x = c.fetchone()[0]

        c.execute("SELECT COUNT(*) FROM wallet_token_history WHERE max_drawdown_seen >= 0.8")
        rugged = c.fetchone()[0]

        c.execute("SELECT COUNT(*) FROM token_scan_log")
        total_scans = c.fetchone()[0]

        c.execute("SELECT MIN(first_seen_at) FROM wallet_token_history")
        earliest = c.fetchone()[0]

        c.close()

        lines = [
            f"Tracking since: {earliest}",
            f"Total tokens tracked: {total_tokens}",
            f"Total scan snapshots logged: {total_scans}",
            f"Tokens that hit 3x+: {pumped_3x}",
            f"Tokens that hit 10x+: {pumped_10x}",
            f"Tokens that drew down 80%+ (likely rugs/dead): {rugged}",
            "",
            "Rule of thumb: you want 20-30+ examples in your smallest",
            "bucket (pumps or rugs, whichever is rarer) before trusting",
            "any pattern drawn from it.",
        ]
        return "<br>".join(lines), 200

    except Exception as e:
        return f"stats error: {e}", 500

    finally:
        conn.close()


@app.route("/analyze")
def analyze():
    """
    Compares early scan behavior (first 3 scans after first buy) between
    tokens that eventually pumped 3x+ vs tokens that eventually rugged
    (80%+ drawdown). Filters to tokens whose FIRST scan showed liquidity
    under $100k, so large/established tokens that simply drifted down
    over time don't get miscounted as memecoin "rugs" and skew the
    averages. Reports both mean and median for every metric.
    """
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute("""
            WITH ranked_scans AS (
                SELECT
                    s.wallet, s.token_mint, s.scanned_at,
                    s.buys_5m, s.sells_5m, s.vol_5m, s.vol_h1,
                    s.liquidity_delta_pct, s.momentum_score, s.liquidity,
                    ROW_NUMBER() OVER (
                        PARTITION BY s.wallet, s.token_mint
                        ORDER BY s.scanned_at
                    ) AS scan_num,
                    h.max_multiplier_seen,
                    h.max_drawdown_seen
                FROM token_scan_log s
                JOIN wallet_token_history h
                    ON h.wallet = s.wallet AND h.token_mint = s.token_mint
            ),
            first_liquidity AS (
                SELECT wallet, token_mint, liquidity AS starting_liquidity
                FROM ranked_scans
                WHERE scan_num = 1
            ),
            early_scans AS (
                SELECT r.*
                FROM ranked_scans r
                JOIN first_liquidity f
                    ON f.wallet = r.wallet AND f.token_mint = r.token_mint
                WHERE r.scan_num <= 3
                AND f.starting_liquidity IS NOT NULL
                AND f.starting_liquidity < 100000
            ),
            categorized AS (
                SELECT *,
                    CASE
                        WHEN COALESCE(max_multiplier_seen, 0) >= 3 THEN 'pumped'
                        WHEN COALESCE(max_drawdown_seen, 0) >= 0.8 THEN 'rugged'
                        ELSE 'neither'
                    END AS bucket
                FROM early_scans
            )
            SELECT
                bucket,
                COUNT(DISTINCT (wallet, token_mint)) AS token_count,
                AVG(CASE WHEN (buys_5m + sells_5m) > 0 THEN buys_5m::float / (buys_5m + sells_5m) END) AS avg_buy_ratio,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY
                    CASE WHEN (buys_5m + sells_5m) > 0 THEN buys_5m::float / (buys_5m + sells_5m) END
                ) AS median_buy_ratio,
                AVG(liquidity_delta_pct) AS avg_liquidity_delta_pct,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY liquidity_delta_pct) AS median_liquidity_delta_pct,
                AVG(vol_5m) AS avg_vol_5m,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vol_5m) AS median_vol_5m,
                AVG(vol_h1) AS avg_vol_h1,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vol_h1) AS median_vol_h1,
                AVG(momentum_score) AS avg_score,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY momentum_score) AS median_score,
                AVG(liquidity) AS avg_liquidity,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY liquidity) AS median_liquidity
            FROM categorized
            WHERE bucket IN ('pumped', 'rugged')
            GROUP BY bucket
            ORDER BY bucket
        """)
        rows = c.fetchall()
        c.close()

        if not rows:
            return "No scan data yet under $100k starting liquidity — needs more small-cap tokens tracked first.", 200

        lines = [
            "<b>Early-scan comparison (first 3 scans, tokens starting under $100k liquidity):</b><br>"
        ]
        data_by_bucket = {}
        cols = [
            "bucket", "token_count",
            "avg_buy_ratio", "median_buy_ratio",
            "avg_liq_delta", "median_liq_delta",
            "avg_vol_5m", "median_vol_5m",
            "avg_vol_h1", "median_vol_h1",
            "avg_score", "median_score",
            "avg_liquidity", "median_liquidity",
        ]
        for row in rows:
            d = dict(zip(cols, row))
            data_by_bucket[d["bucket"]] = d

        def fmt(val, kind="num"):
            if val is None:
                return "n/a"
            if kind == "pct":
                return f"{val*100:.1f}%"
            if kind == "usd":
                return f"${val:,.0f}"
            return f"{val:.2f}"

        for bucket in ["pumped", "rugged"]:
            d = data_by_bucket.get(bucket)
            if not d:
                lines.append(f"{bucket.upper()}: no examples yet<br>")
                continue
            lines.append(f"<br><b>{bucket.upper()}</b> (n={d['token_count']} tokens)")
            lines.append(f"Buy ratio — avg: {fmt(d['avg_buy_ratio'])}, median: {fmt(d['median_buy_ratio'])}")
            lines.append(f"Liquidity change vs prior scan — avg: {fmt(d['avg_liq_delta'], 'pct')}, median: {fmt(d['median_liq_delta'], 'pct')}")
            lines.append(f"5m volume — avg: {fmt(d['avg_vol_5m'], 'usd')}, median: {fmt(d['median_vol_5m'], 'usd')}")
            lines.append(f"1h volume — avg: {fmt(d['avg_vol_h1'], 'usd')}, median: {fmt(d['median_vol_h1'], 'usd')}")
            lines.append(f"Momentum score — avg: {fmt(d['avg_score'])}, median: {fmt(d['median_score'])}")
            lines.append(f"Liquidity — avg: {fmt(d['avg_liquidity'], 'usd')}, median: {fmt(d['median_liquidity'], 'usd')}")

        pumped_n = data_by_bucket.get("pumped", {}).get("token_count", 0)
        rugged_n = data_by_bucket.get("rugged", {}).get("token_count", 0)
        smaller = min(pumped_n, rugged_n)
        lines.append("<br><br>")
        if smaller < 20:
            lines.append(
                f"⚠️ Smallest bucket only has {smaller} tokens after the <$100k starting-liquidity "
                f"filter — treat any gap above as noise until both buckets have 20-30+."
            )
        else:
            lines.append(f"Sample sizes ({pumped_n} pumped, {rugged_n} rugged) still large enough to trust a consistent gap.")

        return "<br>".join(str(l) for l in lines), 200

    except Exception as e:
        return f"analyze error: {e}", 500

    finally:
        conn.close()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
