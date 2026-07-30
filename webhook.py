from flask import Flask, request
import os
import re
import time
import threading
from concurrent.futures import ThreadPoolExecutor
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

MIN_LIQUIDITY_USD = int(os.environ.get("MIN_LIQUIDITY_USD", "8000"))
WSOL_MINT = "So11111111111111111111111111111111111111112"

SCAN_WINDOW_HOURS = int(os.environ.get("SCAN_WINDOW_HOURS", "168"))
MAX_CONCURRENT_DEXSCREENER = int(os.environ.get("MAX_CONCURRENT_DEXSCREENER", "5"))
DB_CONN_REFRESH_EVERY = int(os.environ.get("DB_CONN_REFRESH_EVERY", "150"))

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
_dex_rate_lock = threading.Semaphore(MAX_CONCURRENT_DEXSCREENER)


def looks_like_solana_address(text):
    return bool(SOLANA_ADDRESS_RE.match(text.strip()))


# ---------- DB ----------

def get_conn():
    return psycopg2.connect(
        DATABASE_URL,
        connect_timeout=10,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )


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
        c.execute("ALTER TABLE wallet_token_history ADD COLUMN IF NOT EXISTS price_at_recommendation NUMERIC")
        c.execute("ALTER TABLE wallet_token_history ADD COLUMN IF NOT EXISTS recommended_at TIMESTAMP")
        c.execute("ALTER TABLE wallet_token_history ADD COLUMN IF NOT EXISTS max_multiplier_since_recommendation NUMERIC")
        c.execute("ALTER TABLE wallet_token_history ADD COLUMN IF NOT EXISTS pumped_since_recommendation_alerted BOOLEAN DEFAULT FALSE")
        c.execute("ALTER TABLE wallet_token_history ADD COLUMN IF NOT EXISTS market_cap_at_recommendation NUMERIC")

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
        c.execute("ALTER TABLE token_scan_log ADD COLUMN IF NOT EXISTS multiplier_since_recommendation NUMERIC")
        c.execute("ALTER TABLE token_scan_log ADD COLUMN IF NOT EXISTS market_cap NUMERIC")

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
        if resp.status_code != 200 or not resp.text.strip():
            return None
        data = resp.json()
        pairs = data.get("pairs") or []
        if not pairs:
            return None
        pair = max(pairs, key=lambda p: p.get("liquidity", {}).get("usd", 0) or 0)
        return pair
    except Exception as e:
        print(f"DexScreener error for {mint}: {e}")
        return None


def get_dexscreener_full_ratelimited(mint):
    with _dex_rate_lock:
        result = get_dexscreener_full(mint)
        time.sleep(0.1)
        return result


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


def explain_pump(mint, price_at_recommendation, current_price, multiplier, recommended_at):
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute("""
            SELECT liquidity, vol_5m, vol_h1, pc_5m, pc_h1, pc_h6, buys_5m, sells_5m
            FROM token_scan_log
            WHERE token_mint = %s AND scanned_at >= %s
            ORDER BY scanned_at ASC
            LIMIT 1
        """, (mint, recommended_at))
        then_row = c.fetchone()

        c.execute("""
            SELECT liquidity, vol_5m, vol_h1, pc_5m, pc_h1, pc_h6, buys_5m, sells_5m
            FROM token_scan_log
            WHERE token_mint = %s
            ORDER BY scanned_at DESC
            LIMIT 1
        """, (mint,))
        now_row = c.fetchone()
        c.close()
    finally:
        conn.close()

    if not then_row or not now_row:
        return "Not enough scan history to break down what changed."

    then_liq, then_v5, then_v1, then_p5, then_p1, then_p6, then_b, then_s = then_row
    now_liq, now_v5, now_v1, now_p5, now_p1, now_p6, now_b, now_s = now_row

    def pct_change(old, new):
        if not old or old == 0:
            return "n/a"
        return f"{((new - old) / old) * 100:+.0f}%"

    context = (
        f"Price went {multiplier:.1f}x since recommendation at ${price_at_recommendation} "
        f"(now ${current_price}). Liquidity changed {pct_change(then_liq, now_liq)} "
        f"(${then_liq:,.0f} → ${now_liq:,.0f}). 1h volume changed {pct_change(then_v1, now_v1)} "
        f"(${then_v1:,.0f} → ${now_v1:,.0f}). Buy count (5m window) went from {then_b} to {now_b}, "
        f"sell count from {then_s} to {now_s}."
    )

    prompt = (
        f"Based ONLY on this real before/after data, give a 2-sentence explanation of what actually "
        f"changed to drive this move: {context} "
        f"Do NOT invent external causes like news, influencers, or hype you have no data for — "
        f"stick strictly to what these numbers show."
    )
    return ask_queen(prompt)


# ---------- Momentum scoring ----------

def score_momentum(pair, liquidity_delta_pct=None):
    score = 0
    details = {}

    liquidity = pair.get("liquidity", {}).get("usd", 0) or 0
    details["liquidity"] = liquidity
    if liquidity < MIN_LIQUIDITY_USD:
        return 0, details

    details["liquidity_delta_pct"] = liquidity_delta_pct
    if liquidity_delta_pct is not None and liquidity_delta_pct > 0:
        trend_score = min(1.0, liquidity_delta_pct / 0.10) * 45
    else:
        trend_score = 0
    score += trend_score

    liquidity_score = min(1.0, liquidity / 15000) * 25
    score += liquidity_score

    price_change = pair.get("priceChange", {}) or {}
    pc_5m = price_change.get("m5", 0) or 0
    pc_h1 = price_change.get("h1", 0) or 0
    pc_h6 = price_change.get("h6", 0) or 0
    details["pc_5m"] = pc_5m
    details["pc_h1"] = pc_h1
    details["pc_h6"] = pc_h6
    positive_windows = sum(1 for x in [pc_5m, pc_h1, pc_h6] if x and x > 0)
    score += positive_windows * (20 / 3)

    volume = pair.get("volume", {}) or {}
    vol_5m = volume.get("m5", 0) or 0
    vol_h1 = volume.get("h1", 0) or 0
    details["vol_5m"] = vol_5m
    details["vol_h1"] = vol_h1

    vol_to_liq_ratio = (vol_5m / liquidity) if liquidity > 0 else 0
    details["vol_to_liq_ratio"] = vol_to_liq_ratio
    if vol_to_liq_ratio > 0.5:
        score -= 10
    elif 0.01 <= vol_to_liq_ratio <= 0.3:
        score += 10

    txns = pair.get("txns", {}) or {}
    m5 = txns.get("m5", {}) or {}
    details["buys_5m"] = m5.get("buys", 0) or 0
    details["sells_5m"] = m5.get("sells", 0) or 0

    return round(max(0, score)), details


# ---------- Backward-looking trend labels (zero-delay, informational only) ----------

def get_prior_scan_snapshot(mint):
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute("""
            SELECT liquidity_delta_pct, pc_5m
            FROM token_scan_log
            WHERE token_mint = %s
            ORDER BY scanned_at DESC
            LIMIT 1
        """, (mint,))
        row = c.fetchone()
        c.close()
        if not row:
            return None, None
        return row[0], row[1]
    finally:
        conn.close()


def liquidity_trend_label(current_delta, prior_delta):
    if current_delta is None or prior_delta is None:
        return "⚪ Liquidity trend: not enough history yet to judge consistency."
    if current_delta > 0 and prior_delta > 0:
        return "✅ Liquidity growing across 2+ scans — looks like sustained trend."
    elif current_delta > 0 and prior_delta <= 0:
        return "⚠️ Liquidity just ticked up after a dip — could be a one-off blip, not confirmed trend yet."
    else:
        return "⚠️ Liquidity trend unclear."


def price_trend_label(current_pc_5m, prior_pc_5m):
    if current_pc_5m is None or prior_pc_5m is None:
        return "⚪ Price trend: not enough history yet to judge consistency."
    if current_pc_5m > 0 and prior_pc_5m > 0:
        return "✅ Price also rising on prior scan — momentum looks consistent."
    elif current_pc_5m > 0 and prior_pc_5m <= 0:
        return "⚠️ Price just turned positive this scan — no confirmation yet from prior check."
    else:
        return "⚠️ Price trend unclear."


# ---------- RugCheck integration ----------

def get_rugcheck_data(mint):
    """
    Free, no-key-required RugCheck API check (https://api.rugcheck.xyz/v1).
    Returns (risk_score, liquidity_flags) or (None, None) on any failure —
    never blocks or delays the alert if RugCheck is slow/down, just omits
    this section from the message.

    Note: RugCheck flags CONTRACT-level risks (mint/freeze authority,
    LP lock status) — it does not predict ordinary market-driven dumps
    (e.g. a team simply selling their bags), only structural rug-pull
    mechanisms like unlocked liquidity.
    """
    try:
        url = f"https://api.rugcheck.xyz/v1/tokens/{mint}/report/summary"
        resp = requests.get(url, timeout=5)
        if resp.status_code != 200:
            return None, None
        data = resp.json()
        risk_score = data.get("score_normalised")
        risks = data.get("risks", []) or []

        liquidity_risks = [
            r for r in risks
            if r.get("name") and ("liquidity" in r["name"].lower() or "lp" in r["name"].lower())
        ]
        summary_bits = [r.get("name") for r in liquidity_risks if r.get("name")]

        return risk_score, summary_bits
    except Exception as e:
        print(f"RugCheck error for {mint}: {e}")
        return None, None


def rugcheck_label(risk_score, liquidity_flags):
    if risk_score is None:
        return "⚪ RugCheck: no data available for this token."
    if risk_score <= 30:
        base = f"🟢 RugCheck score: {risk_score}/100 (low risk)"
    elif risk_score <= 60:
        base = f"🟡 RugCheck score: {risk_score}/100 (moderate risk)"
    else:
        base = f"🔴 RugCheck score: {risk_score}/100 (HIGH RISK)"
    if liquidity_flags:
        base += f" — flags: {', '.join(liquidity_flags)}"
    return base


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

        if row is None:
            price = get_current_price(mint)
            c.execute(
                "INSERT INTO wallet_token_history (wallet, token_mint, buy_count, price_at_first_buy) VALUES (%s, %s, 1, %s)",
                (wallet, mint, price)
            )
            conn.commit()
            print(f"🟢 FIRST BUY DETECTED (recorded, no alert): wallet={wallet} token={mint} price={price}")

        else:
            buy_count, price_at_first_buy = row
            new_count = buy_count + 1
            c.execute(
                "UPDATE wallet_token_history SET buy_count = %s WHERE wallet=%s AND token_mint=%s",
                (new_count, wallet, mint)
            )
            conn.commit()
            print(f"🔁 Repeat buy #{new_count} (recorded, no alert): wallet={wallet} token={mint}")

        c.close()
    finally:
        conn.close()


# ---------- Periodic pump/momentum check ----------

def run_pump_check():
    conn = None
    c = None
    checked = 0

    try:
        conn = get_conn()
        c = conn.cursor()

        c.execute("""
            SELECT wallet, token_mint, price_at_first_buy, pumped_3x_alerted,
                   momentum_alerted, last_liquidity, price_at_recommendation,
                   pumped_since_recommendation_alerted, recommended_at,
                   market_cap_at_recommendation
            FROM wallet_token_history
            WHERE first_seen_at > NOW() - (INTERVAL '1 hour' * %s)
            AND (pumped_3x_alerted = FALSE OR momentum_alerted = FALSE
                 OR pumped_since_recommendation_alerted = FALSE)
        """, (SCAN_WINDOW_HOURS,))
        rows = c.fetchall()
        print(f"Checking {len(rows)} tokens for pumps/momentum...")

        mints = [row[1] for row in rows]
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_DEXSCREENER) as executor:
            pair_results = list(executor.map(get_dexscreener_full_ratelimited, mints))
        pairs_by_mint = dict(zip(mints, pair_results))

        for i, (wallet, mint, price_at_first_buy, pumped_alerted, momentum_alerted,
                prev_liquidity, price_at_recommendation, pumped_since_rec_alerted,
                recommended_at, market_cap_at_recommendation) in enumerate(rows):

            if i > 0 and i % DB_CONN_REFRESH_EVERY == 0:
                try:
                    conn.close()
                except Exception:
                    pass
                try:
                    conn = get_conn()
                    c = conn.cursor()
                    print(f"Proactively refreshed DB connection at token {i}")
                except Exception as refresh_err:
                    print(f"Proactive refresh failed at token {i}: {refresh_err}")

            try:
                pair = pairs_by_mint.get(mint)
                if not pair:
                    continue
                checked += 1

                current_price = None
                try:
                    current_price = float(pair.get("priceUsd"))
                except (TypeError, ValueError):
                    pass

                current_market_cap = pair.get("fdv", 0) or 0
                dexscreener_url = f"https://dexscreener.com/solana/{mint}"

                multiplier_from_first_buy = None
                if price_at_first_buy and current_price:
                    try:
                        multiplier_from_first_buy = current_price / float(price_at_first_buy)
                    except (TypeError, ValueError, ZeroDivisionError):
                        multiplier_from_first_buy = None

                drawdown = None
                if price_at_first_buy and current_price:
                    try:
                        drawdown = max(0.0, 1 - (current_price / float(price_at_first_buy)))
                    except (TypeError, ValueError, ZeroDivisionError):
                        drawdown = None

                current_liquidity_raw = pair.get("liquidity", {}).get("usd", 0) or 0
                liquidity_delta_pct = None
                if current_liquidity_raw is not None and prev_liquidity:
                    try:
                        liquidity_delta_pct = (float(current_liquidity_raw) - float(prev_liquidity)) / float(prev_liquidity)
                    except (TypeError, ValueError, ZeroDivisionError):
                        liquidity_delta_pct = None

                score, details = score_momentum(pair, liquidity_delta_pct)
                current_liquidity = details.get("liquidity")

                multiplier_since_recommendation = None
                if price_at_recommendation and current_price:
                    try:
                        multiplier_since_recommendation = current_price / float(price_at_recommendation)
                    except (TypeError, ValueError, ZeroDivisionError):
                        multiplier_since_recommendation = None

                if current_price is not None:
                    c.execute(
                        """
                        UPDATE wallet_token_history
                        SET last_checked_at = NOW(),
                            max_price_seen = GREATEST(COALESCE(max_price_seen, 0), %s),
                            max_multiplier_seen = GREATEST(COALESCE(max_multiplier_seen, 0), COALESCE(%s, 0)),
                            min_price_seen = LEAST(COALESCE(min_price_seen, %s), %s),
                            max_drawdown_seen = GREATEST(COALESCE(max_drawdown_seen, 0), COALESCE(%s, 0)),
                            last_liquidity = COALESCE(%s, last_liquidity),
                            max_multiplier_since_recommendation = GREATEST(
                                COALESCE(max_multiplier_since_recommendation, 0),
                                COALESCE(%s, 0)
                            )
                        WHERE wallet=%s AND token_mint=%s
                        """,
                        (current_price, multiplier_from_first_buy, current_price, current_price,
                         drawdown, current_liquidity, multiplier_since_recommendation, wallet, mint)
                    )

                momentum_alert_fired = False
                pump_alert_fired = False

                if not momentum_alerted and score >= 70:
                    momentum_alert_fired = True

                    prior_liq_delta, prior_pc_5m = get_prior_scan_snapshot(mint)
                    liq_trend_note = liquidity_trend_label(liquidity_delta_pct, prior_liq_delta)
                    price_trend_note = price_trend_label(details.get("pc_5m"), prior_pc_5m)

                    rug_score, rug_liq_flags = get_rugcheck_data(mint)
                    rug_note = rugcheck_label(rug_score, rug_liq_flags)

                    c.execute(
                        """
                        UPDATE wallet_token_history
                        SET momentum_alerted = TRUE,
                            price_at_recommendation = %s,
                            recommended_at = NOW(),
                            market_cap_at_recommendation = %s
                        WHERE wallet=%s AND token_mint=%s
                        """,
                        (current_price, current_market_cap, wallet, mint)
                    )
                    send_telegram_alert(
                        f"🚀 Heating up (score {score}/100)\n"
                        f"Wallet: <code>{wallet}</code>\n"
                        f"Token: <code>{mint}</code>\n\n"
                        f"Market cap: ${current_market_cap:,.0f}\n"
                        f"Liquidity: ${details.get('liquidity', 0):.0f}"
                        + (f" (Δ {liquidity_delta_pct*100:.1f}% since last scan)" if liquidity_delta_pct is not None else "")
                        + f"\n5m volume: ${details.get('vol_5m', 0):.0f} (1h: ${details.get('vol_h1', 0):.0f})\n"
                        f"Price change 5m/1h/6h: {details.get('pc_5m')}% / {details.get('pc_h1')}% / {details.get('pc_h6')}%\n\n"
                        f"{liq_trend_note}\n"
                        f"{price_trend_note}\n"
                        f"{rug_note}\n\n"
                        f"📊 DexScreener: {dexscreener_url}\n\n"
                        f"Recommending this now — tracking from this price to see if it delivers. DYOR."
                    )

                elif (not pumped_since_rec_alerted and price_at_recommendation
                      and multiplier_since_recommendation and multiplier_since_recommendation >= 3):
                    pump_alert_fired = True
                    mc_line = ""
                    if market_cap_at_recommendation:
                        mc_line = (
                            f"Market cap then: ${float(market_cap_at_recommendation):,.0f} → "
                            f"now: ${current_market_cap:,.0f}\n"
                        )
                    send_telegram_alert(
                        f"🎯 RECOMMENDATION PAID OFF — {multiplier_since_recommendation:.1f}x since recommended!\n"
                        f"Wallet: <code>{wallet}</code>\n"
                        f"Token: <code>{mint}</code>\n"
                        f"Price at recommendation: ${price_at_recommendation} → now: ${current_price}\n"
                        f"{mc_line}\n"
                        f"📊 DexScreener: {dexscreener_url}\n\n"
                        f"Type <code>/why {mint}</code> if you want the breakdown of what actually moved."
                    )
                    c.execute(
                        "UPDATE wallet_token_history SET pumped_since_recommendation_alerted = TRUE WHERE wallet=%s AND token_mint=%s",
                        (wallet, mint)
                    )

                c.execute(
                    """
                    INSERT INTO token_scan_log
                        (wallet, token_mint, price, liquidity, vol_5m, vol_h1,
                         pc_5m, pc_h1, pc_h6, buys_5m, sells_5m, momentum_score,
                         multiplier_from_first_buy, drawdown_from_first_buy,
                         liquidity_delta_pct, momentum_alert_fired, pump_alert_fired,
                         multiplier_since_recommendation, market_cap)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        wallet, mint, current_price, details.get("liquidity"),
                        details.get("vol_5m"), details.get("vol_h1"),
                        details.get("pc_5m"), details.get("pc_h1"), details.get("pc_h6"),
                        details.get("buys_5m"), details.get("sells_5m"), score,
                        multiplier_from_first_buy, drawdown, liquidity_delta_pct,
                        momentum_alert_fired, pump_alert_fired, multiplier_since_recommendation,
                        current_market_cap
                    )
                )

                conn.commit()

            except psycopg2.OperationalError as db_err:
                print(f"DB connection dropped mid-scan on {mint}: {db_err} — reconnecting")
                try:
                    conn.close()
                except Exception:
                    pass
                try:
                    conn = get_conn()
                    c = conn.cursor()
                except Exception as reconnect_err:
                    print(f"Reconnect failed: {reconnect_err}")
                    break
                continue

            except Exception as e:
                print(f"Error processing {mint}: {e}")
                continue

        print(f"check_pumps finished — checked {checked} tokens")

    except Exception as e:
        print(f"check_pumps error: {e}")

    finally:
        if c:
            try:
                c.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass
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

    elif stripped.lower().startswith("/why"):
        parts = stripped.split(maxsplit=1)
        if len(parts) < 2:
            send_telegram_alert("Give me a token address: <code>/why &lt;mint_address&gt;</code>", chat_id)
        else:
            mint = parts[1].strip()
            conn = get_conn()
            try:
                c = conn.cursor()
                c.execute("""
                    SELECT price_at_recommendation, recommended_at, max_multiplier_since_recommendation
                    FROM wallet_token_history
                    WHERE token_mint = %s AND price_at_recommendation IS NOT NULL
                    ORDER BY recommended_at DESC
                    LIMIT 1
                """, (mint,))
                row = c.fetchone()
                c.close()
            finally:
                conn.close()

            if not row or not row[0]:
                send_telegram_alert(
                    f"I never recommended <code>{mint}</code> — no baseline to compare against.",
                    chat_id
                )
            else:
                price_at_recommendation, recommended_at, max_mult = row
                current_price = get_current_price(mint)
                if not current_price:
                    send_telegram_alert(f"Can't get current price for <code>{mint}</code> right now.", chat_id)
                else:
                    multiplier_now = current_price / float(price_at_recommendation)
                    explanation = explain_pump(mint, float(price_at_recommendation), current_price, multiplier_now, recommended_at)
                    send_telegram_alert(
                        f"🔍 <b>Why {mint} moved:</b>\n"
                        f"Recommended at ${price_at_recommendation}, now ${current_price} ({multiplier_now:.2f}x)\n\n"
                        f"{explanation}",
                        chat_id
                    )

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

        c.execute("SELECT COUNT(*) FROM wallet_token_history WHERE momentum_alerted = TRUE")
        total_recommended = c.fetchone()[0]

        c.execute("SELECT COUNT(*) FROM wallet_token_history WHERE pumped_since_recommendation_alerted = TRUE")
        recommendations_that_paid_off = c.fetchone()[0]

        c.close()

        lines = [
            f"Tracking since: {earliest}",
            f"Total tokens tracked: {total_tokens}",
            f"Total scan snapshots logged: {total_scans}",
            f"Tokens that hit 3x+ (from first buy): {pumped_3x}",
            f"Tokens that hit 10x+ (from first buy): {pumped_10x}",
            f"Tokens that drew down 80%+ (likely rugs/dead): {rugged}",
            "",
            f"Tokens recommended (score crossed 70): {total_recommended}",
            f"Recommendations that hit 3x+ afterward: {recommendations_that_paid_off}"
            + (f" ({recommendations_that_paid_off/total_recommended*100:.1f}% hit rate)" if total_recommended else ""),
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


@app.route("/analyze-alerts")
def analyze_alerts():
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute("""
            SELECT
                s.wallet, s.token_mint, s.momentum_score,
                h.max_multiplier_since_recommendation,
                h.pumped_since_recommendation_alerted
            FROM token_scan_log s
            JOIN wallet_token_history h
                ON h.wallet = s.wallet AND h.token_mint = s.token_mint
            WHERE s.momentum_alert_fired = TRUE
        """)
        rows = c.fetchall()
        c.close()

        if not rows:
            return "No recommendations logged yet.", 200

        buckets = {
            "70-79": {"total": 0, "hit_3x": 0},
            "80-89": {"total": 0, "hit_3x": 0},
            "90-100": {"total": 0, "hit_3x": 0},
        }

        for wallet, mint, score, max_mult, hit_3x in rows:
            if score is None:
                continue
            if 70 <= score < 80:
                key = "70-79"
            elif 80 <= score < 90:
                key = "80-89"
            elif score >= 90:
                key = "90-100"
            else:
                continue

            buckets[key]["total"] += 1
            if hit_3x or (max_mult and max_mult >= 3):
                buckets[key]["hit_3x"] += 1

        lines = ["<b>Recommendation hit-rate by score bucket:</b><br>"]
        for bucket, d in buckets.items():
            total = d["total"]
            hits = d["hit_3x"]
            rate = f"{hits/total*100:.1f}%" if total else "n/a"
            lines.append(f"<br>Score {bucket}: {hits}/{total} hit 3x+ ({rate})")

        lines.append("<br><br>⚠️ Needs time to mature — recommendations made in the last "
                      "few hours haven't had a chance to play out yet. Check back after "
                      "24-48h of stable running for a meaningful read.")

        return "<br>".join(lines), 200

    except Exception as e:
        return f"analyze_alerts error: {e}", 500

    finally:
        conn.close()


@app.route("/check-gate-risk")
def check_gate_risk():
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute("""
            WITH ranked_scans AS (
                SELECT
                    s.wallet, s.token_mint, s.liquidity_delta_pct,
                    ROW_NUMBER() OVER (
                        PARTITION BY s.wallet, s.token_mint
                        ORDER BY s.scanned_at
                    ) AS scan_num,
                    h.max_multiplier_seen
                FROM token_scan_log s
                JOIN wallet_token_history h
                    ON h.wallet = s.wallet AND h.token_mint = s.token_mint
            ),
            pumped_tokens AS (
                SELECT DISTINCT wallet, token_mint
                FROM ranked_scans
                WHERE max_multiplier_seen >= 3
            ),
            early_scans_of_pumped AS (
                SELECT r.wallet, r.token_mint, r.liquidity_delta_pct
                FROM ranked_scans r
                JOIN pumped_tokens p
                    ON p.wallet = r.wallet AND p.token_mint = r.token_mint
                WHERE r.scan_num <= 3
            )
            SELECT
                COUNT(DISTINCT (wallet, token_mint)) AS total_pumped_tokens,
                COUNT(DISTINCT (wallet, token_mint)) FILTER (
                    WHERE liquidity_delta_pct < 0
                ) AS pumped_tokens_with_early_negative_delta
            FROM early_scans_of_pumped
        """)
        row = c.fetchone()
        c.close()

        if not row or not row[0]:
            return "No pumped tokens with scan history yet.", 200

        total, with_negative = row
        pct = (with_negative / total * 100) if total else 0

        lines = [
            f"Total pumped (3x+) tokens with scan history: {total}",
            f"Of those, tokens with at least one early negative liquidity delta: {with_negative} ({pct:.1f}%)",
            "",
            "If this % is high (e.g. 30%+), a hard gate blocking negative-delta "
            "scans would filter out real winners — better to keep it as a "
            "scored factor, not a strict requirement.",
            "If this % is low, the hard gate is safe to add.",
        ]
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_gate_risk error: {e}", 500

    finally:
        conn.close()


@app.route("/check-volume-floor-risk")
def check_volume_floor_risk():
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute("""
            WITH ranked_scans AS (
                SELECT
                    s.wallet, s.token_mint, s.vol_5m,
                    ROW_NUMBER() OVER (
                        PARTITION BY s.wallet, s.token_mint
                        ORDER BY s.scanned_at
                    ) AS scan_num,
                    h.max_multiplier_seen
                FROM token_scan_log s
                JOIN wallet_token_history h
                    ON h.wallet = s.wallet AND h.token_mint = s.token_mint
            ),
            pumped_tokens AS (
                SELECT DISTINCT wallet, token_mint
                FROM ranked_scans
                WHERE max_multiplier_seen >= 3
            ),
            early_scans_of_pumped AS (
                SELECT r.wallet, r.token_mint, r.vol_5m
                FROM ranked_scans r
                JOIN pumped_tokens p
                    ON p.wallet = r.wallet AND p.token_mint = r.token_mint
                WHERE r.scan_num <= 3
                AND r.vol_5m IS NOT NULL
            )
            SELECT
                MIN(vol_5m) AS min_vol,
                PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY vol_5m) AS p5_vol,
                PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY vol_5m) AS p10_vol,
                COUNT(*) AS total_scans
            FROM early_scans_of_pumped
        """)
        row = c.fetchone()
        c.close()

        if not row or row[3] == 0:
            return "No early scan data for pumped tokens yet.", 200

        min_vol, p5_vol, p10_vol, total_scans = row

        lines = [
            f"Based on {total_scans} early scans of tokens that went on to pump 3x+:",
            "",
            f"Absolute minimum 5m volume seen: ${min_vol:,.2f}",
            f"5th percentile: ${p5_vol:,.2f}",
            f"10th percentile: ${p10_vol:,.2f}",
            "",
            "A MIN_VOL_5M_USD floor set ABOVE the absolute minimum risks "
            "cutting off at least one real winner that started this quiet. "
            "Setting it at or below the 5th percentile keeps that risk very low "
            "(only ~5% of pumped-token early scans were this quiet or quieter).",
        ]
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_volume_floor_risk error: {e}", 500

    finally:
        conn.close()


@app.route("/check-oscillation-risk")
def check_oscillation_risk():
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute("""
            WITH ranked_scans AS (
                SELECT
                    s.wallet, s.token_mint, s.liquidity_delta_pct,
                    s.momentum_alert_fired, s.momentum_score,
                    ROW_NUMBER() OVER (
                        PARTITION BY s.wallet, s.token_mint
                        ORDER BY s.scanned_at
                    ) AS scan_num
                FROM token_scan_log s
            ),
            recommendation_scans AS (
                SELECT wallet, token_mint, scan_num
                FROM ranked_scans
                WHERE momentum_alert_fired = TRUE AND momentum_score >= 90
            ),
            prior_scan_delta AS (
                SELECT
                    r.wallet, r.token_mint,
                    prior.liquidity_delta_pct AS prior_delta
                FROM recommendation_scans r
                JOIN ranked_scans prior
                    ON prior.wallet = r.wallet
                    AND prior.token_mint = r.token_mint
                    AND prior.scan_num = r.scan_num - 1
            )
            SELECT
                p.wallet, p.token_mint, p.prior_delta,
                h.pumped_since_recommendation_alerted,
                h.max_multiplier_since_recommendation
            FROM prior_scan_delta p
            JOIN wallet_token_history h
                ON h.wallet = p.wallet AND h.token_mint = p.token_mint
        """)
        rows = c.fetchall()
        c.close()

        if not rows:
            return "Not enough 90+ recommendations with prior-scan data yet.", 200

        sustained = {"total": 0, "paid_off": 0}
        oscillating = {"total": 0, "paid_off": 0}

        for wallet, mint, prior_delta, paid_off, max_mult in rows:
            hit = bool(paid_off) or (max_mult and max_mult >= 3)
            if prior_delta is not None and prior_delta > 0:
                sustained["total"] += 1
                if hit:
                    sustained["paid_off"] += 1
            elif prior_delta is not None and prior_delta <= 0:
                oscillating["total"] += 1
                if hit:
                    oscillating["paid_off"] += 1

        def rate(d):
            return f"{d['paid_off']}/{d['total']} ({d['paid_off']/d['total']*100:.1f}%)" if d["total"] else "n/a"

        lines = [
            "<b>90+ score recommendations, by liquidity pattern:</b><br>",
            f"<br>Sustained growth (prior scan also positive): {rate(sustained)}",
            f"Oscillating (prior scan was flat/negative): {rate(oscillating)}",
            "<br><br>If oscillating tokens hit noticeably less often, that "
            "confirms the fix is worth the delay. If both rates are similar, "
            "the delay isn't worth adding.",
        ]
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_oscillation_risk error: {e}", 500

    finally:
        conn.close()


@app.route("/token/<mint>")
def token_history(mint):
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute("""
            SELECT scanned_at, price, liquidity, vol_5m, vol_h1,
                   pc_5m, pc_h1, pc_h6, buys_5m, sells_5m,
                   momentum_score, multiplier_from_first_buy,
                   liquidity_delta_pct, momentum_alert_fired, pump_alert_fired,
                   multiplier_since_recommendation, market_cap
            FROM token_scan_log
            WHERE token_mint = %s
            ORDER BY scanned_at ASC
        """, (mint,))
        rows = c.fetchall()
        c.close()

        if not rows:
            return f"No scan history found for {mint}", 200

        lines = [f"<b>Scan history for {mint}</b> ({len(rows)} scans)<br>"]
        for i, row in enumerate(rows, 1):
            (scanned_at, price, liquidity, vol_5m, vol_h1, pc_5m, pc_h1, pc_h6,
             buys_5m, sells_5m, score, multiplier, liq_delta, mom_fired, pump_fired,
             mult_since_rec, market_cap) = row

            flags = []
            if mom_fired:
                flags.append("🚀 RECOMMENDED HERE")
            if pump_fired:
                flags.append("🎯 3x-SINCE-RECOMMENDATION FIRED")
            flag_str = " ".join(flags)

            lines.append(
                f"<br><b>Scan {i}</b> — {scanned_at} {flag_str}<br>"
                f"Price: ${price} | Market cap: {f'${market_cap:,.0f}' if market_cap else 'n/a'} | "
                f"Multiplier from first buy: {f'{multiplier:.2f}x' if multiplier else 'n/a'}"
                + (f" | Since recommendation: {mult_since_rec:.2f}x" if mult_since_rec else "")
                + f"<br>"
                f"Liquidity: ${liquidity:,.0f} (Δ {f'{liq_delta*100:.1f}%' if liq_delta is not None else 'n/a'})<br>"
                f"Volume 5m/1h: ${vol_5m:,.0f} / ${vol_h1:,.0f}<br>"
                f"Price change 5m/1h/6h: {pc_5m}% / {pc_h1}% / {pc_h6}%<br>"
                f"Buys/Sells (5m): {buys_5m}/{sells_5m}<br>"
                f"Momentum score: {score}"
            )

        return "<br>".join(lines), 200

    except Exception as e:
        return f"token_history error: {e}", 500

    finally:
        conn.close()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
