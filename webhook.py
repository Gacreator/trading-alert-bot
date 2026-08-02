from flask import Flask, request
import os
import re
import time
import datetime
import threading
from concurrent.futures import ThreadPoolExecutor
import psycopg2
import requests

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_IDS = [
    cid.strip() for cid in os.environ.get("TELEGRAM_CHAT_ID", "").split(",") if cid.strip()
]
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
HELIUS_API_KEY = os.environ.get("HELIUS_API_KEY")

TRACKED_WALLETS = set(
    w.strip() for w in os.environ.get("TRACKED_WALLETS", "").split(",") if w.strip()
)

MIN_LIQUIDITY_USD = int(os.environ.get("MIN_LIQUIDITY_USD", "8000"))
WSOL_MINT = "So11111111111111111111111111111111111111112"

SCAN_WINDOW_HOURS = int(os.environ.get("SCAN_WINDOW_HOURS", "48"))
MAX_CONCURRENT_DEXSCREENER = int(os.environ.get("MAX_CONCURRENT_DEXSCREENER", "5"))
DB_CONN_REFRESH_EVERY = int(os.environ.get("DB_CONN_REFRESH_EVERY", "150"))

MAX_PLAUSIBLE_MULTIPLIER = float(os.environ.get("MAX_PLAUSIBLE_MULTIPLIER", "50"))
MAX_PLAUSIBLE_PC_H6 = float(os.environ.get("MAX_PLAUSIBLE_PC_H6", "5000"))

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


def get_date_filter_params():
    since_param = request.args.get("since")
    until_param = request.args.get("until")
    return since_param, until_param


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
        c.execute("ALTER TABLE wallet_token_history ADD COLUMN IF NOT EXISTS rugcheck_score_at_recommendation NUMERIC")
        c.execute("ALTER TABLE wallet_token_history ADD COLUMN IF NOT EXISTS top1_holder_pct_at_recommendation NUMERIC")
        c.execute("ALTER TABLE wallet_token_history ADD COLUMN IF NOT EXISTS cluster_count_at_recommendation INTEGER")

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
        c.execute("ALTER TABLE token_scan_log ADD COLUMN IF NOT EXISTS suspect_data BOOLEAN DEFAULT FALSE")

        conn.commit()
        c.close()
    finally:
        conn.close()


init_db()


def _send_to_one_chat(message, chat_id):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        resp = requests.post(url, json=payload, timeout=5)
        print(f"Telegram response ({chat_id}): {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"Failed to send Telegram alert to {chat_id}: {e}")


def send_telegram_alert(message, chat_id=None):
    if chat_id:
        _send_to_one_chat(message, chat_id)
        return

    if not TELEGRAM_CHAT_IDS:
        print("No TELEGRAM_CHAT_ID configured — alert not sent.")
        return

    for cid in TELEGRAM_CHAT_IDS:
        _send_to_one_chat(message, cid)


def send_bare_address_to_rick_chat(mint):
    if len(TELEGRAM_CHAT_IDS) < 2:
        print("No second chat ID configured — skipping Rick address ping.")
        return
    rick_chat_id = TELEGRAM_CHAT_IDS[1]

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": rick_chat_id,
        "text": mint,
    }
    try:
        resp = requests.post(url, json=payload, timeout=5)
        print(f"Rick address ping ({rick_chat_id}): {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"Failed to send Rick address ping: {e}")


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


def score_momentum(pair, liquidity_delta_pct=None, prior_liquidity_delta_pct=None):
    score = 0
    details = {}

    liquidity = pair.get("liquidity", {}).get("usd", 0) or 0
    details["liquidity"] = liquidity
    if liquidity < MIN_LIQUIDITY_USD:
        return 0, details

    details["liquidity_delta_pct"] = liquidity_delta_pct
    details["prior_liquidity_delta_pct"] = prior_liquidity_delta_pct

    if liquidity_delta_pct is not None and liquidity_delta_pct > 0:
        if prior_liquidity_delta_pct is not None and prior_liquidity_delta_pct > 0:
            trend_score = min(1.0, liquidity_delta_pct / 0.10) * 45
        else:
            trend_score = min(1.0, liquidity_delta_pct / 0.10) * 15
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

    vol_h1_to_liq_ratio = (vol_h1 / liquidity) if liquidity > 0 else 0
    details["vol_h1_to_liq_ratio"] = vol_h1_to_liq_ratio
    if vol_h1_to_liq_ratio > 10:
        score -= 20
    elif vol_h1_to_liq_ratio > 5:
        score -= 10

    txns = pair.get("txns", {}) or {}
    m5 = txns.get("m5", {}) or {}
    details["buys_5m"] = m5.get("buys", 0) or 0
    details["sells_5m"] = m5.get("sells", 0) or 0

    return round(max(0, score)), details


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


def is_suspect_scan(multiplier_from_first_buy, multiplier_since_recommendation, pc_h6):
    if multiplier_from_first_buy is not None and multiplier_from_first_buy > MAX_PLAUSIBLE_MULTIPLIER:
        return True
    if multiplier_since_recommendation is not None and multiplier_since_recommendation > MAX_PLAUSIBLE_MULTIPLIER:
        return True
    if pc_h6 is not None and abs(pc_h6) > MAX_PLAUSIBLE_PC_H6:
        return True
    return False


def get_rugcheck_data(mint):
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


def get_top_holder_concentration(mint, pair_address=None):
    if not HELIUS_API_KEY:
        return None
    try:
        url = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"

        supply_resp = requests.post(
            url, json={"jsonrpc": "2.0", "id": 1, "method": "getTokenSupply", "params": [mint]},
            timeout=5
        )
        supply_data = supply_resp.json()
        supply_value = supply_data.get("result", {}).get("value", {}) or {}
        total_supply_ui = float(supply_value.get("uiAmount") or 0)
        if total_supply_ui <= 0:
            return None

        largest_resp = requests.post(
            url, json={"jsonrpc": "2.0", "id": 1, "method": "getTokenLargestAccounts", "params": [mint]},
            timeout=5
        )
        largest_data = largest_resp.json()
        accounts = largest_data.get("result", {}).get("value", []) or []
        if not accounts:
            return None

        token_account_addresses = [acc.get("address") for acc in accounts if acc.get("address")]
        owners_by_account = {}
        if token_account_addresses:
            info_resp = requests.post(
                url,
                json={
                    "jsonrpc": "2.0", "id": 1,
                    "method": "getMultipleAccounts",
                    "params": [token_account_addresses, {"encoding": "jsonParsed"}]
                },
                timeout=5
            )
            info_data = info_resp.json()
            values = info_data.get("result", {}).get("value", []) or []
            for addr, val in zip(token_account_addresses, values):
                if not val:
                    continue
                parsed = val.get("data", {}).get("parsed", {}) or {}
                owner = parsed.get("info", {}).get("owner")
                if owner:
                    owners_by_account[addr] = owner

        holdings_pct = []
        for acc in accounts:
            addr = acc.get("address")
            owner = owners_by_account.get(addr)

            if pair_address and owner == pair_address:
                continue

            ui_amt = float(acc.get("uiAmount") or 0)
            pct = (ui_amt / total_supply_ui) * 100 if total_supply_ui else 0
            holdings_pct.append(pct)

        if not holdings_pct:
            return None

        top1_pct = holdings_pct[0]
        top5_pct = sum(holdings_pct[:5])

        return {"top1_pct": top1_pct, "top5_pct": top5_pct}

    except Exception as e:
        print(f"Holder concentration error for {mint}: {e}")
        return None


def holder_concentration_label(data):
    if not data:
        return "⚪ Holder concentration: unable to check."
    top1 = data["top1_pct"]
    top5 = data["top5_pct"]
    if top1 >= 10:
        base = f"🔴 Top holder: {top1:.1f}% of supply (HIGH concentration risk)"
    elif top1 >= 7:
        base = f"🟡 Top holder: {top1:.1f}% of supply (moderate concentration)"
    else:
        base = f"🟢 Top holder: {top1:.1f}% of supply"
    base += f" | Top 5 combined: {top5:.1f}% (LP pool address excluded where identifiable)"
    return base


def get_wallet_cluster_count(mint, current_wallet):
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute("""
            SELECT DISTINCT wallet
            FROM wallet_token_history
            WHERE token_mint = %s
        """, (mint,))
        wallets = [row[0] for row in c.fetchall()]
        c.close()
        return wallets
    finally:
        conn.close()


def cluster_label(wallets, current_wallet):
    count = len(wallets)
    if count >= 3:
        return f"🔥🔥 STRONG CLUSTER: {count} tracked wallets bought this token independently!"
    elif count == 2:
        return f"🔥 Cluster detected: 2 tracked wallets bought this token independently."
    else:
        return "⚪ Single wallet signal — no other tracked wallets have bought this token yet."


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

                prior_liq_delta, prior_pc_5m = get_prior_scan_snapshot(mint)

                score, details = score_momentum(pair, liquidity_delta_pct, prior_liq_delta)
                current_liquidity = details.get("liquidity")

                multiplier_since_recommendation = None
                if price_at_recommendation and current_price:
                    try:
                        multiplier_since_recommendation = current_price / float(price_at_recommendation)
                    except (TypeError, ValueError, ZeroDivisionError):
                        multiplier_since_recommendation = None

                suspect = is_suspect_scan(
                    multiplier_from_first_buy,
                    multiplier_since_recommendation,
                    details.get("pc_h6")
                )

                if suspect:
                    print(f"⚠️ Suspect data for {mint}: mult_first={multiplier_from_first_buy}, "
                          f"mult_rec={multiplier_since_recommendation}, pc_h6={details.get('pc_h6')} "
                          f"— excluding from alerts/stats this scan")

                if current_price is not None and not suspect:
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
                elif current_price is not None and suspect:
                    c.execute(
                        """
                        UPDATE wallet_token_history
                        SET last_checked_at = NOW(),
                            last_liquidity = COALESCE(%s, last_liquidity)
                        WHERE wallet=%s AND token_mint=%s
                        """,
                        (current_liquidity, wallet, mint)
                    )

                momentum_alert_fired = False
                pump_alert_fired = False

                if not suspect and not momentum_alerted and score >= 70:

                    rug_score, rug_liq_flags = get_rugcheck_data(mint)
                    holder_data = get_top_holder_concentration(mint, pair.get("pairAddress"))
                    top1_pct = holder_data.get("top1_pct") if holder_data else None

                    rug_blocks = rug_score is not None and rug_score > 30
                    holder_blocks = top1_pct is not None and top1_pct >= 7

                    if rug_blocks or holder_blocks:
                        print(f"⛔ Recommendation blocked for {mint}: "
                              f"rug_score={rug_score} (blocks={rug_blocks}), "
                              f"top1_pct={top1_pct} (blocks={holder_blocks})")
                    else:
                        momentum_alert_fired = True

                        liq_trend_note = liquidity_trend_label(liquidity_delta_pct, prior_liq_delta)
                        price_trend_note = price_trend_label(details.get("pc_5m"), prior_pc_5m)
                        rug_note = rugcheck_label(rug_score, rug_liq_flags)
                        holder_note = holder_concentration_label(holder_data)
                        cluster_wallets = get_wallet_cluster_count(mint, wallet)
                        cluster_note = cluster_label(cluster_wallets, wallet)

                        c.execute(
                            """
                            UPDATE wallet_token_history
                            SET momentum_alerted = TRUE,
                                price_at_recommendation = %s,
                                recommended_at = NOW(),
                                market_cap_at_recommendation = %s,
                                rugcheck_score_at_recommendation = %s,
                                top1_holder_pct_at_recommendation = %s,
                                cluster_count_at_recommendation = %s
                            WHERE wallet=%s AND token_mint=%s
                            """,
                            (current_price, current_market_cap, rug_score,
                             top1_pct, len(cluster_wallets), wallet, mint)
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
                            f"{rug_note}\n"
                            f"{holder_note}\n"
                            f"{cluster_note}\n\n"
                            f"📊 DexScreener: {dexscreener_url}\n\n"
                            f"Recommending this now — tracking from this price to see if it delivers. DYOR."
                        )

                        send_bare_address_to_rick_chat(mint)

                elif (not suspect and not pumped_since_rec_alerted and price_at_recommendation
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
                        f"Type <code>/why {mint}</code> for the breakdown, or <code>/peak {mint}</code> "
                        f"anytime to check the all-time-high multiplier since this recommendation."
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
                         multiplier_since_recommendation, market_cap, suspect_data)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        wallet, mint, current_price, details.get("liquidity"),
                        details.get("vol_5m"), details.get("vol_h1"),
                        details.get("pc_5m"), details.get("pc_h1"), details.get("pc_h6"),
                        details.get("buys_5m"), details.get("sells_5m"), score,
                        multiplier_from_first_buy, drawdown, liquidity_delta_pct,
                        momentum_alert_fired, pump_alert_fired, multiplier_since_recommendation,
                        current_market_cap, suspect
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

    elif stripped.lower().startswith("/peak"):
        parts = stripped.split(maxsplit=1)
        if len(parts) < 2:
            send_telegram_alert("Give me a token address: <code>/peak &lt;mint_address&gt;</code>", chat_id)
        else:
            mint = parts[1].strip()
            conn = get_conn()
            try:
                c = conn.cursor()
                c.execute("""
                    SELECT price_at_recommendation, max_multiplier_since_recommendation,
                           pumped_since_recommendation_alerted
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
                    f"I never recommended <code>{mint}</code> — no ATH to report.",
                    chat_id
                )
            else:
                price_at_rec, max_mult, paid_off = row
                current_price = get_current_price(mint)
                current_mult_line = ""
                if current_price and price_at_rec:
                    try:
                        current_mult = current_price / float(price_at_rec)
                        current_mult_line = f"\nCurrently at: {current_mult:.2f}x"
                    except (TypeError, ValueError, ZeroDivisionError):
                        pass
                send_telegram_alert(
                    f"📈 <b>Peak since recommendation for {mint}:</b>\n"
                    f"All-time high: {f'{max_mult:.2f}x' if max_mult else 'n/a'}"
                    f"{current_mult_line}",
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
    since_param, until_param = get_date_filter_params()
    conn = get_conn()
    try:
        c = conn.cursor()

        token_filter = "WHERE 1=1"
        token_params = []
        if since_param:
            token_filter += " AND first_seen_at >= %s"
            token_params.append(since_param)
        if until_param:
            token_filter += " AND first_seen_at < %s"
            token_params.append(until_param)

        c.execute(f"SELECT COUNT(*) FROM wallet_token_history {token_filter}", token_params)
        total_tokens = c.fetchone()[0]

        c.execute(f"SELECT COUNT(*) FROM wallet_token_history {token_filter} AND max_multiplier_seen >= 3", token_params)
        pumped_3x = c.fetchone()[0]

        c.execute(f"SELECT COUNT(*) FROM wallet_token_history {token_filter} AND max_multiplier_seen >= 10", token_params)
        pumped_10x = c.fetchone()[0]

        c.execute(f"SELECT COUNT(*) FROM wallet_token_history {token_filter} AND max_drawdown_seen >= 0.8", token_params)
        rugged = c.fetchone()[0]

        c.execute("SELECT COUNT(*) FROM token_scan_log WHERE suspect_data IS NOT TRUE")
        total_scans = c.fetchone()[0]

        c.execute("SELECT COUNT(*) FROM token_scan_log WHERE suspect_data = TRUE")
        suspect_scans = c.fetchone()[0]

        c.execute("SELECT MIN(first_seen_at) FROM wallet_token_history")
        earliest = c.fetchone()[0]

        rec_filter = "WHERE momentum_alerted = TRUE"
        rec_params = []
        if since_param:
            rec_filter += " AND recommended_at >= %s"
            rec_params.append(since_param)
        if until_param:
            rec_filter += " AND recommended_at < %s"
            rec_params.append(until_param)

        c.execute(f"SELECT COUNT(*) FROM wallet_token_history {rec_filter}", rec_params)
        total_recommended = c.fetchone()[0]

        c.execute(f"SELECT COUNT(*) FROM wallet_token_history {rec_filter} AND pumped_since_recommendation_alerted = TRUE", rec_params)
        recommendations_that_paid_off = c.fetchone()[0]

        c.close()

        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"

        lines = [
            f"Tracking since: {earliest}{range_label}",
            f"Total tokens tracked: {total_tokens}",
            f"Total scan snapshots logged: {total_scans} ({suspect_scans} flagged as suspect/excluded)",
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
    since_param, until_param = get_date_filter_params()
    conn = get_conn()
    try:
        c = conn.cursor()

        date_filter_sql = ""
        params = []
        if since_param:
            date_filter_sql += " AND r.scanned_at >= %s"
            params.append(since_param)
        if until_param:
            date_filter_sql += " AND r.scanned_at < %s"
            params.append(until_param)

        c.execute(f"""
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
                WHERE s.suspect_data IS NOT TRUE
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
                {date_filter_sql}
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
        """, params)
        rows = c.fetchall()
        c.close()

        if not rows:
            return "No scan data yet under $100k starting liquidity — needs more small-cap tokens tracked first.", 200

        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"

        lines = [
            f"<b>Early-scan comparison (first 3 scans, tokens starting under $100k liquidity, suspect data excluded):</b>{range_label}<br>"
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
    since_param, until_param = get_date_filter_params()

    conn = get_conn()
    try:
        c = conn.cursor()

        query = """
            SELECT
                s.wallet, s.token_mint, s.momentum_score,
                h.max_multiplier_since_recommendation,
                h.pumped_since_recommendation_alerted
            FROM token_scan_log s
            JOIN wallet_token_history h
                ON h.wallet = s.wallet AND h.token_mint = s.token_mint
            WHERE s.momentum_alert_fired = TRUE
            AND s.suspect_data IS NOT TRUE
        """
        params = []
        if since_param:
            query += " AND h.recommended_at >= %s"
            params.append(since_param)
        if until_param:
            query += " AND h.recommended_at < %s"
            params.append(until_param)

        c.execute(query, params)
        rows = c.fetchall()
        c.close()

        if not rows:
            return "No recommendations logged in this date range.", 200

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

        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"

        lines = [f"<b>Recommendation hit-rate by score bucket (suspect data excluded):</b>{range_label}<br>"]
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
    since_param, until_param = get_date_filter_params()
    conn = get_conn()
    try:
        c = conn.cursor()

        date_filter_sql = ""
        params = []
        if since_param:
            date_filter_sql += " AND r.scanned_at >= %s"
            params.append(since_param)
        if until_param:
            date_filter_sql += " AND r.scanned_at < %s"
            params.append(until_param)

        c.execute(f"""
            WITH ranked_scans AS (
                SELECT
                    s.wallet, s.token_mint, s.liquidity_delta_pct, s.scanned_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY s.wallet, s.token_mint
                        ORDER BY s.scanned_at
                    ) AS scan_num,
                    h.max_multiplier_seen
                FROM token_scan_log s
                JOIN wallet_token_history h
                    ON h.wallet = s.wallet AND h.token_mint = s.token_mint
                WHERE s.suspect_data IS NOT TRUE
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
                {date_filter_sql}
            )
            SELECT
                COUNT(DISTINCT (wallet, token_mint)) AS total_pumped_tokens,
                COUNT(DISTINCT (wallet, token_mint)) FILTER (
                    WHERE liquidity_delta_pct < 0
                ) AS pumped_tokens_with_early_negative_delta
            FROM early_scans_of_pumped
        """, params)
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
    since_param, until_param = get_date_filter_params()
    conn = get_conn()
    try:
        c = conn.cursor()

        date_filter_sql = ""
        params = []
        if since_param:
            date_filter_sql += " AND r.scanned_at >= %s"
            params.append(since_param)
        if until_param:
            date_filter_sql += " AND r.scanned_at < %s"
            params.append(until_param)

        c.execute(f"""
            WITH ranked_scans AS (
                SELECT
                    s.wallet, s.token_mint, s.vol_5m, s.scanned_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY s.wallet, s.token_mint
                        ORDER BY s.scanned_at
                    ) AS scan_num,
                    h.max_multiplier_seen
                FROM token_scan_log s
                JOIN wallet_token_history h
                    ON h.wallet = s.wallet AND h.token_mint = s.token_mint
                WHERE s.suspect_data IS NOT TRUE
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
                {date_filter_sql}
            )
            SELECT
                MIN(vol_5m) AS min_vol,
                PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY vol_5m) AS p5_vol,
                PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY vol_5m) AS p10_vol,
                COUNT(*) AS total_scans
            FROM early_scans_of_pumped
        """, params)
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
    since_param, until_param = get_date_filter_params()
    conn = get_conn()
    try:
        c = conn.cursor()

        date_filter_sql = ""
        params = []
        if since_param:
            date_filter_sql += " AND h.recommended_at >= %s"
            params.append(since_param)
        if until_param:
            date_filter_sql += " AND h.recommended_at < %s"
            params.append(until_param)

        c.execute(f"""
            WITH ranked_scans AS (
                SELECT
                    s.wallet, s.token_mint, s.liquidity_delta_pct,
                    s.momentum_alert_fired, s.momentum_score,
                    ROW_NUMBER() OVER (
                        PARTITION BY s.wallet, s.token_mint
                        ORDER BY s.scanned_at
                    ) AS scan_num
                FROM token_scan_log s
                WHERE s.suspect_data IS NOT TRUE
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
            WHERE 1=1
            {date_filter_sql}
        """, params)
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
            "<b>90+ score recommendations, by liquidity pattern (suspect data excluded):</b><br>",
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


@app.route("/check-recency-bias")
def check_recency_bias():
    since_param, until_param = get_date_filter_params()
    conn = get_conn()
    try:
        c = conn.cursor()

        query = """
            SELECT
                s.momentum_score,
                h.recommended_at,
                h.max_multiplier_since_recommendation,
                h.pumped_since_recommendation_alerted
            FROM token_scan_log s
            JOIN wallet_token_history h
                ON h.wallet = s.wallet AND h.token_mint = s.token_mint
            WHERE s.momentum_alert_fired = TRUE
            AND s.suspect_data IS NOT TRUE
            AND h.recommended_at IS NOT NULL
        """
        params = []
        if since_param:
            query += " AND h.recommended_at >= %s"
            params.append(since_param)
        if until_param:
            query += " AND h.recommended_at < %s"
            params.append(until_param)

        c.execute(query, params)
        rows = c.fetchall()
        c.close()

        if not rows:
            return "No recommendations yet.", 200

        buckets = {
            "70-79": [],
            "80-89": [],
            "90-100": [],
        }

        for score, recommended_at, max_mult, hit_3x in rows:
            if score is None or recommended_at is None:
                continue
            if 70 <= score < 80:
                key = "70-79"
            elif 80 <= score < 90:
                key = "80-89"
            elif score >= 90:
                key = "90-100"
            else:
                continue
            buckets[key].append(recommended_at)

        now = datetime.datetime.utcnow()

        lines = ["<b>Average maturity time per score bucket:</b><br>"]
        for bucket, items in buckets.items():
            if not items:
                lines.append(f"<br>{bucket}: no data")
                continue
            hours_elapsed = [
                (now - r).total_seconds() / 3600
                for r in items
                if r is not None
            ]
            avg_hours = sum(hours_elapsed) / len(hours_elapsed) if hours_elapsed else 0
            lines.append(
                f"<br>Score {bucket}: {len(items)} recs, "
                f"avg age {avg_hours:.1f}h since recommendation"
            )

        lines.append(
            "<br><br>If avg age is similar across buckets, the hit-rate gap "
            "seen in /analyze-alerts is likely REAL, not just immaturity — "
            "worth investigating whether high scores are catching "
            "manipulation/oscillation patterns rather than genuine momentum."
        )
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_recency_bias error: {e}", 500

    finally:
        conn.close()


@app.route("/check-pump-retention")
def check_pump_retention():
    hours = request.args.get("hours", "6")
    try:
        hours = float(hours)
    except (TypeError, ValueError):
        hours = 6.0

    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute("""
            WITH scans AS (
                SELECT wallet, token_mint, scanned_at, multiplier_from_first_buy
                FROM token_scan_log
                WHERE multiplier_from_first_buy IS NOT NULL
                AND suspect_data IS NOT TRUE
            ),
            peak AS (
                SELECT DISTINCT ON (wallet, token_mint)
                    wallet, token_mint, scanned_at AS peak_at,
                    multiplier_from_first_buy AS peak_mult
                FROM scans
                ORDER BY wallet, token_mint, multiplier_from_first_buy DESC, scanned_at ASC
            ),
            qualifying AS (
                SELECT * FROM peak WHERE peak_mult >= 3
            ),
            latest AS (
                SELECT DISTINCT ON (wallet, token_mint)
                    wallet, token_mint, scanned_at AS latest_at,
                    multiplier_from_first_buy AS latest_mult
                FROM scans
                ORDER BY wallet, token_mint, scanned_at DESC
            )
            SELECT q.wallet, q.token_mint, q.peak_mult, q.peak_at,
                   l.latest_mult, l.latest_at
            FROM qualifying q
            JOIN latest l ON l.wallet = q.wallet AND l.token_mint = q.token_mint
            WHERE l.latest_at >= q.peak_at + (INTERVAL '1 hour' * %s)
        """, (hours,))
        rows = c.fetchall()
        c.close()

        if not rows:
            return (
                f"No tokens have both peaked at 3x+ AND had {hours}+ hours "
                f"pass since that peak yet. Try a smaller ?hours= value or "
                f"check back later.",
                200
            )

        held = 0
        dumped = 0
        for wallet, mint, peak_mult, peak_at, latest_mult, latest_at in rows:
            threshold = float(peak_mult) * 0.5
            if latest_mult is not None and float(latest_mult) >= threshold:
                held += 1
            else:
                dumped += 1

        total = held + dumped
        held_pct = (held / total * 100) if total else 0

        lines = [
            f"<b>Pump retention check (peak 3x+, {hours}+ hours after peak, suspect data excluded):</b><br>",
            f"<br>Total qualifying tokens: {total}",
            f"Still holding 50%+ of peak: {held} ({held_pct:.1f}%)",
            f"Dumped below 50% of peak: {dumped} ({100-held_pct:.1f}%)",
            "<br><br>Try different ?hours= values (e.g. ?hours=1, ?hours=12, "
            "?hours=24) to see how retention changes with more time elapsed.",
        ]
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_pump_retention error: {e}", 500

    finally:
        conn.close()


@app.route("/check-pump-retention-detail")
def check_pump_retention_detail():
    hours = request.args.get("hours", "6")
    only_held = request.args.get("only_held", "true").lower() == "true"
    try:
        hours = float(hours)
    except (TypeError, ValueError):
        hours = 6.0

    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute("""
            WITH scans AS (
                SELECT wallet, token_mint, scanned_at, multiplier_from_first_buy
                FROM token_scan_log
                WHERE multiplier_from_first_buy IS NOT NULL
                AND suspect_data IS NOT TRUE
            ),
            peak AS (
                SELECT DISTINCT ON (wallet, token_mint)
                    wallet, token_mint, scanned_at AS peak_at,
                    multiplier_from_first_buy AS peak_mult
                FROM scans
                ORDER BY wallet, token_mint, multiplier_from_first_buy DESC, scanned_at ASC
            ),
            qualifying AS (
                SELECT * FROM peak WHERE peak_mult >= 3
            ),
            latest AS (
                SELECT DISTINCT ON (wallet, token_mint)
                    wallet, token_mint, scanned_at AS latest_at,
                    multiplier_from_first_buy AS latest_mult
                FROM scans
                ORDER BY wallet, token_mint, scanned_at DESC
            )
            SELECT q.wallet, q.token_mint, q.peak_mult, q.peak_at,
                   l.latest_mult, l.latest_at
            FROM qualifying q
            JOIN latest l ON l.wallet = q.wallet AND l.token_mint = q.token_mint
            WHERE l.latest_at >= q.peak_at + (INTERVAL '1 hour' * %s)
            ORDER BY q.peak_mult DESC
        """, (hours,))
        rows = c.fetchall()
        c.close()

        if not rows:
            return f"No qualifying tokens for {hours}+ hours after peak.", 200

        lines = [f"<b>Pump retention detail ({hours}+ hours after peak, suspect data excluded):</b><br>"]
        shown = 0
        for wallet, mint, peak_mult, peak_at, latest_mult, latest_at in rows:
            threshold = float(peak_mult) * 0.5
            held = latest_mult is not None and float(latest_mult) >= threshold
            if only_held and not held:
                continue
            shown += 1
            retained_pct = (float(latest_mult) / float(peak_mult) * 100) if latest_mult else 0
            lines.append(
                f"<br><b>{'✅ HELD' if held else '❌ dumped'}</b> — "
                f"<code>{mint}</code><br>"
                f"Peak: {peak_mult:.2f}x at {peak_at} | "
                f"Now: {latest_mult:.2f}x ({retained_pct:.0f}% of peak retained)<br>"
                f"Wallet: <code>{wallet}</code>"
            )

        if shown == 0:
            lines.append("<br>No tokens matched the filter (try ?only_held=false to see all).")

        lines.append(
            f"<br><br>Showing {shown} of {len(rows)} total qualifying tokens. "
            f"Use ?only_held=false to see dumped ones too, or ?hours= to change the window."
        )
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_pump_retention_detail error: {e}", 500

    finally:
        conn.close()


@app.route("/check-extension-risk")
def check_extension_risk():
    since_param, until_param = get_date_filter_params()
    conn = get_conn()
    try:
        c = conn.cursor()

        query = """
            SELECT s.momentum_score, s.multiplier_from_first_buy
            FROM token_scan_log s
            WHERE s.momentum_alert_fired = TRUE
            AND s.multiplier_from_first_buy IS NOT NULL
            AND s.suspect_data IS NOT TRUE
        """
        params = []
        if since_param:
            query += " AND s.scanned_at >= %s"
            params.append(since_param)
        if until_param:
            query += " AND s.scanned_at < %s"
            params.append(until_param)

        c.execute(query, params)
        rows = c.fetchall()
        c.close()

        if not rows:
            return "No recommendation data with multiplier_from_first_buy yet.", 200

        buckets = {
            "70-79": [],
            "80-89": [],
            "90-100": [],
        }

        for score, mult in rows:
            if score is None or mult is None:
                continue
            if 70 <= score < 80:
                key = "70-79"
            elif 80 <= score < 90:
                key = "80-89"
            elif score >= 90:
                key = "90-100"
            else:
                continue
            buckets[key].append(float(mult))

        def median(vals):
            s = sorted(vals)
            n = len(s)
            if n == 0:
                return None
            mid = n // 2
            if n % 2 == 0:
                return (s[mid - 1] + s[mid]) / 2
            return s[mid]

        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"

        lines = [f"<b>Multiplier-from-first-buy AT RECOMMENDATION, by score bucket (suspect data excluded):</b>{range_label}<br>"]
        for bucket, vals in buckets.items():
            if not vals:
                lines.append(f"<br>{bucket}: no data")
                continue
            avg_mult = sum(vals) / len(vals)
            med_mult = median(vals)
            lines.append(
                f"<br>Score {bucket}: n={len(vals)}, "
                f"avg {avg_mult:.2f}x, median {med_mult:.2f}x already run up "
                f"before recommendation"
            )

        lines.append(
            "<br><br>If 90-100 shows a noticeably higher avg/median multiplier "
            "than 70-79, that means high scores are systematically catching "
            "tokens further into an already-extended move — i.e. closer to "
            "the top, not the start — which would explain poor hit-rate "
            "independent of any liquidity-oscillation pattern."
        )
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_extension_risk error: {e}", 500

    finally:
        conn.close()


@app.route("/check-retention-patterns")
def check_retention_patterns():
    hours = request.args.get("hours", "6")
    try:
        hours = float(hours)
    except (TypeError, ValueError):
        hours = 6.0

    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute("""
            WITH scans AS (
                SELECT wallet, token_mint, scanned_at, multiplier_from_first_buy
                FROM token_scan_log
                WHERE multiplier_from_first_buy IS NOT NULL
                AND suspect_data IS NOT TRUE
            ),
            peak AS (
                SELECT DISTINCT ON (wallet, token_mint)
                    wallet, token_mint, scanned_at AS peak_at,
                    multiplier_from_first_buy AS peak_mult
                FROM scans
                ORDER BY wallet, token_mint, multiplier_from_first_buy DESC, scanned_at ASC
            ),
            qualifying AS (
                SELECT * FROM peak WHERE peak_mult >= 3
            ),
            latest AS (
                SELECT DISTINCT ON (wallet, token_mint)
                    wallet, token_mint, scanned_at AS latest_at,
                    multiplier_from_first_buy AS latest_mult
                FROM scans
                ORDER BY wallet, token_mint, scanned_at DESC
            ),
            outcomes AS (
                SELECT q.wallet, q.token_mint, q.peak_mult, q.peak_at,
                       l.latest_mult, l.latest_at,
                       CASE WHEN l.latest_mult >= q.peak_mult * 0.5
                            THEN 'held' ELSE 'dumped' END AS outcome
                FROM qualifying q
                JOIN latest l ON l.wallet = q.wallet AND l.token_mint = q.token_mint
                WHERE l.latest_at >= q.peak_at + (INTERVAL '1 hour' * %s)
            ),
            recommendation_scan AS (
                SELECT
                    s.wallet, s.token_mint, s.momentum_score, s.liquidity,
                    s.liquidity_delta_pct, s.vol_5m, s.vol_h1,
                    s.pc_5m, s.pc_h1, s.pc_h6
                FROM token_scan_log s
                WHERE s.momentum_alert_fired = TRUE
                AND s.suspect_data IS NOT TRUE
            ),
            joined AS (
                SELECT o.outcome, r.momentum_score, r.liquidity,
                       r.liquidity_delta_pct, r.vol_5m, r.vol_h1,
                       r.pc_5m, r.pc_h1, r.pc_h6
                FROM outcomes o
                JOIN recommendation_scan r
                    ON r.wallet = o.wallet AND r.token_mint = o.token_mint
            )
            SELECT
                outcome,
                COUNT(*) AS n,
                AVG(momentum_score) AS avg_score,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY momentum_score) AS median_score,
                AVG(liquidity) AS avg_liquidity,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY liquidity) AS median_liquidity,
                AVG(liquidity_delta_pct) AS avg_liq_delta,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY liquidity_delta_pct) AS median_liq_delta,
                AVG(vol_5m) AS avg_vol_5m,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vol_5m) AS median_vol_5m,
                AVG(vol_h1) AS avg_vol_h1,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vol_h1) AS median_vol_h1,
                AVG(pc_5m) AS avg_pc_5m,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pc_5m) AS median_pc_5m,
                AVG(pc_h1) AS avg_pc_h1,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pc_h1) AS median_pc_h1,
                AVG(pc_h6) AS avg_pc_h6,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pc_h6) AS median_pc_h6
            FROM joined
            GROUP BY outcome
        """, (hours,))
        rows = c.fetchall()
        c.close()

        if not rows:
            return f"No qualifying tokens for {hours}+ hours after peak with recommendation data.", 200

        cols = [
            "outcome", "n",
            "avg_score", "median_score",
            "avg_liquidity", "median_liquidity",
            "avg_liq_delta", "median_liq_delta",
            "avg_vol_5m", "median_vol_5m",
            "avg_vol_h1", "median_vol_h1",
            "avg_pc_5m", "median_pc_5m",
            "avg_pc_h1", "median_pc_h1",
            "avg_pc_h6", "median_pc_h6",
        ]
        data = {}
        for row in rows:
            d = dict(zip(cols, row))
            data[d["outcome"]] = d

        def fmt(val, kind="num"):
            if val is None:
                return "n/a"
            if kind == "pct":
                return f"{val*100:.1f}%"
            if kind == "usd":
                return f"${val:,.0f}"
            return f"{val:.2f}"

        lines = [f"<b>Retention patterns at recommendation time ({hours}+ hours after peak, suspect data excluded):</b><br>"]
        for outcome in ["held", "dumped"]:
            d = data.get(outcome)
            if not d:
                lines.append(f"<br>{outcome.upper()}: no examples")
                continue
            lines.append(f"<br><b>{outcome.upper()}</b> (n={d['n']})")
            lines.append(f"Momentum score — avg: {fmt(d['avg_score'])}, median: {fmt(d['median_score'])}")
            lines.append(f"Liquidity — avg: {fmt(d['avg_liquidity'], 'usd')}, median: {fmt(d['median_liquidity'], 'usd')}")
            lines.append(f"Liquidity delta — avg: {fmt(d['avg_liq_delta'], 'pct')}, median: {fmt(d['median_liq_delta'], 'pct')}")
            lines.append(f"5m volume — avg: {fmt(d['avg_vol_5m'], 'usd')}, median: {fmt(d['median_vol_5m'], 'usd')}")
            lines.append(f"1h volume — avg: {fmt(d['avg_vol_h1'], 'usd')}, median: {fmt(d['median_vol_h1'], 'usd')}")
            lines.append(f"Price change 5m — avg: {fmt(d['avg_pc_5m'])}%, median: {fmt(d['median_pc_5m'])}%")
            lines.append(f"Price change 1h — avg: {fmt(d['avg_pc_h1'])}%, median: {fmt(d['median_pc_h1'])}%")
            lines.append(f"Price change 6h — avg: {fmt(d['avg_pc_h6'])}%, median: {fmt(d['median_pc_h6'])}%")

        lines.append("<br><br>Compare medians especially — averages can be "
                      "distorted by a single extreme outlier. A metric where "
                      "BOTH avg and median show a consistent gap is the most "
                      "trustworthy candidate signal.")
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_retention_patterns error: {e}", 500

    finally:
        conn.close()


@app.route("/backfill-suspect-data", methods=["GET", "POST"])
def backfill_suspect_data():
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute("""
            SELECT id, multiplier_from_first_buy, multiplier_since_recommendation, pc_h6
            FROM token_scan_log
            WHERE suspect_data IS NOT TRUE
        """)
        rows = c.fetchall()

        flagged = 0
        checked = 0

        for row_id, mult_first, mult_rec, pc_h6 in rows:
            checked += 1
            mult_first_f = float(mult_first) if mult_first is not None else None
            mult_rec_f = float(mult_rec) if mult_rec is not None else None
            pc_h6_f = float(pc_h6) if pc_h6 is not None else None

            if is_suspect_scan(mult_first_f, mult_rec_f, pc_h6_f):
                c.execute(
                    "UPDATE token_scan_log SET suspect_data = TRUE WHERE id = %s",
                    (row_id,)
                )
                flagged += 1

        conn.commit()
        c.close()

        return (
            f"Backfill complete — checked {checked} existing scans, "
            f"newly flagged {flagged} as suspect data (multiplier > "
            f"{MAX_PLAUSIBLE_MULTIPLIER}x or 6h price change beyond "
            f"±{MAX_PLAUSIBLE_PC_H6}%). These are now excluded from all "
            f"analysis endpoints going forward.",
            200
        )

    except Exception as e:
        return f"backfill_suspect_data error: {e}", 500

    finally:
        conn.close()


@app.route("/check-pump-timing-risk")
def check_pump_timing_risk():
    since_param, until_param = get_date_filter_params()
    conn = get_conn()
    try:
        c = conn.cursor()

        query = """
            SELECT s.pc_h1, h.max_multiplier_since_recommendation,
                   h.pumped_since_recommendation_alerted
            FROM token_scan_log s
            JOIN wallet_token_history h
                ON h.wallet = s.wallet AND h.token_mint = s.token_mint
            WHERE s.momentum_alert_fired = TRUE
            AND s.suspect_data IS NOT TRUE
            AND s.pc_h1 IS NOT NULL
        """
        params = []
        if since_param:
            query += " AND h.recommended_at >= %s"
            params.append(since_param)
        if until_param:
            query += " AND h.recommended_at < %s"
            params.append(until_param)

        c.execute(query, params)
        rows = c.fetchall()
        c.close()

        if not rows:
            return "No recommendation data with pc_h1 yet.", 200

        buckets = {
            "under 50%": {"total": 0, "hit_3x": 0},
            "50-150%": {"total": 0, "hit_3x": 0},
            "150-300%": {"total": 0, "hit_3x": 0},
            "over 300%": {"total": 0, "hit_3x": 0},
        }

        for pc_h1, max_mult, hit_3x in rows:
            pc_h1 = float(pc_h1)
            if pc_h1 < 50:
                key = "under 50%"
            elif pc_h1 < 150:
                key = "50-150%"
            elif pc_h1 < 300:
                key = "150-300%"
            else:
                key = "over 300%"

            buckets[key]["total"] += 1
            hit = bool(hit_3x) or (max_mult and max_mult >= 3)
            if hit:
                buckets[key]["hit_3x"] += 1

        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"

        lines = [f"<b>Recommendation hit-rate by 1h price-change AT RECOMMENDATION:</b>{range_label}<br>"]
        for bucket, d in buckets.items():
            total = d["total"]
            hits = d["hit_3x"]
            rate = f"{hits/total*100:.1f}%" if total else "n/a"
            lines.append(f"<br>Already up {bucket} in the last hour: {hits}/{total} hit 3x+ ({rate})")

        lines.append(
            "<br><br>If lower pc_h1 buckets (under 50%, 50-150%) show "
            "meaningfully HIGHER hit rates than the high buckets (150-300%, "
            "over 300%), that confirms tokens are being caught too late — "
            "already extended before recommendation — and a pc_h1 cap "
            "should be added to the score."
        )
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_pump_timing_risk error: {e}", 500

    finally:
        conn.close()


@app.route("/check-pump-timing-risk-6h")
def check_pump_timing_risk_6h():
    since_param, until_param = get_date_filter_params()
    conn = get_conn()
    try:
        c = conn.cursor()

        query = """
            SELECT s.pc_h6, h.max_multiplier_since_recommendation,
                   h.pumped_since_recommendation_alerted
            FROM token_scan_log s
            JOIN wallet_token_history h
                ON h.wallet = s.wallet AND h.token_mint = s.token_mint
            WHERE s.momentum_alert_fired = TRUE
            AND s.suspect_data IS NOT TRUE
            AND s.pc_h6 IS NOT NULL
        """
        params = []
        if since_param:
            query += " AND h.recommended_at >= %s"
            params.append(since_param)
        if until_param:
            query += " AND h.recommended_at < %s"
            params.append(until_param)

        c.execute(query, params)
        rows = c.fetchall()
        c.close()

        if not rows:
            return "No recommendation data with pc_h6 yet.", 200

        buckets = {
            "under 100%": {"total": 0, "hit_3x": 0},
            "100-300%": {"total": 0, "hit_3x": 0},
            "300-600%": {"total": 0, "hit_3x": 0},
            "over 600%": {"total": 0, "hit_3x": 0},
        }

        for pc_h6, max_mult, hit_3x in rows:
            pc_h6 = float(pc_h6)
            if pc_h6 < 100:
                key = "under 100%"
            elif pc_h6 < 300:
                key = "100-300%"
            elif pc_h6 < 600:
                key = "300-600%"
            else:
                key = "over 600%"

            buckets[key]["total"] += 1
            hit = bool(hit_3x) or (max_mult and max_mult >= 3)
            if hit:
                buckets[key]["hit_3x"] += 1

        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"

        lines = [f"<b>Recommendation hit-rate by 6h price-change AT RECOMMENDATION:</b>{range_label}<br>"]
        for bucket, d in buckets.items():
            total = d["total"]
            hits = d["hit_3x"]
            rate = f"{hits/total*100:.1f}%" if total else "n/a"
            lines.append(f"<br>Already up {bucket} in the last 6h: {hits}/{total} hit 3x+ ({rate})")

        lines.append(
            "<br><br>Same idea as the 1h check, but over a longer window — "
            "if lower pc_h6 buckets show meaningfully HIGHER hit rates than "
            "the high buckets, that's a cleaner signal that tokens are being "
            "caught too late in their run."
        )
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_pump_timing_risk_6h error: {e}", 500

    finally:
        conn.close()


@app.route("/check-recommendation-value")
def check_recommendation_value():
    since_param, until_param = get_date_filter_params()
    conn = get_conn()
    try:
        c = conn.cursor()

        token_filter = "WHERE 1=1"
        params = []
        if since_param:
            token_filter += " AND first_seen_at >= %s"
            params.append(since_param)
        if until_param:
            token_filter += " AND first_seen_at < %s"
            params.append(until_param)

        c.execute(f"""
            SELECT
                momentum_alerted,
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE COALESCE(max_multiplier_seen, 0) >= 3) AS hit_3x
            FROM wallet_token_history
            {token_filter}
            GROUP BY momentum_alerted
        """, params)
        rows = c.fetchall()
        c.close()

        if not rows:
            return "No token data in this range.", 200

        data = {}
        for was_recommended, total, hit_3x in rows:
            data[bool(was_recommended)] = {"total": total, "hit_3x": hit_3x}

        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"

        lines = [f"<b>Hit-rate-from-first-buy: recommended vs never-recommended</b>{range_label}<br>"]

        recommended = data.get(True, {"total": 0, "hit_3x": 0})
        not_recommended = data.get(False, {"total": 0, "hit_3x": 0})

        rec_rate = (recommended["hit_3x"] / recommended["total"] * 100) if recommended["total"] else 0
        not_rec_rate = (not_recommended["hit_3x"] / not_recommended["total"] * 100) if not_recommended["total"] else 0

        lines.append(
            f"<br><b>RECOMMENDED</b> (score crossed 70 at some point): "
            f"{recommended['hit_3x']}/{recommended['total']} hit 3x+ from first buy ({rec_rate:.1f}%)"
        )
        lines.append(
            f"<br><b>NEVER RECOMMENDED</b>: "
            f"{not_recommended['hit_3x']}/{not_recommended['total']} hit 3x+ from first buy ({not_rec_rate:.1f}%)"
        )

        lines.append("<br><br>")
        if rec_rate > not_rec_rate:
            lines.append(
                f"✅ Recommended tokens hit 3x+ from first buy at a HIGHER rate "
                f"than never-recommended ones — the recommendation system is "
                f"genuinely concentrating winners, using the same baseline for both groups."
            )
        else:
            lines.append(
                f"⚠️ Recommended tokens do NOT outperform never-recommended ones "
                f"on this fair, same-baseline comparison — worth investigating "
                f"further before trusting the current scoring threshold."
            )

        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_recommendation_value error: {e}", 500

    finally:
        conn.close()


@app.route("/check-wallet-performance")
def check_wallet_performance():
    since_param, until_param = get_date_filter_params()
    conn = get_conn()
    try:
        c = conn.cursor()

        query = """
            SELECT
                wallet,
                COUNT(*) AS total_recommended,
                COUNT(*) FILTER (WHERE pumped_since_recommendation_alerted = TRUE
                                  OR COALESCE(max_multiplier_since_recommendation, 0) >= 3) AS hit_3x
            FROM wallet_token_history
            WHERE momentum_alerted = TRUE
        """
        params = []
        if since_param:
            query += " AND recommended_at >= %s"
            params.append(since_param)
        if until_param:
            query += " AND recommended_at < %s"
            params.append(until_param)
        query += " GROUP BY wallet ORDER BY total_recommended DESC"

        c.execute(query, params)
        rows = c.fetchall()
        c.close()

        if not rows:
            return "No recommendation data yet.", 200

        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"

        lines = [f"<b>Recommendation hit-rate by tracked wallet:</b>{range_label}<br>"]
        for wallet, total, hits in rows:
            rate = f"{hits/total*100:.1f}%" if total else "n/a"
            lines.append(
                f"<br><code>{wallet}</code><br>"
                f"{hits}/{total} recommendations hit 3x+ ({rate})"
            )

        lines.append(
            "<br><br>If one wallet's hit rate is meaningfully lower than the "
            "others, its buys may be adding noise rather than signal. If one "
            "is meaningfully higher, that wallet's calls deserve extra "
            "weight or priority when sourcing new wallets to track."
        )
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_wallet_performance error: {e}", 500

    finally:
        conn.close()


@app.route("/check-time-of-day")
def check_time_of_day():
    since_param, until_param = get_date_filter_params()
    conn = get_conn()
    try:
        c = conn.cursor()

        query = """
            SELECT recommended_at, max_multiplier_since_recommendation,
                   pumped_since_recommendation_alerted
            FROM wallet_token_history
            WHERE momentum_alerted = TRUE
            AND recommended_at IS NOT NULL
        """
        params = []
        if since_param:
            query += " AND recommended_at >= %s"
            params.append(since_param)
        if until_param:
            query += " AND recommended_at < %s"
            params.append(until_param)

        c.execute(query, params)
        rows = c.fetchall()
        c.close()

        if not rows:
            return "No recommendation data yet.", 200

        buckets = {
            "00-05 UTC": {"total": 0, "hit_3x": 0},
            "06-11 UTC": {"total": 0, "hit_3x": 0},
            "12-17 UTC": {"total": 0, "hit_3x": 0},
            "18-23 UTC": {"total": 0, "hit_3x": 0},
        }

        for recommended_at, max_mult, hit_3x in rows:
            if recommended_at is None:
                continue
            hour = recommended_at.hour
            if hour < 6:
                key = "00-05 UTC"
            elif hour < 12:
                key = "06-11 UTC"
            elif hour < 18:
                key = "12-17 UTC"
            else:
                key = "18-23 UTC"

            buckets[key]["total"] += 1
            hit = bool(hit_3x) or (max_mult and max_mult >= 3)
            if hit:
                buckets[key]["hit_3x"] += 1

        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"

        lines = [f"<b>Recommendation hit-rate by time of day (UTC):</b>{range_label}<br>"]
        for bucket, d in buckets.items():
            total = d["total"]
            hits = d["hit_3x"]
            rate = f"{hits/total*100:.1f}%" if total else "n/a"
            lines.append(f"<br>{bucket}: {hits}/{total} hit 3x+ ({rate})")

        lines.append(
            "<br><br>If one window shows a meaningfully higher/lower rate "
            "with a large enough sample, that could reflect real "
            "differences in market activity/liquidity at that time — but "
            "check sample sizes per bucket before trusting any gap."
        )
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_time_of_day error: {e}", 500

    finally:
        conn.close()


@app.route("/check-cluster-performance")
def check_cluster_performance():
    since_param, until_param = get_date_filter_params()
    conn = get_conn()
    try:
        c = conn.cursor()

        query = """
            SELECT cluster_count_at_recommendation,
                   max_multiplier_since_recommendation,
                   pumped_since_recommendation_alerted
            FROM wallet_token_history
            WHERE momentum_alerted = TRUE
            AND cluster_count_at_recommendation IS NOT NULL
        """
        params = []
        if since_param:
            query += " AND recommended_at >= %s"
            params.append(since_param)
        if until_param:
            query += " AND recommended_at < %s"
            params.append(until_param)

        c.execute(query, params)
        rows = c.fetchall()
        c.close()

        if not rows:
            return "No recommendation data with cluster tracking yet.", 200

        single = {"total": 0, "hit_3x": 0}
        cluster = {"total": 0, "hit_3x": 0}

        for cluster_count, max_mult, hit_3x in rows:
            hit = bool(hit_3x) or (max_mult and max_mult >= 3)
            if cluster_count and cluster_count >= 2:
                cluster["total"] += 1
                if hit:
                    cluster["hit_3x"] += 1
            else:
                single["total"] += 1
                if hit:
                    single["hit_3x"] += 1

        def rate(d):
            return f"{d['hit_3x']}/{d['total']} ({d['hit_3x']/d['total']*100:.1f}%)" if d["total"] else "n/a"

        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"

        lines = [f"<b>Recommendation hit-rate: cluster vs single-wallet</b>{range_label}<br>"]
        lines.append(f"<br>Single wallet (1 tracked wallet bought): {rate(single)}")
        lines.append(f"Cluster (2+ tracked wallets bought): {rate(cluster)}")
        lines.append(
            "<br><br>If cluster hit rate is meaningfully higher AND the "
            "sample size is large enough (20-30+), that validates adding a "
            "score boost for clusters. Too early to trust with a small sample."
        )
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_cluster_performance error: {e}", 500

    finally:
        conn.close()


@app.route("/check-rugcheck-vs-outcome")
def check_rugcheck_vs_outcome():
    since_param, until_param = get_date_filter_params()
    conn = get_conn()
    try:
        c = conn.cursor()

        query = """
            SELECT rugcheck_score_at_recommendation,
                   max_multiplier_since_recommendation,
                   pumped_since_recommendation_alerted
            FROM wallet_token_history
            WHERE momentum_alerted = TRUE
            AND rugcheck_score_at_recommendation IS NOT NULL
        """
        params = []
        if since_param:
            query += " AND recommended_at >= %s"
            params.append(since_param)
        if until_param:
            query += " AND recommended_at < %s"
            params.append(until_param)

        c.execute(query, params)
        rows = c.fetchall()
        c.close()

        if not rows:
            return "No recommendation data with RugCheck score yet.", 200

        buckets = {
            "0-30 (low risk)": {"total": 0, "hit_3x": 0},
            "31-60 (moderate risk)": {"total": 0, "hit_3x": 0},
            "61-100 (high risk)": {"total": 0, "hit_3x": 0},
        }

        for rug_score, max_mult, hit_3x in rows:
            rug_score = float(rug_score)
            if rug_score <= 30:
                key = "0-30 (low risk)"
            elif rug_score <= 60:
                key = "31-60 (moderate risk)"
            else:
                key = "61-100 (high risk)"

            buckets[key]["total"] += 1
            hit = bool(hit_3x) or (max_mult and max_mult >= 3)
            if hit:
                buckets[key]["hit_3x"] += 1

        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"

        lines = [f"<b>Recommendation hit-rate by RugCheck score AT RECOMMENDATION:</b>{range_label}<br>"]
        for bucket, d in buckets.items():
            total = d["total"]
            hits = d["hit_3x"]
            rate = f"{hits/total*100:.1f}%" if total else "n/a"
            lines.append(f"<br>{bucket}: {hits}/{total} hit 3x+ ({rate})")

        lines.append(
            "<br><br>If low-risk (0-30) shows a meaningfully HIGHER hit rate "
            "than high-risk (61-100), that validates using RugCheck as a "
            "real scoring factor. If rates are similar, RugCheck score isn't "
            "adding predictive value for this bot's use case."
        )
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_rugcheck_vs_outcome error: {e}", 500

    finally:
        conn.close()


@app.route("/check-holder-vs-outcome")
def check_holder_vs_outcome():
    since_param, until_param = get_date_filter_params()
    conn = get_conn()
    try:
        c = conn.cursor()

        query = """
            SELECT top1_holder_pct_at_recommendation,
                   max_multiplier_since_recommendation,
                   pumped_since_recommendation_alerted
            FROM wallet_token_history
            WHERE momentum_alerted = TRUE
            AND top1_holder_pct_at_recommendation IS NOT NULL
        """
        params = []
        if since_param:
            query += " AND recommended_at >= %s"
            params.append(since_param)
        if until_param:
            query += " AND recommended_at < %s"
            params.append(until_param)

        c.execute(query, params)
        rows = c.fetchall()
        c.close()

        if not rows:
            return "No recommendation data with holder concentration yet.", 200

        buckets = {
            "under 5%": {"total": 0, "hit_3x": 0},
            "5-7%": {"total": 0, "hit_3x": 0},
            "7-10%": {"total": 0, "hit_3x": 0},
            "over 10%": {"total": 0, "hit_3x": 0},
        }

        for top1_pct, max_mult, hit_3x in rows:
            top1_pct = float(top1_pct)
            if top1_pct < 5:
                key = "under 5%"
            elif top1_pct < 7:
                key = "5-7%"
            elif top1_pct < 10:
                key = "7-10%"
            else:
                key = "over 10%"

            buckets[key]["total"] += 1
            hit = bool(hit_3x) or (max_mult and max_mult >= 3)
            if hit:
                buckets[key]["hit_3x"] += 1

        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"

        lines = [f"<b>Recommendation hit-rate by top1 holder % AT RECOMMENDATION:</b>{range_label}<br>"]
        for bucket, d in buckets.items():
            total = d["total"]
            hits = d["hit_3x"]
            rate = f"{hits/total*100:.1f}%" if total else "n/a"
            lines.append(f"<br>Top holder {bucket}: {hits}/{total} hit 3x+ ({rate})")

        lines.append(
            "<br><br>If lower-concentration buckets show meaningfully "
            "HIGHER hit rates than the over-10% bucket, that validates the "
            "current 7%/10% alert thresholds and suggests holder "
            "concentration should factor into the score directly, not just "
            "be shown as an info label."
        )
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_holder_vs_outcome error: {e}", 500

    finally:
        conn.close()


@app.route("/recommendation/<mint>")
def recommendation_lookup(mint):
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute("""
            SELECT wallet, price_at_recommendation, recommended_at,
                   max_multiplier_since_recommendation,
                   pumped_since_recommendation_alerted,
                   market_cap_at_recommendation
            FROM wallet_token_history
            WHERE token_mint = %s AND price_at_recommendation IS NOT NULL
            ORDER BY recommended_at DESC
            LIMIT 1
        """, (mint,))
        row = c.fetchone()
        c.close()

        if not row:
            return f"No recommendation found for {mint} — this token was never recommended.", 200

        (wallet, price_at_rec, recommended_at, max_mult,
         paid_off, market_cap_at_rec) = row

        current_price = get_current_price(mint)
        current_mult = None
        if current_price and price_at_rec:
            try:
                current_mult = current_price / float(price_at_rec)
            except (TypeError, ValueError, ZeroDivisionError):
                current_mult = None

        lines = [
            f"<b>Recommendation lookup: {mint}</b><br>",
            f"<br>Wallet: <code>{wallet}</code>",
            f"Recommended at: {recommended_at}",
            f"Price at recommendation: ${price_at_rec}",
            f"Market cap at recommendation: ${float(market_cap_at_rec):,.0f}" if market_cap_at_rec else "Market cap at recommendation: n/a",
            "",
            f"<b>ALL-TIME HIGH since recommendation: {f'{max_mult:.2f}x' if max_mult else 'n/a'}</b>",
            f"Current multiplier: {f'{current_mult:.2f}x' if current_mult else 'n/a'} (price now: ${current_price if current_price else 'n/a'})",
            f"3x confirmation fired: {'Yes' if paid_off else 'No'}",
        ]
        return "<br>".join(lines), 200

    except Exception as e:
        return f"recommendation_lookup error: {e}", 500

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
                   multiplier_since_recommendation, market_cap, suspect_data
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
             mult_since_rec, market_cap, suspect) = row

            flags = []
            if mom_fired:
                flags.append("🚀 RECOMMENDED HERE")
            if pump_fired:
                flags.append("🎯 3x-SINCE-RECOMMENDATION FIRED")
            if suspect:
                flags.append("⚠️ SUSPECT DATA (excluded from analysis)")
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
