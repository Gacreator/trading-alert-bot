from flask import Flask, request
import os
import re
import time
import datetime
import json
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
import psycopg2
from psycopg2 import pool as pg_pool
import requests

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_IDS = [
    cid.strip() for cid in os.environ.get("TELEGRAM_CHAT_ID", "").split(",") if cid.strip()
]
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
HELIUS_API_KEY = os.environ.get("HELIUS_API_KEY")
JUPITER_API_KEY = os.environ.get("JUPITER_API_KEY")
RUGCHECK_API_KEY = os.environ.get("RUGCHECK_API_KEY")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET")
TELEGRAM_WEBHOOK_SECRET = os.environ.get("TELEGRAM_WEBHOOK_SECRET")

TRACKED_WALLETS = set(
    w.strip() for w in os.environ.get("TRACKED_WALLETS", "").split(",") if w.strip()
)

MIN_LIQUIDITY_USD = int(os.environ.get("MIN_LIQUIDITY_USD", "8000"))
WSOL_MINT = "So11111111111111111111111111111111111111112"

SCAN_WINDOW_HOURS = int(os.environ.get("SCAN_WINDOW_HOURS", "48"))
MAX_CONCURRENT_DEXSCREENER = int(os.environ.get("MAX_CONCURRENT_DEXSCREENER", "5"))
DB_CONN_REFRESH_EVERY = int(os.environ.get("DB_CONN_REFRESH_EVERY", "150"))

MAX_PLAUSIBLE_MULTIPLIER = float(os.environ.get("MAX_PLAUSIBLE_MULTIPLIER", "50"))
MAX_PLAUSIBLE_PC_H6 = float(os.environ.get("MAX_PLAUSIBLE_PC_H6", "20000"))

QUEEN_SYSTEM_PROMPT = (
    "You are 'Queen' — the user's witty, confident friend who happens to run a Solana trading "
    "alert bot. You talk to the user like a close friend, not a subject or servant — no 'my loyal "
    "subject', no 'thee/thou', no medieval decree language. You're modern, sharp-tongued, a little "
    "dramatic, and you know you're good at what you do, but it comes through as confidence and "
    "banter between friends, not royal distance. Keep responses short and casual (2-4 sentences) "
    "since this is a Telegram chat. Never break character, but stay strictly accurate to any facts "
    "given to you — never invent usernames, links, or data that wasn't provided."
)

QUEEN_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "check_recommendation",
            "description": "Look up whether a specific token mint address was recommended, and its current status.",
            "parameters": {
                "type": "object",
                "properties": {
                    "mint": {
                        "type": "string",
                        "description": "The Solana token mint address to look up"
                    }
                },
                "required": ["mint"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "check_paper_trading_summary",
            "description": "Get a summary of the paper-trading agent's current performance, including open and closed positions, win rate, and average return.",
            "parameters": {
                "type": "object",
                "properties": {}
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "check_current_price",
            "description": "Get the current live price and market cap of a specific token mint address.",
            "parameters": {
                "type": "object",
                "properties": {
                    "mint": {
                        "type": "string",
                        "description": "The Solana token mint address to check"
                    }
                },
                "required": ["mint"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "check_bot_stats",
            "description": "Get overall bot statistics: total tokens tracked, total recommendations made, and recommendation hit rate.",
            "parameters": {
                "type": "object",
                "properties": {}
            }
        }
    }
]

SOLANA_ADDRESS_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")

_check_pumps_lock = threading.Lock()
_check_pumps_lock_time = None
_check_pumps_run_id = None
_dex_rate_lock = threading.Semaphore(MAX_CONCURRENT_DEXSCREENER)

_connection_pool = pg_pool.ThreadedConnectionPool(
    minconn=2,
    maxconn=20,
    dsn=DATABASE_URL,
    connect_timeout=10,
    keepalives=1,
    keepalives_idle=30,
    keepalives_interval=10,
    keepalives_count=5,
)


def get_conn():
    return _connection_pool.getconn()


def put_conn(conn, close=False):
    try:
        _connection_pool.putconn(conn, close=close)
    except Exception:
        pass


def looks_like_solana_address(text):
    return bool(SOLANA_ADDRESS_RE.match(text.strip()))


def get_date_filter_params():
    since_param = request.args.get("since")
    until_param = request.args.get("until")
    return since_param, until_param


def get_held_map(hours):
    """
    Shared query for the 'held 50%+ after Nh' subquery. Not date-filtered
    by design — the outer route's date-filtered `rows` query determines
    which (wallet, mint) pairs actually get looked up.
    """
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
            SELECT q.wallet, q.token_mint,
                   CASE WHEN l.latest_mult >= q.peak_mult * 0.5 THEN TRUE ELSE FALSE END AS held
            FROM qualifying q
            JOIN latest l ON l.wallet = q.wallet AND l.token_mint = q.token_mint
            WHERE l.latest_at >= q.peak_at + (INTERVAL '1 hour' * %s)
        """, (hours,))
        held_rows = c.fetchall()
        c.close()
        return {(w, m): held for w, m, held in held_rows}
    finally:
        put_conn(conn)


def bucketize_outcome(rows, bucket_fn, bucket_order, held_map=None, wallet_mint_fn=None):
    if held_map is not None and wallet_mint_fn is None:
        raise ValueError("wallet_mint_fn is required when held_map is provided")

    buckets = {k: {"touched_total": 0, "touched_hit": 0, "held_total": 0, "held_hit": 0} for k in bucket_order}

    for row in rows:
        max_mult, hit_3x = row[-2], row[-1]
        rest = row[:-2]

        key = bucket_fn(rest)
        if key is None or key not in buckets:
            continue

        touched = bool(hit_3x) or (max_mult and max_mult >= 3)
        buckets[key]["touched_total"] += 1
        if touched:
            buckets[key]["touched_hit"] += 1

        if held_map is not None:
            wallet, mint = wallet_mint_fn(row)
            if (wallet, mint) in held_map:
                buckets[key]["held_total"] += 1
                if held_map[(wallet, mint)]:
                    buckets[key]["held_hit"] += 1

    return buckets


def format_bucket_report(title, buckets, bucket_order, footer_note, range_label="", show_held=True):
    if show_held:
        any_held_data = any(d["held_total"] > 0 for d in buckets.values())
        if not any_held_data:
            raise ValueError(
                "format_bucket_report called with show_held=True but no bucket "
                "has any held_total — did you forget to pass held_map to bucketize_outcome?"
            )

    lines = [f"<b>{title}</b>{range_label}<br>"]
    for key in bucket_order:
        d = buckets[key]
        t_total, t_hit = d["touched_total"], d["touched_hit"]
        rate = f"{t_hit/t_total*100:.1f}%" if t_total else "n/a"
        if show_held:
            h_total, h_hit = d["held_total"], d["held_hit"]
            h_rate = f"{h_hit/h_total*100:.1f}%" if h_total else "n/a"
            lines.append(
                f"<br><b>{key.upper()}</b><br>"
                f"Touched 3x+: {t_hit}/{t_total} ({rate})<br>"
                f"Held 50%+: {h_hit}/{h_total} ({h_rate})"
            )
        else:
            lines.append(f"<br>{key}: {t_hit}/{t_total} hit 3x+ ({rate})")
    lines.append(f"<br><br>{footer_note}")
    return "<br>".join(lines)


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
        c.execute("ALTER TABLE wallet_token_history ADD COLUMN IF NOT EXISTS buy_count_at_recommendation INTEGER")
        c.execute("ALTER TABLE wallet_token_history ADD COLUMN IF NOT EXISTS buy_trajectory_at_recommendation TEXT")
        c.execute("ALTER TABLE wallet_token_history ADD COLUMN IF NOT EXISTS liquidity_trend_points_at_recommendation NUMERIC")
        c.execute("ALTER TABLE wallet_token_history ADD COLUMN IF NOT EXISTS liquidity_level_points_at_recommendation NUMERIC")
        c.execute("ALTER TABLE wallet_token_history ADD COLUMN IF NOT EXISTS price_window_points_at_recommendation NUMERIC")
        c.execute("ALTER TABLE wallet_token_history ADD COLUMN IF NOT EXISTS volume_sanity_points_at_recommendation NUMERIC")
        c.execute("ALTER TABLE wallet_token_history ADD COLUMN IF NOT EXISTS clean_signal_tier_at_recommendation TEXT")
        c.execute("ALTER TABLE wallet_token_history ADD COLUMN IF NOT EXISTS conviction_tier_at_recommendation TEXT")
        c.execute("ALTER TABLE wallet_token_history ADD COLUMN IF NOT EXISTS too_perfect_penalty_applied BOOLEAN DEFAULT FALSE")
        c.execute("ALTER TABLE wallet_token_history ADD COLUMN IF NOT EXISTS sellable_check_result TEXT")
        c.execute("ALTER TABLE wallet_token_history ADD COLUMN IF NOT EXISTS historical_peak_ratio_at_recommendation NUMERIC")
        c.execute("ALTER TABLE wallet_token_history ADD COLUMN IF NOT EXISTS block_reason_at_last_attempt TEXT")
        c.execute("ALTER TABLE wallet_token_history ADD COLUMN IF NOT EXISTS decline_alert_fired BOOLEAN DEFAULT FALSE")
        c.execute("""
            CREATE TABLE IF NOT EXISTS wallet_buy_events (
                id SERIAL PRIMARY KEY,
                wallet TEXT,
                token_mint TEXT,
                buy_number INTEGER,
                price NUMERIC,
                bought_at TIMESTAMP DEFAULT NOW()
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS queen_conversations (
                id SERIAL PRIMARY KEY,
                chat_id TEXT,
                role TEXT,
                content TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_queen_conv_chat_time ON queen_conversations (chat_id, created_at)")

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
        c.execute("CREATE INDEX IF NOT EXISTS idx_scan_log_wallet_mint_time ON token_scan_log (wallet, token_mint, scanned_at)")
        c.execute("ALTER TABLE token_scan_log ADD COLUMN IF NOT EXISTS drawdown_from_first_buy NUMERIC")
        c.execute("ALTER TABLE token_scan_log ADD COLUMN IF NOT EXISTS liquidity_delta_pct NUMERIC")
        c.execute("ALTER TABLE token_scan_log ADD COLUMN IF NOT EXISTS multiplier_since_recommendation NUMERIC")
        c.execute("ALTER TABLE token_scan_log ADD COLUMN IF NOT EXISTS market_cap NUMERIC")
        c.execute("ALTER TABLE token_scan_log ADD COLUMN IF NOT EXISTS suspect_data BOOLEAN DEFAULT FALSE")
        c.execute("""
            CREATE TABLE IF NOT EXISTS paper_trades (
                id SERIAL PRIMARY KEY,
                wallet TEXT,
                token_mint TEXT,
                entry_price NUMERIC,
                entry_time TIMESTAMP DEFAULT NOW(),
                peak_price NUMERIC,
                remaining_pct NUMERIC DEFAULT 100,
                tp_3x_hit BOOLEAN DEFAULT FALSE,
                tp_10x_hit BOOLEAN DEFAULT FALSE,
                tp_15x_hit BOOLEAN DEFAULT FALSE,
                tp_30x_hit BOOLEAN DEFAULT FALSE,
                status TEXT DEFAULT 'open',
                close_reason TEXT,
                closed_at TIMESTAMP,
                realized_return_pct NUMERIC
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_paper_trades_status ON paper_trades (status)")

        conn.commit()
        c.close()
    finally:
        put_conn(conn)


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


def ask_queen(user_message, extra_context="", chat_id=None):
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

    if chat_id is not None:
        history = get_queen_history(chat_id)
        for role, content in history:
            messages.append({"role": role, "content": content})

    messages.append({"role": "user", "content": user_message})

    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": messages,
        "max_tokens": 300,
        "temperature": 0.9,
        "tools": QUEEN_TOOLS,
        "tool_choice": "auto"
    }

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=15)
        data = resp.json()
        message = data["choices"][0]["message"]

        tool_calls = message.get("tool_calls")

        if tool_calls:
            messages.append(message)

            for tool_call in tool_calls:
                func_name = tool_call["function"]["name"]
                func_args = json.loads(tool_call["function"]["arguments"])

                if func_name == "check_recommendation":
                    result = tool_check_recommendation(func_args.get("mint", ""))
                elif func_name == "check_paper_trading_summary":
                    result = tool_check_paper_trading_summary()
                elif func_name == "check_current_price":
                    result = tool_check_current_price(func_args.get("mint", ""))
                elif func_name == "check_bot_stats":
                    result = tool_check_bot_stats()
                else:
                    result = "Unknown tool."

                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call["id"],
                    "content": result
                })

            payload2 = {
                "model": "llama-3.3-70b-versatile",
                "messages": messages,
                "max_tokens": 300,
                "temperature": 0.9
            }
            resp2 = requests.post(url, headers=headers, json=payload2, timeout=15)
            data2 = resp2.json()
            reply = data2["choices"][0]["message"]["content"]
        else:
            reply = message["content"]

        if chat_id is not None:
            save_queen_message(chat_id, "user", user_message)
            save_queen_message(chat_id, "assistant", reply)

        return reply

    except Exception as e:
        print(f"Groq error: {e}")
        return "Ugh, brain fog moment — try me again in a sec."


def get_queen_history(chat_id, limit=10):
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute(
            """
            SELECT role, content FROM queen_conversations
            WHERE chat_id = %s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (str(chat_id), limit)
        )
        rows = c.fetchall()
        c.close()
        return list(reversed(rows))
    finally:
        put_conn(conn)


def save_queen_message(chat_id, role, content):
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute(
            "INSERT INTO queen_conversations (chat_id, role, content) VALUES (%s, %s, %s)",
            (str(chat_id), role, content)
        )
        conn.commit()
        c.close()
    except Exception as e:
        print(f"Error saving queen message: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        put_conn(conn)


def tool_check_recommendation(mint):
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute("""
            SELECT price_at_recommendation, recommended_at,
                   max_multiplier_since_recommendation,
                   pumped_since_recommendation_alerted
            FROM wallet_token_history
            WHERE token_mint = %s AND price_at_recommendation IS NOT NULL
            ORDER BY recommended_at DESC
            LIMIT 1
        """, (mint,))
        row = c.fetchone()
        c.close()
    finally:
        put_conn(conn)

    if not row or not row[0]:
        return f"This token was never recommended."

    price_at_rec, recommended_at, max_mult, paid_off = row
    return (
        f"Recommended at ${price_at_rec} on {recommended_at}. "
        f"All-time high multiplier since: {f'{max_mult:.2f}x' if max_mult else 'n/a'}. "
        f"3x confirmation fired: {'Yes' if paid_off else 'No'}."
    )


def tool_check_paper_trading_summary():
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute("""
            SELECT status, close_reason, realized_return_pct
            FROM paper_trades
        """)
        rows = c.fetchall()
        c.close()
    finally:
        put_conn(conn)

    if not rows:
        return "No paper trades have opened yet."

    open_count = sum(1 for r in rows if r[0] == "open")
    closed = [r for r in rows if r[0] == "closed"]
    closed_count = len(closed)

    if not closed:
        return f"{open_count} open paper position(s), no closed trades yet."

    returns = [float(r[2]) / 100 for r in closed if r[2] is not None]
    avg_return = sum(returns) / len(returns) if returns else 0
    wins = sum(1 for r in returns if r > 1.0)
    win_rate = wins / len(returns) * 100 if returns else 0

    return (
        f"{open_count} open position(s), {closed_count} closed. "
        f"Average realized return on closed trades: {avg_return:.2f}x. "
        f"Win rate: {wins}/{len(returns)} ({win_rate:.1f}%)."
    )


def tool_check_current_price(mint):
    pair = get_dexscreener_single(mint)
    if not pair:
        return "Couldn't find current price data for this token — it may not have an active trading pair yet."

    price = pair.get("priceUsd")
    market_cap = pair.get("fdv", 0) or 0
    liquidity = pair.get("liquidity", {}).get("usd", 0) or 0

    return f"Current price: ${price}. Market cap: ${market_cap:,.0f}. Liquidity: ${liquidity:,.0f}."


def tool_check_bot_stats():
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM wallet_token_history")
        total_tokens = c.fetchone()[0]

        c.execute("SELECT COUNT(*) FROM wallet_token_history WHERE momentum_alerted = TRUE")
        total_recommended = c.fetchone()[0]

        c.execute("SELECT COUNT(*) FROM wallet_token_history WHERE momentum_alerted = TRUE AND pumped_since_recommendation_alerted = TRUE")
        paid_off = c.fetchone()[0]

        c.close()
    finally:
        put_conn(conn)

    rate = f"{paid_off/total_recommended*100:.1f}%" if total_recommended else "n/a"
    return (
        f"Tracking {total_tokens} tokens total. "
        f"{total_recommended} recommended. "
        f"{paid_off} confirmed hit 3x+ ({rate} hit rate)."
    )


def get_pumpfun_data(mint):
    try:
        url = f"https://frontend-api-v3.pump.fun/coins/{mint}"
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


def get_dexscreener_batch(mints):
    result = {}
    if not mints:
        return result

    joined = ",".join(mints)
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{joined}"
        resp = requests.get(url, timeout=8)

        if resp.status_code == 429:
            print(f"⚠️ DexScreener batch 429 for {len(mints)} mints")
            return {m: None for m in mints}

        if resp.status_code != 200:
            print(f"⚠️ DexScreener batch non-200: HTTP {resp.status_code} for {len(mints)} mints")
            return {m: None for m in mints}

        if not resp.text.strip():
            print(f"⚠️ DexScreener batch empty response for {len(mints)} mints")
            return {m: None for m in mints}

        data = resp.json()
        pairs = data.get("pairs") or []

        best_pair_by_mint = {}
        for pair in pairs:
            base_mint = pair.get("baseToken", {}).get("address")
            if not base_mint:
                continue
            liq = pair.get("liquidity", {}).get("usd", 0) or 0
            if base_mint not in best_pair_by_mint or liq > (best_pair_by_mint[base_mint].get("liquidity", {}).get("usd", 0) or 0):
                best_pair_by_mint[base_mint] = pair

        for m in mints:
            result[m] = best_pair_by_mint.get(m)

        return result

    except Exception as e:
        print(f"DexScreener batch error for {len(mints)} mints: {e}")
        return {m: None for m in mints}


def get_dexscreener_batches_ratelimited(mints, batch_size=30):
    all_results = {}
    batches = [mints[i:i + batch_size] for i in range(0, len(mints), batch_size)]

    for batch in batches:
        with _dex_rate_lock:
            batch_result = get_dexscreener_batch(batch)
            all_results.update(batch_result)
            time.sleep(0.3)

    return all_results


def get_dexscreener_single(mint):
    result = get_dexscreener_batch([mint])
    return result.get(mint)


def get_dexscreener_data(mint):
    pair = get_dexscreener_single(mint)
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
    pair = get_dexscreener_single(mint)
    if pair and pair.get("priceUsd"):
        try:
            return float(pair["priceUsd"])
        except (TypeError, ValueError):
            return None
    return None


def get_token_context(mint):
    with ThreadPoolExecutor(max_workers=2) as ex:
        pf_future = ex.submit(get_pumpfun_data, mint)
        ds_future = ex.submit(get_dexscreener_data, mint)
        pf = pf_future.result()
        ds = ds_future.result()

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
        put_conn(conn)

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


def score_momentum(pair, liquidity_delta_pct=None, prior_liquidity_delta_pct=None, buy_trajectory=None):
    score = 0
    details = {}

    liquidity = pair.get("liquidity", {}).get("usd", 0) or 0
    details["liquidity"] = liquidity

    txns = pair.get("txns", {}) or {}
    m5 = txns.get("m5", {}) or {}
    details["buys_5m"] = m5.get("buys", 0) or 0
    details["sells_5m"] = m5.get("sells", 0) or 0

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
    details["liquidity_trend_points"] = trend_score
    details["liquidity_level_points"] = liquidity_score
    score += liquidity_score

    price_change = pair.get("priceChange", {}) or {}
    pc_5m = price_change.get("m5", 0) or 0
    pc_h1 = price_change.get("h1", 0) or 0
    pc_h6 = price_change.get("h6", 0) or 0
    details["pc_5m"] = pc_5m
    details["pc_h1"] = pc_h1
    details["pc_h6"] = pc_h6
    positive_windows = sum(1 for x in [pc_5m, pc_h1, pc_h6] if x and x > 0)
    price_window_points = positive_windows * (20 / 3)
    score += price_window_points
    details["price_window_points"] = price_window_points

    volume = pair.get("volume", {}) or {}
    vol_5m = volume.get("m5", 0) or 0
    vol_h1 = volume.get("h1", 0) or 0
    details["vol_5m"] = vol_5m
    details["vol_h1"] = vol_h1

    vol_to_liq_ratio = (vol_5m / liquidity) if liquidity > 0 else 0
    details["vol_to_liq_ratio"] = vol_to_liq_ratio
    volume_sanity_points = 0
    if vol_to_liq_ratio > 0.5:
        volume_sanity_points = -10
    elif 0.01 <= vol_to_liq_ratio <= 0.3:
        volume_sanity_points = 10
    score += volume_sanity_points
    details["volume_sanity_points"] = volume_sanity_points

    component_ratios = [
        trend_score / 45,
        liquidity_score / 25,
        price_window_points / 20,
        max(0, volume_sanity_points) / 10,
    ]
    near_max_count = sum(1 for r in component_ratios if r >= 0.9)
    too_perfect = near_max_count >= 3
    details["too_perfect_simultaneously"] = too_perfect

    vol_h1_to_liq_ratio = (vol_h1 / liquidity) if liquidity > 0 else 0
    details["vol_h1_to_liq_ratio"] = vol_h1_to_liq_ratio
    if vol_h1_to_liq_ratio > 5:
        score -= 20

    buys_5m_val = details["buys_5m"]
    sells_5m_val = details["sells_5m"]
    if sells_5m_val > 0:
        buy_sell_ratio = buys_5m_val / sells_5m_val
    else:
        buy_sell_ratio = float(buys_5m_val) if buys_5m_val else 0
    details["buy_sell_ratio"] = buy_sell_ratio

    details["buy_trajectory_used"] = buy_trajectory
    if buy_trajectory == "rising":
        score += 8
    elif buy_trajectory == "falling":
        score -= 12

    market_cap = pair.get("fdv", 0) or 0
    details["market_cap_used"] = market_cap
    if market_cap < 300000:
        score += 5

    return round(max(0, min(100, score))), details


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
        put_conn(conn)


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
        headers = {"X-API-KEY": RUGCHECK_API_KEY} if RUGCHECK_API_KEY else {}
        resp = requests.get(url, headers=headers, timeout=5)
        if resp.status_code != 200:
            print(f"⚠️ RugCheck non-200 for {mint}: HTTP {resp.status_code} — {resp.text[:200]}")
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


def get_wallet_cluster_count(mint):
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
        put_conn(conn)


def cluster_label(wallets, current_wallet):
    count = len(wallets)
    if count >= 3:
        return f"🔥🔥 STRONG CLUSTER: {count} tracked wallets bought this token independently!"
    elif count == 2:
        return f"🔥 Cluster detected: 2 tracked wallets bought this token independently."
    else:
        return "⚪ Single wallet signal — no other tracked wallets have bought this token yet."


def clean_signal_tier(rug_score, top1_pct, vol_h1_to_liq_ratio):
    if rug_score is None or top1_pct is None:
        return "unknown"

    rug_clean = rug_score <= 15
    holder_clean = top1_pct < 5
    vol_clean = vol_h1_to_liq_ratio is not None and vol_h1_to_liq_ratio < 3

    clean_count = sum([rug_clean, holder_clean, vol_clean])
    if clean_count == 3:
        return "strong"
    elif clean_count == 2:
        return "mostly clean"
    else:
        return "marginal"


def conviction_tier(buy_count):
    if buy_count is None:
        return "unknown"
    if buy_count >= 15:
        return "high conviction"
    elif buy_count >= 3:
        return "moderate conviction"
    else:
        return "low conviction"


def check_sellable_via_jupiter(mint, test_amount_lamports=10000000):
    start_time = time.time()
    try:
        url = "https://api.jup.ag/swap/v1/quote"
        params = {
            "inputMint": mint,
            "outputMint": WSOL_MINT,
            "amount": test_amount_lamports,
            "slippageBps": 500,
        }
        headers = {"x-api-key": JUPITER_API_KEY} if JUPITER_API_KEY else {}
        resp = requests.get(url, params=params, headers=headers, timeout=(3, 4))
        if resp.status_code != 200:
            elapsed = time.time() - start_time
            print(f"⏱️ Jupiter quote failed for {mint}: HTTP {resp.status_code} (took {elapsed:.2f}s)")
            return None

        data = resp.json()
        out_amount = data.get("outAmount")
        if out_amount is None:
            elapsed = time.time() - start_time
            print(f"⏱️ ⛔ Jupiter returned no sell route for {mint} — likely honeypot (took {elapsed:.2f}s)")
            return False

        try:
            out_val = float(out_amount)
        except (TypeError, ValueError):
            elapsed = time.time() - start_time
            print(f"⏱️ Jupiter output parse error for {mint} (took {elapsed:.2f}s)")
            return None

        if out_val <= 0:
            elapsed = time.time() - start_time
            print(f"⏱️ ⛔ Jupiter sell quote for {mint} returned zero/near-zero output — likely honeypot (took {elapsed:.2f}s)")
            return False

        elapsed = time.time() - start_time
        print(f"⏱️ ✅ Jupiter sellable check passed for {mint} (took {elapsed:.2f}s)")
        return True

    except Exception as e:
        elapsed = time.time() - start_time
        print(f"⏱️ Jupiter sellability check error for {mint}: {e} (took {elapsed:.2f}s)")
        return None


def has_token_been_recommended_before(mint):
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute(
            "SELECT 1 FROM wallet_token_history WHERE token_mint = %s AND momentum_alerted = TRUE",
            (mint,)
        )
        exists = c.fetchone() is not None
        c.close()
        return exists
    finally:
        put_conn(conn)


def get_historical_peak_ratio(wallet, mint, up_to_time=None):
    conn = get_conn()
    try:
        c = conn.cursor()
        if up_to_time:
            c.execute(
                """
                SELECT liquidity, vol_h1
                FROM token_scan_log
                WHERE wallet = %s AND token_mint = %s
                AND scanned_at <= %s
                AND liquidity IS NOT NULL AND liquidity > 0
                AND vol_h1 IS NOT NULL
                """,
                (wallet, mint, up_to_time)
            )
        else:
            c.execute(
                """
                SELECT liquidity, vol_h1
                FROM token_scan_log
                WHERE wallet = %s AND token_mint = %s
                AND liquidity IS NOT NULL AND liquidity > 0
                AND vol_h1 IS NOT NULL
                """,
                (wallet, mint)
            )
        rows = c.fetchall()
        c.close()

        if not rows:
            return None

        ratios = [float(vol_h1) / float(liq) for liq, vol_h1 in rows if liq]
        return max(ratios) if ratios else None
    finally:
        put_conn(conn)


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
    incoming_secret = request.headers.get("Authorization")
    if incoming_secret != WEBHOOK_SECRET:
        return "unauthorized", 401

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
    max_attempts = 2
    for attempt in range(max_attempts):
        conn = get_conn()
        try:
            c = conn.cursor()

            c.execute(
                "SELECT 1 FROM wallet_token_history WHERE wallet=%s AND token_mint=%s",
                (wallet, mint)
            )
            exists = c.fetchone() is not None

            price = get_current_price(mint)

            if not exists:
                try:
                    c.execute(
                        """
                        INSERT INTO wallet_token_history (wallet, token_mint, buy_count, price_at_first_buy)
                        VALUES (%s, %s, 1, %s)
                        ON CONFLICT (wallet, token_mint) DO NOTHING
                        """,
                        (wallet, mint, price)
                    )
                    conn.commit()
                    buy_number = 1
                    print(f"🟢 FIRST BUY DETECTED (recorded, no alert): wallet={wallet} token={mint} price={price}")
                except Exception as insert_err:
                    print(f"❌ FIRST BUY INSERT FAILED for wallet={wallet} token={mint}: {insert_err}")
                    conn.rollback()
                    c.close()
                    put_conn(conn)
                    return
            else:
                c.execute(
                    """
                    UPDATE wallet_token_history
                    SET buy_count = buy_count + 1
                    WHERE wallet=%s AND token_mint=%s
                    RETURNING buy_count
                    """,
                    (wallet, mint)
                )
                new_count_row = c.fetchone()
                conn.commit()
                buy_number = new_count_row[0] if new_count_row else None
                print(f"🔁 Repeat buy #{buy_number} (recorded, no alert): wallet={wallet} token={mint}")

            if price is not None:
                c.execute(
                    """
                    INSERT INTO wallet_buy_events (wallet, token_mint, buy_number, price)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (wallet, mint, buy_number, price)
                )
                conn.commit()

            c.close()
            put_conn(conn)
            return

        except psycopg2.OperationalError as db_err:
            print(f"⚠️ DB connection dropped in check_and_record_buy for wallet={wallet} token={mint}: {db_err}")
            put_conn(conn, close=True)
            if attempt < max_attempts - 1:
                print(f"🔁 Retrying check_and_record_buy for wallet={wallet} token={mint} (attempt {attempt + 2}/{max_attempts})")
                continue
            else:
                print(f"❌ check_and_record_buy gave up for wallet={wallet} token={mint} after {max_attempts} attempts")
                return

        except Exception as e:
            print(f"❌ Unexpected error in check_and_record_buy for wallet={wallet} token={mint}: {e}")
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                put_conn(conn)
            except Exception:
                pass
            return


def get_buy_trajectory(wallet, mint):
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute(
            """
            SELECT price FROM wallet_buy_events
            WHERE wallet=%s AND token_mint=%s
            ORDER BY buy_number ASC
            """,
            (wallet, mint)
        )
        prices = [float(r[0]) for r in c.fetchall() if r[0] is not None]
        c.close()

        if len(prices) < 2:
            return None

        first, last = prices[0], prices[-1]
        if last > first * 1.05:
            return "rising"
        elif last < first * 0.95:
            return "falling"
        else:
            return "flat"
    finally:
        put_conn(conn)


def _gate_check_one_token(item):
    """
    Runs the full gate-check for one qualifying token, using its own
    short-lived DB connections (separate from the main scan loop's
    connection, since this runs concurrently for multiple tokens).
    Returns a dict describing the outcome so the caller can write
    results and send alerts sequentially afterward.
    """
    wallet = item["wallet"]
    mint = item["mint"]
    pair = item["pair"]
    score = item["score"]
    details = item["details"]

    already_recommended_elsewhere = has_token_been_recommended_before(mint)

    with ThreadPoolExecutor(max_workers=3) as gate_executor:
        rug_future = gate_executor.submit(get_rugcheck_data, mint)
        holder_future = gate_executor.submit(get_top_holder_concentration, mint, pair.get("pairAddress"))
        sellable_future = gate_executor.submit(check_sellable_via_jupiter, mint)

        rug_score, rug_liq_flags = rug_future.result()
        holder_data = holder_future.result()
        sellable_result = sellable_future.result()

    top1_pct = holder_data.get("top1_pct") if holder_data else None
    vol_h1_to_liq_ratio = details.get("vol_h1_to_liq_ratio", 0)
    historical_peak_ratio = get_historical_peak_ratio(wallet, mint)

    rug_blocks = rug_score is not None and rug_score > 30
    holder_blocks = top1_pct is not None and top1_pct >= 7
    volume_blocks = vol_h1_to_liq_ratio is not None and vol_h1_to_liq_ratio > 10
    historical_volume_blocks = historical_peak_ratio is not None and historical_peak_ratio > 50

    buy_sell_ratio_at_rec = details.get("buy_sell_ratio", 0)
    buysell_blocks = buy_sell_ratio_at_rec is not None and buy_sell_ratio_at_rec >= 10

    sellable_str = (
        "sellable" if sellable_result is True
        else "not_sellable" if sellable_result is False
        else "inconclusive"
    )
    sell_blocks = sellable_result is False

    blocked = (rug_blocks or holder_blocks or volume_blocks or sell_blocks
               or already_recommended_elsewhere or historical_volume_blocks or buysell_blocks)

    result = {
        "item": item,
        "blocked": blocked,
        "rug_score": rug_score,
        "rug_liq_flags": rug_liq_flags,
        "holder_data": holder_data,
        "top1_pct": top1_pct,
        "vol_h1_to_liq_ratio": vol_h1_to_liq_ratio,
        "historical_peak_ratio": historical_peak_ratio,
        "buy_sell_ratio_at_rec": buy_sell_ratio_at_rec,
        "sellable_str": sellable_str,
        "already_recommended_elsewhere": already_recommended_elsewhere,
    }

    if blocked:
        block_reasons = []
        if rug_blocks:
            block_reasons.append(f"rugcheck({rug_score})")
        if holder_blocks:
            block_reasons.append(f"holder_pct({top1_pct:.1f})")
        if volume_blocks:
            block_reasons.append(f"vol_ratio({vol_h1_to_liq_ratio:.1f}x)")
        if historical_volume_blocks:
            block_reasons.append(f"historical_peak_ratio({historical_peak_ratio:.1f}x)")
        if buysell_blocks:
            block_reasons.append(f"buysell_ratio({buy_sell_ratio_at_rec:.1f}x)")
        if sell_blocks:
            block_reasons.append("not_sellable")
        if already_recommended_elsewhere:
            block_reasons.append("already_recommended")
        result["block_reason_str"] = ", ".join(block_reasons)

    return result


def _apply_gate_result(result):
    """
    Writes the outcome of one gate-check to the database and sends the
    Telegram alert if applicable. Uses its own short-lived connection,
    called sequentially after all concurrent gate-checks complete.
    """
    item = result["item"]
    wallet = item["wallet"]
    mint = item["mint"]
    pair = item["pair"]
    score = item["score"]
    details = item["details"]
    current_price = item["current_price"]
    current_market_cap = item["current_market_cap"]
    liquidity_delta_pct = item["liquidity_delta_pct"]
    prior_liq_delta = item["prior_liq_delta"]
    prior_pc_5m = item["prior_pc_5m"]
    current_trajectory = item["current_trajectory"]

    dexscreener_url = f"https://dexscreener.com/solana/{mint}"

    conn = get_conn()
    try:
        c = conn.cursor()

        if result["blocked"]:
            c.execute(
                "UPDATE wallet_token_history SET block_reason_at_last_attempt = %s WHERE wallet=%s AND token_mint=%s",
                (result["block_reason_str"], wallet, mint)
            )
            conn.commit()
            print(f"⛔ Recommendation blocked for {mint}: {result['block_reason_str']}")
            return

        liq_trend_note = liquidity_trend_label(liquidity_delta_pct, prior_liq_delta)
        price_trend_note = price_trend_label(details.get("pc_5m"), prior_pc_5m)
        rug_note = rugcheck_label(result["rug_score"], result["rug_liq_flags"])
        holder_note = holder_concentration_label(result["holder_data"])
        cluster_wallets = get_wallet_cluster_count(mint)
        cluster_note = cluster_label(cluster_wallets, wallet)

        c.execute(
            "SELECT buy_count FROM wallet_token_history WHERE wallet=%s AND token_mint=%s",
            (wallet, mint)
        )
        current_buy_count_row = c.fetchone()
        current_buy_count = current_buy_count_row[0] if current_buy_count_row else None
        liquidity_trend_pts = details.get("liquidity_trend_points")
        liquidity_level_pts = details.get("liquidity_level_points")
        price_window_pts = details.get("price_window_points")
        volume_sanity_pts = details.get("volume_sanity_points")
        too_perfect_flag = details.get("too_perfect_simultaneously", False)
        buy_trajectory = current_trajectory
        clean_tier = clean_signal_tier(result["rug_score"], result["top1_pct"], result["vol_h1_to_liq_ratio"])
        conv_tier = conviction_tier(current_buy_count)

        clean_tier_note = "🌟 STRONG SETUP (cleared all gates comfortably)\n" if clean_tier == "strong" else ""
        conv_tier_note = "💪 HIGH CONVICTION (wallet bought 15+ times)\n" if conv_tier == "high conviction" else ""

        c.execute(
            "UPDATE token_scan_log SET momentum_alert_fired = TRUE WHERE id = %s",
            (item["scan_log_id"],)
        )

        c.execute(
            """
            UPDATE wallet_token_history
            SET momentum_alerted = TRUE,
                price_at_recommendation = %s,
                recommended_at = NOW(),
                market_cap_at_recommendation = %s,
                rugcheck_score_at_recommendation = %s,
                top1_holder_pct_at_recommendation = %s,
                cluster_count_at_recommendation = %s,
                buy_count_at_recommendation = %s,
                liquidity_trend_points_at_recommendation = %s,
                liquidity_level_points_at_recommendation = %s,
                price_window_points_at_recommendation = %s,
                volume_sanity_points_at_recommendation = %s,
                buy_trajectory_at_recommendation = %s,
                clean_signal_tier_at_recommendation = %s,
                conviction_tier_at_recommendation = %s,
                too_perfect_penalty_applied = %s,
                sellable_check_result = %s,
                historical_peak_ratio_at_recommendation = %s
            WHERE wallet=%s AND token_mint=%s
            """,
            (current_price, current_market_cap, result["rug_score"],
             result["top1_pct"], len(cluster_wallets), current_buy_count,
             liquidity_trend_pts, liquidity_level_pts, price_window_pts, volume_sanity_pts,
             buy_trajectory, clean_tier, conv_tier, too_perfect_flag, result["sellable_str"],
             result["historical_peak_ratio"], wallet, mint)
        )
        conn.commit()

        send_telegram_alert(
            f"🚀 Heating up (score {score}/100)\n"
            f"{clean_tier_note}"
            f"{conv_tier_note}"
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
        _open_paper_trade(wallet, mint, current_price, current_market_cap, result["rug_score"], score)

    except Exception as e:
        print(f"Error applying gate result for {mint}: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        put_conn(conn)


def _open_paper_trade(wallet, mint, current_price, current_market_cap, rug_score, score):
    if score < 85:
        return
    if current_market_cap is None or current_market_cap > 100000:
        return
    if rug_score is None:
        return

    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute(
            "SELECT 1 FROM paper_trades WHERE wallet=%s AND token_mint=%s",
            (wallet, mint)
        )
        if c.fetchone() is not None:
            c.close()
            return

        c.execute(
            """
            INSERT INTO paper_trades
                (wallet, token_mint, entry_price, peak_price, remaining_pct, status)
            VALUES (%s, %s, %s, %s, 100, 'open')
            """,
            (wallet, mint, current_price, current_price)
        )
        conn.commit()
        c.close()
        print(f"📝 PAPER TRADE OPENED: wallet={wallet} token={mint} entry=${current_price} mc=${current_market_cap:,.0f}")

    except Exception as e:
        print(f"Error opening paper trade for {mint}: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        put_conn(conn)


def _check_paper_trades():
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute("""
            SELECT id, wallet, token_mint, entry_price, peak_price, remaining_pct,
                   tp_3x_hit, tp_10x_hit, tp_15x_hit, tp_30x_hit
            FROM paper_trades
            WHERE status = 'open'
        """)
        open_trades = c.fetchall()
        c.close()
    finally:
        put_conn(conn)

    if not open_trades:
        return

    mints = [row[2] for row in open_trades]
    pairs_by_mint = get_dexscreener_batches_ratelimited(mints, batch_size=30)

    for (trade_id, wallet, mint, entry_price, peak_price,
         remaining_pct, tp_3x, tp_10x, tp_15x, tp_30x) in open_trades:

        pair = pairs_by_mint.get(mint)
        if not pair:
            continue

        current_price = None
        try:
            current_price = float(pair.get("priceUsd"))
        except (TypeError, ValueError):
            continue

        if not current_price or not entry_price:
            continue

        entry_price = float(entry_price)
        peak_price = float(peak_price) if peak_price else entry_price
        remaining_pct = float(remaining_pct)
        multiplier = current_price / entry_price

        new_peak = max(peak_price, current_price)

        conn2 = get_conn()
        try:
            c2 = conn2.cursor()

            realized_this_cycle = 0.0
            new_tp_3x, new_tp_10x, new_tp_15x, new_tp_30x = tp_3x, tp_10x, tp_15x, tp_30x
            new_remaining = remaining_pct
            closed = False
            close_reason = None

            if multiplier >= 30 and not tp_30x:
                new_remaining -= 25
                new_tp_30x = True
                realized_this_cycle += 25 * 30
                print(f"📈 PAPER TP 30x hit for {mint}: sold 25% at {multiplier:.1f}x")
            if multiplier >= 15 and not tp_15x:
                new_remaining -= 25
                new_tp_15x = True
                realized_this_cycle += 25 * 15
                print(f"📈 PAPER TP 15x hit for {mint}: sold 25% at {multiplier:.1f}x")
            if multiplier >= 10 and not tp_10x:
                new_remaining -= 25
                new_tp_10x = True
                realized_this_cycle += 25 * 10
                print(f"📈 PAPER TP 10x hit for {mint}: sold 25% at {multiplier:.1f}x")
            if multiplier >= 3 and not tp_3x:
                new_remaining -= 25
                new_tp_3x = True
                realized_this_cycle += 25 * 3
                print(f"📈 PAPER TP 3x hit for {mint}: sold 25% at {multiplier:.1f}x")

            any_profit_taken = new_tp_3x or new_tp_10x or new_tp_15x or new_tp_30x

            stop_loss_hit = (not any_profit_taken) and current_price <= entry_price * 0.7
            trailing_stop_hit = (
                new_peak > 0
                and current_price <= new_peak * 0.7
                and (any_profit_taken or current_price > entry_price * 0.7)
            )

            if stop_loss_hit and new_remaining > 0:
                realized_this_cycle += new_remaining * multiplier
                print(f"🛑 PAPER STOP LOSS for {mint}: sold remaining {new_remaining}% at {multiplier:.1f}x")
                new_remaining = 0
                closed = True
                close_reason = "stop_loss"
            elif trailing_stop_hit and new_remaining > 0:
                realized_this_cycle += new_remaining * multiplier
                print(f"🔻 PAPER TRAILING STOP for {mint}: sold remaining {new_remaining}% at {multiplier:.1f}x (peak was {new_peak/entry_price:.1f}x)")
                new_remaining = 0
                closed = True
                close_reason = "trailing_stop"
            elif new_remaining <= 0:
                closed = True
                close_reason = "full_take_profit"

            if closed:
                c2.execute(
                    """
                    UPDATE paper_trades
                    SET peak_price = %s, remaining_pct = 0,
                        tp_3x_hit = %s, tp_10x_hit = %s, tp_15x_hit = %s, tp_30x_hit = %s,
                        status = 'closed', close_reason = %s, closed_at = NOW(),
                        realized_return_pct = COALESCE(realized_return_pct, 0) + %s
                    WHERE id = %s
                    """,
                    (new_peak, new_tp_3x, new_tp_10x, new_tp_15x, new_tp_30x,
                     close_reason, realized_this_cycle, trade_id)
                )
            else:
                c2.execute(
                    """
                    UPDATE paper_trades
                    SET peak_price = %s, remaining_pct = %s,
                        tp_3x_hit = %s, tp_10x_hit = %s, tp_15x_hit = %s, tp_30x_hit = %s,
                        realized_return_pct = COALESCE(realized_return_pct, 0) + %s
                    WHERE id = %s
                    """,
                    (new_peak, new_remaining, new_tp_3x, new_tp_10x, new_tp_15x, new_tp_30x,
                     realized_this_cycle, trade_id)
                )
            conn2.commit()
            c2.close()

        except Exception as e:
            print(f"Error checking paper trade {trade_id} for {mint}: {e}")
            try:
                conn2.rollback()
            except Exception:
                pass
        finally:
            put_conn(conn2)


def run_pump_check(run_id):
    conn = None
    c = None
    checked = 0
    qualifying_tokens = []

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
        """, (SCAN_WINDOW_HOURS,))
        rows = c.fetchall()
        print(f"Checking {len(rows)} tokens for pumps/momentum...")

        mints = [row[1] for row in rows]
        pairs_by_mint = get_dexscreener_batches_ratelimited(mints, batch_size=30)

        for i, (wallet, mint, price_at_first_buy, pumped_alerted, momentum_alerted,
                prev_liquidity, price_at_recommendation, pumped_since_rec_alerted,
                recommended_at, market_cap_at_recommendation) in enumerate(rows):

            if i > 0 and i % DB_CONN_REFRESH_EVERY == 0:
                try:
                    put_conn(conn)
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
                current_trajectory = get_buy_trajectory(wallet, mint)

                score, details = score_momentum(pair, liquidity_delta_pct, prior_liq_delta, current_trajectory)
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

                decline_alert_needed = (
                    not suspect
                    and price_at_recommendation
                    and multiplier_since_recommendation is not None
                    and multiplier_since_recommendation < 0.7
                    and momentum_alerted
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

                pump_alert_fired = False

                if not suspect and not pumped_since_rec_alerted and price_at_recommendation \
                   and multiplier_since_recommendation and multiplier_since_recommendation >= 3 \
                   and not (not momentum_alerted and score >= 70):
                    pump_alert_fired = True
                    mc_line = ""
                    if market_cap_at_recommendation:
                        mc_line = (
                            f"Market cap then: ${float(market_cap_at_recommendation):,.0f} → "
                            f"now: ${current_market_cap:,.0f}\n"
                        )
                    dexscreener_url = f"https://dexscreener.com/solana/{mint}"
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

                if not suspect and not momentum_alerted and score >= 70:
                    c.execute(
                        """
                        INSERT INTO token_scan_log
                            (wallet, token_mint, price, liquidity, vol_5m, vol_h1,
                             pc_5m, pc_h1, pc_h6, buys_5m, sells_5m, momentum_score,
                             multiplier_from_first_buy, drawdown_from_first_buy,
                             liquidity_delta_pct, momentum_alert_fired, pump_alert_fired,
                             multiplier_since_recommendation, market_cap, suspect_data)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                        """,
                        (
                            wallet, mint, current_price, details.get("liquidity"),
                            details.get("vol_5m"), details.get("vol_h1"),
                            details.get("pc_5m"), details.get("pc_h1"), details.get("pc_h6"),
                            details.get("buys_5m"), details.get("sells_5m"), score,
                            multiplier_from_first_buy, drawdown, liquidity_delta_pct,
                            False, pump_alert_fired, multiplier_since_recommendation,
                            current_market_cap, suspect
                        )
                    )
                    inserted_scan_id = c.fetchone()[0]
                    conn.commit()

                    qualifying_tokens.append({
                        "wallet": wallet, "mint": mint, "pair": pair,
                        "score": score, "details": details,
                        "current_price": current_price, "current_market_cap": current_market_cap,
                        "liquidity_delta_pct": liquidity_delta_pct,
                        "prior_liq_delta": prior_liq_delta, "prior_pc_5m": prior_pc_5m,
                        "current_trajectory": current_trajectory,
                        "scan_log_id": inserted_scan_id,
                    })
                else:
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
                            False, pump_alert_fired, multiplier_since_recommendation,
                            current_market_cap, suspect
                        )
                    )
                    conn.commit()

                if decline_alert_needed:
                    c.execute(
                        "SELECT decline_alert_fired FROM wallet_token_history WHERE wallet=%s AND token_mint=%s",
                        (wallet, mint)
                    )
                    already_alerted_row = c.fetchone()
                    already_declined_alerted = already_alerted_row[0] if already_alerted_row else False

                    if not already_declined_alerted:
                        decline_pct = (1 - multiplier_since_recommendation) * 100
                        send_telegram_alert(
                            f"⚠️ DECLINING — down {decline_pct:.0f}% since recommendation\n"
                            f"Wallet: <code>{wallet}</code>\n"
                            f"Token: <code>{mint}</code>\n"
                            f"Price at recommendation: ${price_at_recommendation} → now: ${current_price}\n\n"
                            f"Validated: tokens down 30%+ at 1h only hit 3x+ afterward 4.5% of the time "
                            f"(vs 16.0% baseline) — this one is unlikely to recover. DYOR."
                        )
                        c.execute(
                            "UPDATE wallet_token_history SET decline_alert_fired = TRUE WHERE wallet=%s AND token_mint=%s",
                            (wallet, mint)
                        )
                        conn.commit()

            except psycopg2.OperationalError as db_err:
                print(f"DB connection dropped mid-scan on {mint}: {db_err} — reconnecting")
                put_conn(conn, close=True)
                try:
                    conn = get_conn()
                    c = conn.cursor()
                except Exception as reconnect_err:
                    print(f"Reconnect failed: {reconnect_err}")
                    break
                continue

            except Exception as e:
                print(f"Error processing {mint}: {e}")
                try:
                    conn.rollback()
                except Exception:
                    pass
                continue

        if qualifying_tokens:
            seen_mints = set()
            deduped = []
            for item in qualifying_tokens:
                if item["mint"] in seen_mints:
                    print(f"⚠️ Skipping duplicate qualifying token {item['mint']} (wallet {item['wallet']}) — already queued from another wallet this cycle")
                    continue
                seen_mints.add(item["mint"])
                deduped.append(item)
            qualifying_tokens = deduped

            print(f"Gate-checking {len(qualifying_tokens)} qualifying tokens concurrently...")
            with ThreadPoolExecutor(max_workers=min(len(qualifying_tokens), 3)) as outer_executor:
                gate_results = list(outer_executor.map(_gate_check_one_token, qualifying_tokens))

            for result in gate_results:
                _apply_gate_result(result)

        _check_paper_trades()
        
        print(f"check_pumps finished — checked {checked} tokens, {len(qualifying_tokens)} qualified for gate-check")

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
                conn.rollback()
            except Exception:
                pass
            try:
                put_conn(conn)
            except Exception:
                pass

        global _check_pumps_run_id
        if _check_pumps_run_id == run_id:
            _check_pumps_run_id = None
            try:
                _check_pumps_lock.release()
            except RuntimeError:
                pass
        else:
            print(f"⚠️ run_pump_check ({run_id}) finished after being force-released — skipping release to avoid stealing the new owner's lock")


@app.route("/check-pumps", methods=["GET", "POST"])
def check_pumps():
    global _check_pumps_lock_time, _check_pumps_run_id

    acquired = _check_pumps_lock.acquire(blocking=False)
    if not acquired:
        if _check_pumps_lock_time and (time.time() - _check_pumps_lock_time) > 1800:
            print("⚠️ check_pumps lock held for 30+ minutes — force releasing (likely stuck)")
            try:
                _check_pumps_lock.release()
            except RuntimeError:
                pass
            acquired = _check_pumps_lock.acquire(blocking=False)
        if not acquired:
            print("check-pumps already running, skipping this trigger")
            return "already running", 200

    run_id = uuid.uuid4()
    _check_pumps_lock_time = time.time()
    _check_pumps_run_id = run_id

    threading.Thread(target=run_pump_check, args=(run_id,), daemon=True).start()
    return "started", 200


@app.route("/telegram", methods=["POST"])
def telegram_webhook():
    incoming_secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
    if incoming_secret != TELEGRAM_WEBHOOK_SECRET:
        return "unauthorized", 401

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
                put_conn(conn)

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
                put_conn(conn)

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
        reply = ask_queen(text, chat_id=chat_id)
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
        put_conn(conn)


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
        put_conn(conn)


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
        put_conn(conn)


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
        put_conn(conn)


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
        put_conn(conn)


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
        put_conn(conn)


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
        put_conn(conn)


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
        put_conn(conn)


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
        put_conn(conn)


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
        put_conn(conn)


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
        put_conn(conn)


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
        put_conn(conn)


@app.route("/cleanup-old-scans", methods=["GET", "POST"])
def cleanup_old_scans():
    days = request.args.get("days", "5")
    try:
        days = float(days)
    except (TypeError, ValueError):
        days = 5.0

    conn = get_conn()
    try:
        c = conn.cursor()

        c.execute("DELETE FROM token_scan_log WHERE scanned_at < NOW() - (INTERVAL '1 day' * %s)", (days,))
        deleted = c.rowcount
        conn.commit()
        c.close()

        return (
            f"Cleanup complete — deleted {deleted} scan log rows older than "
            f"{days} days. Note: run VACUUM FULL token_scan_log manually in "
            f"Neon's SQL editor periodically to actually reclaim disk space "
            f"(DELETE alone frees rows for reuse but doesn't shrink the file).",
            200
        )

    except Exception as e:
        return f"cleanup_old_scans error: {e}", 500

    finally:
        put_conn(conn)


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
        put_conn(conn)


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
        put_conn(conn)


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
        put_conn(conn)


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
        put_conn(conn)


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
        put_conn(conn)


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
        put_conn(conn)


@app.route("/check-volume-ratio-vs-outcome")
def check_volume_ratio_vs_outcome():
    since_param, until_param = get_date_filter_params()
    conn = get_conn()
    try:
        c = conn.cursor()

        query = """
            SELECT s.liquidity, s.vol_h1,
                   h.max_multiplier_since_recommendation,
                   h.pumped_since_recommendation_alerted
            FROM token_scan_log s
            JOIN wallet_token_history h
                ON h.wallet = s.wallet AND h.token_mint = s.token_mint
            WHERE s.momentum_alert_fired = TRUE
            AND s.suspect_data IS NOT TRUE
            AND s.liquidity IS NOT NULL AND s.liquidity > 0
            AND s.vol_h1 IS NOT NULL
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
            return "No recommendation data with liquidity/volume yet.", 200

        buckets = {
            "under 3x": {"total": 0, "hit_3x": 0},
            "3-5x": {"total": 0, "hit_3x": 0},
            "5-10x": {"total": 0, "hit_3x": 0},
            "over 10x": {"total": 0, "hit_3x": 0},
        }

        for liquidity, vol_h1, max_mult, hit_3x in rows:
            ratio = float(vol_h1) / float(liquidity) if liquidity else 0
            if ratio < 3:
                key = "under 3x"
            elif ratio < 5:
                key = "3-5x"
            elif ratio < 10:
                key = "5-10x"
            else:
                key = "over 10x"

            buckets[key]["total"] += 1
            hit = bool(hit_3x) or (max_mult and max_mult >= 3)
            if hit:
                buckets[key]["hit_3x"] += 1

        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"

        lines = [f"<b>Recommendation hit-rate by 1h-volume/liquidity ratio AT RECOMMENDATION:</b>{range_label}<br>"]
        for bucket, d in buckets.items():
            total = d["total"]
            hits = d["hit_3x"]
            rate = f"{hits/total*100:.1f}%" if total else "n/a"
            lines.append(f"<br>Ratio {bucket}: {hits}/{total} hit 3x+ ({rate})")

        lines.append(
            "<br><br>If ratio buckets above 5x show meaningfully LOWER hit "
            "rates than under-3x, that validates strengthening the current "
            "penalty into a hard gate. If rates are similar across buckets, "
            "the soft penalty is already doing enough and a hard gate isn't "
            "justified."
        )
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_volume_ratio_vs_outcome error: {e}", 500

    finally:
        put_conn(conn)


@app.route("/check-volume-ratio-vs-outcome-sustained")
def check_volume_ratio_vs_outcome_sustained():
    since_param, until_param = get_date_filter_params()
    hours = request.args.get("hours", "1")
    try:
        hours = float(hours)
    except (TypeError, ValueError):
        hours = 1.0

    conn = get_conn()
    try:
        c = conn.cursor()

        query = """
            WITH scans AS (
                SELECT wallet, token_mint, scanned_at, multiplier_from_first_buy,
                       liquidity, vol_h1
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
                            THEN TRUE ELSE FALSE END AS held_sustained
                FROM qualifying q
                JOIN latest l ON l.wallet = q.wallet AND l.token_mint = q.token_mint
                WHERE l.latest_at >= q.peak_at + (INTERVAL '1 hour' * %s)
            ),
            recommendation_scan AS (
                SELECT s.wallet, s.token_mint, s.liquidity, s.vol_h1
                FROM token_scan_log s
                WHERE s.momentum_alert_fired = TRUE
                AND s.suspect_data IS NOT TRUE
            )
            SELECT r.liquidity, r.vol_h1, o.held_sustained
            FROM recommendation_scan r
            JOIN outcomes o
                ON o.wallet = r.wallet AND o.token_mint = r.token_mint
        """
        params = [hours]
        if since_param:
            query += " WHERE o.peak_at >= %s"
            params.append(since_param)
        if until_param:
            query += (" AND" if since_param else " WHERE") + " o.peak_at < %s"
            params.append(until_param)

        c.execute(query, params)
        rows = c.fetchall()
        c.close()

        if not rows:
            return f"No tokens peaked at 3x+ AND had {hours}+ hours pass since, with recommendation liquidity/volume data.", 200

        buckets = {
            "under 3x": {"total": 0, "held": 0},
            "3-5x": {"total": 0, "held": 0},
            "5-10x": {"total": 0, "held": 0},
            "over 10x": {"total": 0, "held": 0},
        }

        for liquidity, vol_h1, held in rows:
            if not liquidity or liquidity == 0:
                continue
            ratio = float(vol_h1) / float(liquidity) if vol_h1 else 0
            if ratio < 3:
                key = "under 3x"
            elif ratio < 5:
                key = "3-5x"
            elif ratio < 10:
                key = "5-10x"
            else:
                key = "over 10x"

            buckets[key]["total"] += 1
            if held:
                buckets[key]["held"] += 1

        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"

        lines = [
            f"<b>SUSTAINED hit-rate (peaked 3x+ AND held 50%+ after {hours}h) "
            f"by 1h-volume/liquidity ratio AT RECOMMENDATION:</b>{range_label}<br>"
        ]
        for bucket, d in buckets.items():
            total = d["total"]
            held = d["held"]
            rate = f"{held/total*100:.1f}%" if total else "n/a"
            lines.append(f"<br>Ratio {bucket}: {held}/{total} held 50%+ ({rate})")

        lines.append(
            "<br><br>Compare this against /check-volume-ratio-vs-outcome "
            "(which only checks if 3x was ever touched). If high-ratio "
            "buckets show a much bigger drop here than in the touch-only "
            "version, that confirms high volume ratio predicts spike-and-dump "
            "behavior specifically, not failure to move at all."
        )
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_volume_ratio_vs_outcome_sustained error: {e}", 500

    finally:
        put_conn(conn)


@app.route("/check-combined-signal-vs-outcome")
def check_combined_signal_vs_outcome():
    since_param, until_param = get_date_filter_params()
    hours = request.args.get("hours", "1")
    try:
        hours = float(hours)
    except (TypeError, ValueError):
        hours = 1.0

    conn = get_conn()
    try:
        c = conn.cursor()

        query = """
            SELECT
                h.wallet, h.token_mint,
                h.rugcheck_score_at_recommendation,
                h.top1_holder_pct_at_recommendation,
                s.liquidity, s.vol_h1,
                h.max_multiplier_since_recommendation,
                h.pumped_since_recommendation_alerted
            FROM wallet_token_history h
            JOIN token_scan_log s
                ON s.wallet = h.wallet AND s.token_mint = h.token_mint
                AND s.momentum_alert_fired = TRUE
            WHERE h.momentum_alerted = TRUE
            AND h.rugcheck_score_at_recommendation IS NOT NULL
            AND h.top1_holder_pct_at_recommendation IS NOT NULL
            AND s.liquidity IS NOT NULL AND s.liquidity > 0
            AND s.vol_h1 IS NOT NULL
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
        put_conn(conn)
        conn = None

        if not rows:
            return "No recommendations with all three signal values yet.", 200

        conn2 = get_conn()
        c2 = conn2.cursor()
        c2.execute("""
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
            SELECT q.wallet, q.token_mint,
                   CASE WHEN l.latest_mult >= q.peak_mult * 0.5 THEN TRUE ELSE FALSE END AS held
            FROM qualifying q
            JOIN latest l ON l.wallet = q.wallet AND l.token_mint = q.token_mint
            WHERE l.latest_at >= q.peak_at + (INTERVAL '1 hour' * %s)
        """, (hours,))
        held_rows = c2.fetchall()
        c2.close()
        put_conn(conn2)

        held_map = {(w, m): held for w, m, held in held_rows}

        buckets = {
            "all clean": {"touched_total": 0, "touched_hit": 0, "held_total": 0, "held_hit": 0},
            "mostly clean": {"touched_total": 0, "touched_hit": 0, "held_total": 0, "held_hit": 0},
            "marginal": {"touched_total": 0, "touched_hit": 0, "held_total": 0, "held_hit": 0},
        }

        for wallet, mint, rug_score, top1_pct, liquidity, vol_h1, max_mult, hit_3x in rows:
            rug_score = float(rug_score)
            top1_pct = float(top1_pct)
            ratio = float(vol_h1) / float(liquidity) if liquidity else 0

            rug_clean = rug_score <= 15
            holder_clean = top1_pct < 5
            vol_clean = ratio < 3

            clean_count = sum([rug_clean, holder_clean, vol_clean])
            if clean_count == 3:
                key = "all clean"
            elif clean_count == 2:
                key = "mostly clean"
            else:
                key = "marginal"

            touched = bool(hit_3x) or (max_mult and max_mult >= 3)
            buckets[key]["touched_total"] += 1
            if touched:
                buckets[key]["touched_hit"] += 1

            if (wallet, mint) in held_map:
                buckets[key]["held_total"] += 1
                if held_map[(wallet, mint)]:
                    buckets[key]["held_hit"] += 1

        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"

        lines = [f"<b>Combined signal (RugCheck ≤15, holder &lt;5%, vol ratio &lt;3x) vs outcome:</b>{range_label}<br>"]
        for bucket, d in buckets.items():
            t_total, t_hit = d["touched_total"], d["touched_hit"]
            h_total, h_hit = d["held_total"], d["held_hit"]
            t_rate = f"{t_hit/t_total*100:.1f}%" if t_total else "n/a"
            h_rate = f"{h_hit/h_total*100:.1f}%" if h_total else "n/a"
            lines.append(
                f"<br><b>{bucket.upper()}</b><br>"
                f"Touched 3x+: {t_hit}/{t_total} ({t_rate})<br>"
                f"Held 50%+ after {hours}h: {h_hit}/{h_total} ({h_rate})"
            )

        lines.append(
            "<br><br>If 'all clean' shows meaningfully higher rates (especially "
            "the HELD metric) than 'marginal', that validates rewarding "
            "tokens that clear all three gates comfortably, not just barely. "
            "If rates are similar across buckets, the signals are largely "
            "redundant and stacking them adds little."
        )
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_combined_signal_vs_outcome error: {e}", 500

    finally:
        if conn:
            put_conn(conn)


@app.route("/check-conviction-vs-outcome")
def check_conviction_vs_outcome():
    since_param, until_param = get_date_filter_params()
    hours = request.args.get("hours", "1")
    try:
        hours = float(hours)
    except (TypeError, ValueError):
        hours = 1.0

    conn = get_conn()
    try:
        c = conn.cursor()

        query = """
            SELECT h.wallet, h.token_mint, h.buy_count_at_recommendation,
                   h.max_multiplier_since_recommendation,
                   h.pumped_since_recommendation_alerted
            FROM wallet_token_history h
            WHERE h.momentum_alerted = TRUE
            AND h.buy_count_at_recommendation IS NOT NULL
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
        put_conn(conn)
        conn = None

        if not rows:
            return "No recommendation data with buy_count_at_recommendation yet.", 200

        conn2 = get_conn()
        c2 = conn2.cursor()
        c2.execute("""
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
            SELECT q.wallet, q.token_mint,
                   CASE WHEN l.latest_mult >= q.peak_mult * 0.5 THEN TRUE ELSE FALSE END AS held
            FROM qualifying q
            JOIN latest l ON l.wallet = q.wallet AND l.token_mint = q.token_mint
            WHERE l.latest_at >= q.peak_at + (INTERVAL '1 hour' * %s)
        """, (hours,))
        held_rows = c2.fetchall()
        c2.close()
        put_conn(conn2)

        held_map = {(w, m): held for w, m, held in held_rows}

        buckets = {
            "1 buy": {"touched_total": 0, "touched_hit": 0, "held_total": 0, "held_hit": 0},
            "2 buys": {"touched_total": 0, "touched_hit": 0, "held_total": 0, "held_hit": 0},
            "3+ buys": {"touched_total": 0, "touched_hit": 0, "held_total": 0, "held_hit": 0},
        }

        for wallet, mint, buy_count, max_mult, hit_3x in rows:
            if buy_count <= 1:
                key = "1 buy"
            elif buy_count == 2:
                key = "2 buys"
            else:
                key = "3+ buys"

            touched = bool(hit_3x) or (max_mult and max_mult >= 3)
            buckets[key]["touched_total"] += 1
            if touched:
                buckets[key]["touched_hit"] += 1

            if (wallet, mint) in held_map:
                buckets[key]["held_total"] += 1
                if held_map[(wallet, mint)]:
                    buckets[key]["held_hit"] += 1

        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"

        lines = [f"<b>Conviction check (buy_count snapshotted AT recommendation time):</b>{range_label}<br>"]
        for bucket, d in buckets.items():
            t_total, t_hit = d["touched_total"], d["touched_hit"]
            h_total, h_hit = d["held_total"], d["held_hit"]
            t_rate = f"{t_hit/t_total*100:.1f}%" if t_total else "n/a"
            h_rate = f"{h_hit/h_total*100:.1f}%" if h_total else "n/a"
            lines.append(
                f"<br><b>{bucket.upper()}</b><br>"
                f"Touched 3x+: {t_hit}/{t_total} ({t_rate})<br>"
                f"Held 50%+ after {hours}h: {h_hit}/{h_total} ({h_rate})"
            )

        lines.append(
            "<br><br>✅ Using buy_count snapshotted AT recommendation time — "
            "accurate reflection of conviction at the moment the alert fired."
        )
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_conviction_vs_outcome error: {e}", 500

    finally:
        if conn:
            put_conn(conn)


@app.route("/check-conviction-tier-vs-outcome")
def check_conviction_tier_vs_outcome():
    since_param, until_param = get_date_filter_params()
    conn = get_conn()
    try:
        c = conn.cursor()

        query = """
            SELECT conviction_tier_at_recommendation,
                   max_multiplier_since_recommendation,
                   pumped_since_recommendation_alerted
            FROM wallet_token_history
            WHERE momentum_alerted = TRUE
            AND conviction_tier_at_recommendation IS NOT NULL
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
            return "No recommendation data with conviction tier yet.", 200

        buckets = {
            "low conviction": {"total": 0, "hit_3x": 0},
            "moderate conviction": {"total": 0, "hit_3x": 0},
            "high conviction": {"total": 0, "hit_3x": 0},
        }

        for tier, max_mult, hit_3x in rows:
            if tier not in buckets:
                continue
            buckets[tier]["total"] += 1
            hit = bool(hit_3x) or (max_mult and max_mult >= 3)
            if hit:
                buckets[tier]["hit_3x"] += 1

        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"

        lines = [f"<b>Recommendation hit-rate by conviction tier:</b>{range_label}<br>"]
        for bucket, d in buckets.items():
            total = d["total"]
            hits = d["hit_3x"]
            rate = f"{hits/total*100:.1f}%" if total else "n/a"
            lines.append(f"<br>{bucket.upper()}: {hits}/{total} hit 3x+ ({rate})")

        lines.append(
            "<br><br>Confirms whether the buy-count tier boundaries (3+ / 15+) "
            "are well-calibrated, using the same touched-3x metric as other checks."
        )
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_conviction_tier_vs_outcome error: {e}", 500

    finally:
        put_conn(conn)


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
        put_conn(conn)


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
        put_conn(conn)


@app.route("/check-marketcap-growth-vs-score")
def check_marketcap_growth_vs_score():
    since_param, until_param = get_date_filter_params()
    conn = get_conn()
    try:
        c = conn.cursor()

        query = """
            SELECT s.momentum_score, h.market_cap_at_recommendation,
                   fs.market_cap AS market_cap_at_first_buy
            FROM token_scan_log s
            JOIN wallet_token_history h
                ON h.wallet = s.wallet AND h.token_mint = s.token_mint
            JOIN LATERAL (
                SELECT market_cap
                FROM token_scan_log fs2
                WHERE fs2.wallet = h.wallet AND fs2.token_mint = h.token_mint
                ORDER BY fs2.scanned_at ASC
                LIMIT 1
            ) fs ON TRUE
            WHERE s.momentum_alert_fired = TRUE
            AND s.suspect_data IS NOT TRUE
            AND h.market_cap_at_recommendation IS NOT NULL
            AND fs.market_cap IS NOT NULL AND fs.market_cap > 0
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
            return "No recommendation data with market cap history yet.", 200

        buckets = {
            "70-79": [],
            "80-89": [],
            "90-100": [],
        }

        for score, mc_at_rec, mc_at_first_buy in rows:
            if score is None:
                continue
            growth_mult = float(mc_at_rec) / float(mc_at_first_buy) if mc_at_first_buy else None
            if growth_mult is None:
                continue

            if 70 <= score < 80:
                key = "70-79"
            elif 80 <= score < 90:
                key = "80-89"
            elif score >= 90:
                key = "90-100"
            else:
                continue

            buckets[key].append((float(mc_at_first_buy), float(mc_at_rec), growth_mult))

        def median(vals):
            s = sorted(vals)
            n = len(s)
            if n == 0:
                return None
            mid = n // 2
            return (s[mid - 1] + s[mid]) / 2 if n % 2 == 0 else s[mid]

        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"

        lines = [f"<b>Market cap growth (first buy → recommendation) by score bucket:</b>{range_label}<br>"]
        for bucket, vals in buckets.items():
            if not vals:
                lines.append(f"<br>{bucket}: no data")
                continue
            first_buys = [v[0] for v in vals]
            recs = [v[1] for v in vals]
            growths = [v[2] for v in vals]
            lines.append(
                f"<br><b>Score {bucket}</b> (n={len(vals)})<br>"
                f"Market cap at first buy — median: ${median(first_buys):,.0f}<br>"
                f"Market cap at recommendation — median: ${median(recs):,.0f}<br>"
                f"Growth multiplier — median: {median(growths):.2f}x"
            )

        lines.append(
            "<br><br>If 90-100 shows a much higher market-cap-at-recommendation "
            "and growth multiplier than 70-79, that confirms high scores are "
            "systematically firing after market cap has already ballooned — "
            "validating a market cap cap/gate for high scores specifically."
        )
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_marketcap_growth_vs_score error: {e}", 500

    finally:
        put_conn(conn)


@app.route("/check-marketcap-vs-outcome-detailed")
def check_marketcap_vs_outcome_detailed():
    since_param, until_param = get_date_filter_params()
    hours = request.args.get("hours", "1")
    try:
        hours = float(hours)
    except (TypeError, ValueError):
        hours = 1.0

    conn = get_conn()
    try:
        c = conn.cursor()

        query = """
            SELECT h.wallet, h.token_mint, h.market_cap_at_recommendation,
                   h.max_multiplier_since_recommendation,
                   h.pumped_since_recommendation_alerted
            FROM wallet_token_history h
            WHERE h.momentum_alerted = TRUE
            AND h.market_cap_at_recommendation IS NOT NULL
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
        put_conn(conn)
        conn = None

        if not rows:
            return "No recommendation data with market cap yet.", 200

        conn2 = get_conn()
        c2 = conn2.cursor()
        c2.execute("""
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
            SELECT q.wallet, q.token_mint,
                   CASE WHEN l.latest_mult >= q.peak_mult * 0.5 THEN TRUE ELSE FALSE END AS held
            FROM qualifying q
            JOIN latest l ON l.wallet = q.wallet AND l.token_mint = q.token_mint
            WHERE l.latest_at >= q.peak_at + (INTERVAL '1 hour' * %s)
        """, (hours,))
        held_rows = c2.fetchall()
        c2.close()
        put_conn(conn2)

        held_map = {(w, m): held for w, m, held in held_rows}

        buckets = {
            "under 100k": {"touched_total": 0, "touched_hit": 0, "held_total": 0, "held_hit": 0},
            "100k-300k": {"touched_total": 0, "touched_hit": 0, "held_total": 0, "held_hit": 0},
            "300k-500k": {"touched_total": 0, "touched_hit": 0, "held_total": 0, "held_hit": 0},
            "500k-750k": {"touched_total": 0, "touched_hit": 0, "held_total": 0, "held_hit": 0},
            "750k-1M": {"touched_total": 0, "touched_hit": 0, "held_total": 0, "held_hit": 0},
            "1M-2M": {"touched_total": 0, "touched_hit": 0, "held_total": 0, "held_hit": 0},
            "over 2M": {"touched_total": 0, "touched_hit": 0, "held_total": 0, "held_hit": 0},
        }

        for wallet, mint, mc, max_mult, hit_3x in rows:
            mc = float(mc)
            if mc < 100000:
                key = "under 100k"
            elif mc < 300000:
                key = "100k-300k"
            elif mc < 500000:
                key = "300k-500k"
            elif mc < 750000:
                key = "500k-750k"
            elif mc < 1000000:
                key = "750k-1M"
            elif mc < 2000000:
                key = "1M-2M"
            else:
                key = "over 2M"

            touched = bool(hit_3x) or (max_mult and max_mult >= 3)
            buckets[key]["touched_total"] += 1
            if touched:
                buckets[key]["touched_hit"] += 1

            if (wallet, mint) in held_map:
                buckets[key]["held_total"] += 1
                if held_map[(wallet, mint)]:
                    buckets[key]["held_hit"] += 1

        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"

        lines = [f"<b>Recommendation hit-rate by market cap AT RECOMMENDATION (detailed):</b>{range_label}<br>"]
        for bucket, d in buckets.items():
            t_total, t_hit = d["touched_total"], d["touched_hit"]
            h_total, h_hit = d["held_total"], d["held_hit"]
            if t_total == 0:
                continue
            t_rate = f"{t_hit/t_total*100:.1f}%" if t_total else "n/a"
            h_rate = f"{h_hit/h_total*100:.1f}%" if h_total else "n/a"
            lines.append(
                f"<br><b>{bucket.upper()}</b><br>"
                f"Touched 3x+: {t_hit}/{t_total} ({t_rate})<br>"
                f"Held 50%+ after {hours}h: {h_hit}/{h_total} ({h_rate})"
            )

        lines.append(
            "<br><br>⚠️ Sample sizes shrink fast in the higher buckets — treat "
            "any single bucket's numbers cautiously unless it has 20-30+ "
            "in the HELD column specifically."
        )
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_marketcap_vs_outcome_detailed error: {e}", 500

    finally:
        if conn:
            put_conn(conn)


@app.route("/check-price-velocity-by-score")
def check_price_velocity_by_score():
    since_param, until_param = get_date_filter_params()
    conn = get_conn()
    try:
        c = conn.cursor()

        query = """
            SELECT s.momentum_score, s.pc_h1, s.pc_h6
            FROM token_scan_log s
            WHERE s.momentum_alert_fired = TRUE
            AND s.suspect_data IS NOT TRUE
            AND s.pc_h1 IS NOT NULL
            AND s.pc_h6 IS NOT NULL
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
            return "No recommendation data with pc_h1/pc_h6 yet.", 200

        buckets = {
            "70-79": {"pc_h1": [], "pc_h6": []},
            "80-89": {"pc_h1": [], "pc_h6": []},
            "90-100": {"pc_h1": [], "pc_h6": []},
        }

        for score, pc_h1, pc_h6 in rows:
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

            buckets[key]["pc_h1"].append(float(pc_h1))
            buckets[key]["pc_h6"].append(float(pc_h6))

        def median(vals):
            s = sorted(vals)
            n = len(s)
            if n == 0:
                return None
            mid = n // 2
            return (s[mid - 1] + s[mid]) / 2 if n % 2 == 0 else s[mid]

        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"

        lines = [f"<b>Recent price velocity AT RECOMMENDATION, by score bucket:</b>{range_label}<br>"]
        for bucket, d in buckets.items():
            n = len(d["pc_h1"])
            if n == 0:
                lines.append(f"<br>{bucket}: no data")
                continue
            avg_h1 = sum(d["pc_h1"]) / n
            med_h1 = median(d["pc_h1"])
            avg_h6 = sum(d["pc_h6"]) / n
            med_h6 = median(d["pc_h6"])
            lines.append(
                f"<br><b>Score {bucket}</b> (n={n})<br>"
                f"1h price change — avg: {avg_h1:.1f}%, median: {med_h1:.1f}%<br>"
                f"6h price change — avg: {avg_h6:.1f}%, median: {med_h6:.1f}%"
            )

        lines.append(
            "<br><br>If 90-100 shows a noticeably higher pc_h1/pc_h6 than "
            "70-79, that confirms high scores are catching tokens right "
            "after their biggest recent move — supporting a price-velocity "
            "cap specifically for the highest score tier."
        )
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_price_velocity_by_score error: {e}", 500

    finally:
        put_conn(conn)


@app.route("/check-recommendation-timing")
def check_recommendation_timing():
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute("""
            SELECT EXTRACT(EPOCH FROM (recommended_at - first_seen_at)) / 3600 AS hours_to_recommend
            FROM wallet_token_history
            WHERE momentum_alerted = TRUE
            AND recommended_at IS NOT NULL
            AND first_seen_at IS NOT NULL
        """)
        rows = [r[0] for r in c.fetchall() if r[0] is not None]
        c.close()

        if not rows:
            return "No recommendation timing data yet.", 200

        rows.sort()
        n = len(rows)

        def percentile(p):
            idx = int(n * p)
            idx = min(idx, n - 1)
            return rows[idx]

        lines = [
            f"<b>Time from first_seen_at to recommended_at</b> (n={n})<br>",
            f"<br>Median: {percentile(0.5):.1f}h",
            f"75th percentile: {percentile(0.75):.1f}h",
            f"90th percentile: {percentile(0.90):.1f}h",
            f"95th percentile: {percentile(0.95):.1f}h",
            f"Max: {max(rows):.1f}h",
            "<br><br>If your current SCAN_WINDOW_HOURS is comfortably above "
            "the 90-95th percentile here, shrinking it is safe. If a "
            "meaningful chunk of recommendations happen close to or beyond "
            "your proposed new window, shrinking it would cut them off."
        ]
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_recommendation_timing error: {e}", 500

    finally:
        put_conn(conn)


@app.route("/check-timing-by-score")
def check_timing_by_score():
    since_param, until_param = get_date_filter_params()
    conn = get_conn()
    try:
        c = conn.cursor()

        query = """
            SELECT s.momentum_score,
                   EXTRACT(EPOCH FROM (h.recommended_at - h.first_seen_at)) / 3600 AS hours_to_rec
            FROM token_scan_log s
            JOIN wallet_token_history h
                ON h.wallet = s.wallet AND h.token_mint = s.token_mint
            WHERE s.momentum_alert_fired = TRUE
            AND s.suspect_data IS NOT TRUE
            AND h.recommended_at IS NOT NULL
            AND h.first_seen_at IS NOT NULL
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
            return "No recommendation data with timing yet.", 200

        buckets = {"70-79": [], "80-89": [], "90-100": []}

        for score, hours in rows:
            if score is None or hours is None:
                continue
            if 70 <= score < 80:
                key = "70-79"
            elif 80 <= score < 90:
                key = "80-89"
            elif score >= 90:
                key = "90-100"
            else:
                continue
            buckets[key].append(float(hours))

        def median(vals):
            s = sorted(vals)
            n = len(s)
            if n == 0:
                return None
            mid = n // 2
            return (s[mid - 1] + s[mid]) / 2 if n % 2 == 0 else s[mid]

        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"

        lines = [f"<b>Time from first_seen_at to recommended_at, by score bucket:</b>{range_label}<br>"]
        for bucket, vals in buckets.items():
            if not vals:
                lines.append(f"<br>{bucket}: no data")
                continue
            avg_h = sum(vals) / len(vals)
            med_h = median(vals)
            lines.append(
                f"<br><b>Score {bucket}</b> (n={len(vals)})<br>"
                f"avg: {avg_h:.1f}h, median: {med_h:.1f}h"
            )

        lines.append(
            "<br><br>If 90-100 shows a noticeably higher median time-to-"
            "recommendation than 70-79, that suggests high scores are "
            "systematically catching tokens that took longer to mature — "
            "possibly meaning momentum built up (or was manufactured) "
            "over more scan cycles, rather than genuine early strength."
        )
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_timing_by_score error: {e}", 500

    finally:
        put_conn(conn)


@app.route("/check-score-components-by-bucket")
def check_score_components_by_bucket():
    since_param, until_param = get_date_filter_params()
    conn = get_conn()
    try:
        c = conn.cursor()
        query = """
            SELECT s.momentum_score,
                   h.liquidity_trend_points_at_recommendation,
                   h.liquidity_level_points_at_recommendation,
                   h.price_window_points_at_recommendation,
                   h.volume_sanity_points_at_recommendation
            FROM token_scan_log s
            JOIN wallet_token_history h
                ON h.wallet = s.wallet AND h.token_mint = s.token_mint
            WHERE s.momentum_alert_fired = TRUE
            AND s.suspect_data IS NOT TRUE
            AND h.liquidity_trend_points_at_recommendation IS NOT NULL
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
            return "No recommendation data with component breakdown yet.", 200

        buckets = {"70-79": [], "80-89": [], "90-100": []}
        for score, trend, level, window, vol in rows:
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
            buckets[key].append((trend or 0, level or 0, window or 0, vol or 0))

        def avg(vals):
            return sum(vals) / len(vals) if vals else None

        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"

        lines = [f"<b>Score component breakdown by bucket:</b>{range_label}<br>"]
        for bucket, vals in buckets.items():
            if not vals:
                lines.append(f"<br>{bucket}: no data")
                continue
            trends = [v[0] for v in vals]
            levels = [v[1] for v in vals]
            windows = [v[2] for v in vals]
            vols = [v[3] for v in vals]
            lines.append(
                f"<br><b>Score {bucket}</b> (n={len(vals)})<br>"
                f"Liquidity trend pts (max 45) — avg: {avg(trends):.1f}<br>"
                f"Liquidity level pts (max 25) — avg: {avg(levels):.1f}<br>"
                f"Price window pts (max 20) — avg: {avg(windows):.1f}<br>"
                f"Volume sanity pts (-10 to +10) — avg: {avg(vols):.1f}"
            )

        lines.append(
            "<br><br>Look for which component(s) 90-100 maxes out disproportionately "
            "more than 70-79 — that's the specific factor most associated "
            "with reaching the highest score tier, and a candidate for "
            "re-examining if it's genuinely predictive or not."
        )
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_score_components_by_bucket error: {e}", 500

    finally:
        put_conn(conn)


@app.route("/check-full-profile-vs-outcome")
def check_full_profile_vs_outcome():
    since_param, until_param = get_date_filter_params()
    conn = get_conn()
    try:
        c = conn.cursor()

        query = """
            SELECT
                h.rugcheck_score_at_recommendation,
                h.top1_holder_pct_at_recommendation,
                h.market_cap_at_recommendation,
                h.buy_count_at_recommendation,
                h.cluster_count_at_recommendation,
                s.liquidity, s.vol_h1,
                h.max_multiplier_since_recommendation,
                h.pumped_since_recommendation_alerted
            FROM wallet_token_history h
            JOIN token_scan_log s
                ON s.wallet = h.wallet AND s.token_mint = h.token_mint
                AND s.momentum_alert_fired = TRUE
            WHERE h.momentum_alerted = TRUE
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
            return "No recommendation data yet.", 200

        winners = {"rug": [], "holder": [], "mc": [], "buy_count": [], "cluster": [], "vol_ratio": []}
        losers = {"rug": [], "holder": [], "mc": [], "buy_count": [], "cluster": [], "vol_ratio": []}

        for rug, holder, mc, buy_count, cluster, liquidity, vol_h1, max_mult, hit_3x in rows:
            hit = bool(hit_3x) or (max_mult and max_mult >= 3)
            bucket = winners if hit else losers

            if rug is not None:
                bucket["rug"].append(float(rug))
            if holder is not None:
                bucket["holder"].append(float(holder))
            if mc is not None:
                bucket["mc"].append(float(mc))
            if buy_count is not None:
                bucket["buy_count"].append(float(buy_count))
            if cluster is not None:
                bucket["cluster"].append(float(cluster))
            if liquidity and vol_h1 is not None:
                bucket["vol_ratio"].append(float(vol_h1) / float(liquidity))

        winner_count = sum(1 for r, h, m, b, c, l, v, mm, hh in rows if bool(hh) or (mm and mm >= 3))
        loser_count = len(rows) - winner_count

        def median(vals):
            if not vals:
                return None
            s = sorted(vals)
            n = len(s)
            mid = n // 2
            return (s[mid - 1] + s[mid]) / 2 if n % 2 == 0 else s[mid]

        def fmt(vals, kind="num"):
            m = median(vals)
            if m is None:
                return "n/a"
            if kind == "usd":
                return f"${m:,.0f}"
            return f"{m:.2f}"

        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"

        lines = [f"<b>Full profile comparison: 3x+ winners vs losers</b>{range_label}<br>"]

        lines.append(f"<br><b>WINNERS</b> (n={winner_count})")
        lines.append(f"RugCheck score — median: {fmt(winners['rug'])}")
        lines.append(f"Top holder % — median: {fmt(winners['holder'])}")
        lines.append(f"Market cap — median: {fmt(winners['mc'], 'usd')}")
        lines.append(f"Buy count — median: {fmt(winners['buy_count'])}")
        lines.append(f"Cluster count — median: {fmt(winners['cluster'])}")
        lines.append(f"Vol/liq ratio — median: {fmt(winners['vol_ratio'])}")

        lines.append(f"<br><br><b>LOSERS</b> (n={loser_count})")
        lines.append(f"RugCheck score — median: {fmt(losers['rug'])}")
        lines.append(f"Top holder % — median: {fmt(losers['holder'])}")
        lines.append(f"Market cap — median: {fmt(losers['mc'], 'usd')}")
        lines.append(f"Buy count — median: {fmt(losers['buy_count'])}")
        lines.append(f"Cluster count — median: {fmt(losers['cluster'])}")
        lines.append(f"Vol/liq ratio — median: {fmt(losers['vol_ratio'])}")

        lines.append(
            "<br><br>Compare each row between WINNERS and LOSERS — a "
            "metric with a clear, consistent gap is a genuine candidate "
            "signal. Small samples in either group should be treated "
            "cautiously per usual."
        )
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_full_profile_vs_outcome error: {e}", 500

    finally:
        put_conn(conn)


@app.route("/check-buy-trajectory-vs-outcome")
def check_buy_trajectory_vs_outcome():
    since_param, until_param = get_date_filter_params()
    max_mc = request.args.get("max_mc")
    hours = request.args.get("hours", "1")
    try:
        hours = float(hours)
    except (TypeError, ValueError):
        hours = 1.0

    conn = get_conn()
    try:
        c = conn.cursor()

        query = """
            SELECT h.wallet, h.token_mint, h.buy_trajectory_at_recommendation,
                   h.market_cap_at_recommendation,
                   h.max_multiplier_since_recommendation,
                   h.pumped_since_recommendation_alerted
            FROM wallet_token_history h
            WHERE h.momentum_alerted = TRUE
            AND h.buy_trajectory_at_recommendation IS NOT NULL
        """
        params = []
        if since_param:
            query += " AND h.recommended_at >= %s"
            params.append(since_param)
        if until_param:
            query += " AND h.recommended_at < %s"
            params.append(until_param)
        if max_mc:
            query += " AND h.market_cap_at_recommendation <= %s"
            params.append(float(max_mc))

        c.execute(query, params)
        rows = c.fetchall()
        c.close()
        put_conn(conn)
        conn = None

        if not rows:
            return "No recommendation data with buy trajectory yet.", 200

        conn2 = get_conn()
        c2 = conn2.cursor()
        c2.execute("""
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
            SELECT q.wallet, q.token_mint,
                   CASE WHEN l.latest_mult >= q.peak_mult * 0.5 THEN TRUE ELSE FALSE END AS held
            FROM qualifying q
            JOIN latest l ON l.wallet = q.wallet AND l.token_mint = q.token_mint
            WHERE l.latest_at >= q.peak_at + (INTERVAL '1 hour' * %s)
        """, (hours,))
        held_rows = c2.fetchall()
        c2.close()
        put_conn(conn2)

        held_map = {(w, m): held for w, m, held in held_rows}

        buckets = {
            "rising": {"touched_total": 0, "touched_hit": 0, "held_total": 0, "held_hit": 0},
            "falling": {"touched_total": 0, "touched_hit": 0, "held_total": 0, "held_hit": 0},
            "flat": {"touched_total": 0, "touched_hit": 0, "held_total": 0, "held_hit": 0},
        }

        for wallet, mint, trajectory, mc, max_mult, hit_3x in rows:
            if trajectory not in buckets:
                continue
            touched = bool(hit_3x) or (max_mult and max_mult >= 3)
            buckets[trajectory]["touched_total"] += 1
            if touched:
                buckets[trajectory]["touched_hit"] += 1
            if (wallet, mint) in held_map:
                buckets[trajectory]["held_total"] += 1
                if held_map[(wallet, mint)]:
                    buckets[trajectory]["held_hit"] += 1

        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"
        mc_label = f"<br>Market cap filter: ≤${float(max_mc):,.0f}<br>" if max_mc else ""

        lines = [f"<b>Buy trajectory vs outcome:</b>{range_label}{mc_label}<br>"]
        for bucket, d in buckets.items():
            t_total, t_hit = d["touched_total"], d["touched_hit"]
            h_total, h_hit = d["held_total"], d["held_hit"]
            t_rate = f"{t_hit/t_total*100:.1f}%" if t_total else "n/a"
            h_rate = f"{h_hit/h_total*100:.1f}%" if h_total else "n/a"
            lines.append(
                f"<br><b>{bucket.upper()}</b><br>"
                f"Touched 3x+: {t_hit}/{t_total} ({t_rate})<br>"
                f"Held 50%+ after {hours}h: {h_hit}/{h_total} ({h_rate})"
            )

        lines.append(
            "<br><br>If RISING outperforms FALLING/FLAT, that confirms buy "
            "trajectory (not just raw count) is the real signal. Try "
            "?max_mc=50000 to test specifically at low market cap."
        )
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_buy_trajectory_vs_outcome error: {e}", 500

    finally:
        if conn:
            put_conn(conn)


@app.route("/check-too-perfect-vs-outcome")
def check_too_perfect_vs_outcome():
    since_param, until_param = get_date_filter_params()
    conn = get_conn()
    try:
        c = conn.cursor()

        query = """
            SELECT too_perfect_penalty_applied,
                   max_multiplier_since_recommendation,
                   pumped_since_recommendation_alerted
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

        c.execute(query, params)
        rows = c.fetchall()
        c.close()

        if not rows:
            return "No recommendation data yet.", 200

        flagged = {"total": 0, "hit_3x": 0}
        not_flagged = {"total": 0, "hit_3x": 0}

        for too_perfect, max_mult, hit_3x in rows:
            bucket = flagged if too_perfect else not_flagged
            bucket["total"] += 1
            hit = bool(hit_3x) or (max_mult and max_mult >= 3)
            if hit:
                bucket["hit_3x"] += 1

        def rate(d):
            return f"{d['hit_3x']}/{d['total']} ({d['hit_3x']/d['total']*100:.1f}%)" if d["total"] else "n/a"

        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"

        lines = [f"<b>Hit-rate: too-perfect-simultaneously flagged vs not</b>{range_label}<br>"]
        lines.append(f"<br>Flagged (3+ components near max): {rate(flagged)}")
        lines.append(f"Not flagged: {rate(not_flagged)}")
        lines.append(
            "<br><br>Note: this signal is no longer subtracted from the "
            "score (removed after data showed it flipped the wrong way — "
            "flagged tokens hit 3x+ at roughly 2x the rate of non-flagged). "
            "Kept as an informational check only."
        )
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_too_perfect_vs_outcome error: {e}", 500

    finally:
        put_conn(conn)


@app.route("/check-historical-peak-ratio-vs-outcome")
def check_historical_peak_ratio_vs_outcome():
    since_param, until_param = get_date_filter_params()
    hours = request.args.get("hours", "1")
    try:
        hours = float(hours)
    except (TypeError, ValueError):
        hours = 1.0

    conn = get_conn()
    try:
        c = conn.cursor()

        query = """
            SELECT h.wallet, h.token_mint, h.recommended_at,
                   h.max_multiplier_since_recommendation,
                   h.pumped_since_recommendation_alerted
            FROM wallet_token_history h
            WHERE h.momentum_alerted = TRUE
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
        recs = c.fetchall()
        c.close()
        put_conn(conn)
        conn = None

        if not recs:
            return "No recommendation data yet.", 200

        conn2 = get_conn()
        c2 = conn2.cursor()
        c2.execute("""
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
            SELECT q.wallet, q.token_mint,
                   CASE WHEN l.latest_mult >= q.peak_mult * 0.5 THEN TRUE ELSE FALSE END AS held
            FROM qualifying q
            JOIN latest l ON l.wallet = q.wallet AND l.token_mint = q.token_mint
            WHERE l.latest_at >= q.peak_at + (INTERVAL '1 hour' * %s)
        """, (hours,))
        held_rows = c2.fetchall()
        c2.close()
        put_conn(conn2)
        held_map = {(w, m): held for w, m, held in held_rows}

        buckets = {
            "never over 10x": {"touched_total": 0, "touched_hit": 0, "held_total": 0, "held_hit": 0},
            "peaked over 10x": {"touched_total": 0, "touched_hit": 0, "held_total": 0, "held_hit": 0},
        }

        conn3 = get_conn()
        c3 = conn3.cursor()

        for wallet, mint, recommended_at, max_mult, hit_3x in recs:
            c3.execute("""
                SELECT liquidity, vol_h1
                FROM token_scan_log
                WHERE wallet = %s AND token_mint = %s
                AND scanned_at <= %s
                AND liquidity IS NOT NULL AND liquidity > 0
                AND vol_h1 IS NOT NULL
            """, (wallet, mint, recommended_at))
            prior_scans = c3.fetchall()

            if not prior_scans:
                continue

            peak_ratio = max(
                (float(vol_h1) / float(liq)) for liq, vol_h1 in prior_scans if liq
            )

            key = "peaked over 10x" if peak_ratio > 10 else "never over 10x"

            touched = bool(hit_3x) or (max_mult and max_mult >= 3)
            buckets[key]["touched_total"] += 1
            if touched:
                buckets[key]["touched_hit"] += 1

            if (wallet, mint) in held_map:
                buckets[key]["held_total"] += 1
                if held_map[(wallet, mint)]:
                    buckets[key]["held_hit"] += 1

        c3.close()
        put_conn(conn3)

        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"

        lines = [f"<b>Historical peak vol/liq ratio vs outcome:</b>{range_label}<br>"]
        for bucket, d in buckets.items():
            t_total, t_hit = d["touched_total"], d["touched_hit"]
            h_total, h_hit = d["held_total"], d["held_hit"]
            t_rate = f"{t_hit/t_total*100:.1f}%" if t_total else "n/a"
            h_rate = f"{h_hit/h_total*100:.1f}%" if h_total else "n/a"
            lines.append(
                f"<br><b>{bucket.upper()}</b><br>"
                f"Touched 3x+: {t_hit}/{t_total} ({t_rate})<br>"
                f"Held 50%+ after {hours}h: {h_hit}/{h_total} ({h_rate})"
            )

        lines.append(
            "<br><br>If 'peaked over 10x' shows meaningfully LOWER rates "
            "(especially HELD) than 'never over 10x', that validates "
            "gating on historical peak ratio."
        )
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_historical_peak_ratio_vs_outcome error: {e}", 500

    finally:
        if conn:
            put_conn(conn)


@app.route("/check-historical-peak-ratio-buckets")
def check_historical_peak_ratio_buckets():
    since_param, until_param = get_date_filter_params()
    hours = request.args.get("hours", "1")
    try:
        hours = float(hours)
    except (TypeError, ValueError):
        hours = 1.0

    conn = get_conn()
    try:
        c = conn.cursor()

        query = """
            SELECT h.wallet, h.token_mint, h.recommended_at,
                   h.max_multiplier_since_recommendation,
                   h.pumped_since_recommendation_alerted
            FROM wallet_token_history h
            WHERE h.momentum_alerted = TRUE
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
        recs = c.fetchall()
        c.close()
        put_conn(conn)
        conn = None

        if not recs:
            return "No recommendation data yet.", 200

        conn2 = get_conn()
        c2 = conn2.cursor()
        c2.execute("""
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
            SELECT q.wallet, q.token_mint,
                   CASE WHEN l.latest_mult >= q.peak_mult * 0.5 THEN TRUE ELSE FALSE END AS held
            FROM qualifying q
            JOIN latest l ON l.wallet = q.wallet AND l.token_mint = q.token_mint
            WHERE l.latest_at >= q.peak_at + (INTERVAL '1 hour' * %s)
        """, (hours,))
        held_rows = c2.fetchall()
        c2.close()
        put_conn(conn2)
        held_map = {(w, m): held for w, m, held in held_rows}

        buckets = {
            "under 5x": {"touched_total": 0, "touched_hit": 0, "held_total": 0, "held_hit": 0},
            "5-8x": {"touched_total": 0, "touched_hit": 0, "held_total": 0, "held_hit": 0},
            "8-10x": {"touched_total": 0, "touched_hit": 0, "held_total": 0, "held_hit": 0},
            "10-12x": {"touched_total": 0, "touched_hit": 0, "held_total": 0, "held_hit": 0},
            "12-15x": {"touched_total": 0, "touched_hit": 0, "held_total": 0, "held_hit": 0},
            "15-20x": {"touched_total": 0, "touched_hit": 0, "held_total": 0, "held_hit": 0},
            "over 20x": {"touched_total": 0, "touched_hit": 0, "held_total": 0, "held_hit": 0},
        }

        conn3 = get_conn()
        c3 = conn3.cursor()

        for wallet, mint, recommended_at, max_mult, hit_3x in recs:
            c3.execute("""
                SELECT liquidity, vol_h1
                FROM token_scan_log
                WHERE wallet = %s AND token_mint = %s
                AND scanned_at <= %s
                AND liquidity IS NOT NULL AND liquidity > 0
                AND vol_h1 IS NOT NULL
            """, (wallet, mint, recommended_at))
            prior_scans = c3.fetchall()

            if not prior_scans:
                continue

            peak_ratio = max(
                (float(vol_h1) / float(liq)) for liq, vol_h1 in prior_scans if liq
            )

            if peak_ratio < 5:
                key = "under 5x"
            elif peak_ratio < 8:
                key = "5-8x"
            elif peak_ratio < 10:
                key = "8-10x"
            elif peak_ratio < 12:
                key = "10-12x"
            elif peak_ratio < 15:
                key = "12-15x"
            elif peak_ratio < 20:
                key = "15-20x"
            else:
                key = "over 20x"

            touched = bool(hit_3x) or (max_mult and max_mult >= 3)
            buckets[key]["touched_total"] += 1
            if touched:
                buckets[key]["touched_hit"] += 1

            if (wallet, mint) in held_map:
                buckets[key]["held_total"] += 1
                if held_map[(wallet, mint)]:
                    buckets[key]["held_hit"] += 1

        c3.close()
        put_conn(conn3)

        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"

        lines = [f"<b>Historical peak ratio, finer buckets:</b>{range_label}<br>"]
        for bucket, d in buckets.items():
            t_total, t_hit = d["touched_total"], d["touched_hit"]
            h_total, h_hit = d["held_total"], d["held_hit"]
            t_rate = f"{t_hit/t_total*100:.1f}%" if t_total else "n/a"
            h_rate = f"{h_hit/h_total*100:.1f}%" if h_total else "n/a"
            lines.append(
                f"<br><b>{bucket.upper()}</b><br>"
                f"Touched 3x+: {t_hit}/{t_total} ({t_rate})<br>"
                f"Held 50%+ after {hours}h: {h_hit}/{h_total} ({h_rate})"
            )

        lines.append(
            "<br><br>Look for where held-rate actually drops sharply. "
            "Treat any bucket under ~15-20 samples cautiously."
        )
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_historical_peak_ratio_buckets error: {e}", 500

    finally:
        if conn:
            put_conn(conn)


@app.route("/check-buysell-ratio-at-recommendation")
def check_buysell_ratio_at_recommendation():
    since_param, until_param = get_date_filter_params()
    conn = get_conn()
    try:
        c = conn.cursor()

        query = """
            SELECT h.wallet, h.token_mint, h.recommended_at,
                   s.buys_5m, s.sells_5m,
                   h.max_multiplier_since_recommendation,
                   h.pumped_since_recommendation_alerted
            FROM wallet_token_history h
            JOIN token_scan_log s
                ON s.wallet = h.wallet AND s.token_mint = h.token_mint
                AND s.momentum_alert_fired = TRUE
            WHERE h.momentum_alerted = TRUE
        """
        params = []
        if since_param:
            query += " AND h.recommended_at >= %s"
            params.append(since_param)
        if until_param:
            query += " AND h.recommended_at < %s"
            params.append(until_param)
        query += " ORDER BY h.recommended_at ASC"

        c.execute(query, params)
        rows = c.fetchall()
        c.close()

        if not rows:
            return "No recommendation data in this range.", 200

        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"

        lines = [f"<b>Buy/sell ratio at recommendation:</b>{range_label}<br>"]
        for wallet, mint, recommended_at, buys, sells, max_mult, hit_3x in rows:
            hit = bool(hit_3x) or (max_mult and max_mult >= 3)
            outcome_label = "✅ hit 3x+" if hit else "❌ no hit"

            if buys is not None and sells is not None and sells > 0:
                ratio = buys / sells
                ratio_str = f"{ratio:.1f}:1"
                flag = " ⚠️ EXTREME SKEW" if ratio >= 10 else ""
            elif buys is not None and sells == 0:
                ratio_str = f"{buys}:0 (infinite)"
                flag = " ⚠️ EXTREME SKEW"
            else:
                ratio_str = "n/a"
                flag = ""

            lines.append(
                f"<br><code>{mint}</code><br>"
                f"Recommended: {recommended_at} | Buys/Sells: {buys}/{sells} "
                f"(ratio {ratio_str}){flag} | Outcome: {outcome_label}"
            )

        lines.append(
            "<br><br>Look for '⚠️ EXTREME SKEW' flags (ratio ≥10:1) — if "
            "these disproportionately show '❌ no hit' or correlate with "
            "known rugs, that validates a buy/sell ratio gate."
        )
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_buysell_ratio_at_recommendation error: {e}", 500

    finally:
        put_conn(conn)


@app.route("/check-buysell-ratio-vs-outcome")
def check_buysell_ratio_vs_outcome():
    since_param, until_param = get_date_filter_params()
    conn = get_conn()
    try:
        c = conn.cursor()

        query = """
            SELECT s.buys_5m, s.sells_5m,
                   h.max_multiplier_since_recommendation,
                   h.pumped_since_recommendation_alerted
            FROM wallet_token_history h
            JOIN token_scan_log s
                ON s.wallet = h.wallet AND s.token_mint = h.token_mint
                AND s.momentum_alert_fired = TRUE
            WHERE h.momentum_alerted = TRUE
            AND s.buys_5m IS NOT NULL AND s.sells_5m IS NOT NULL
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
            return "No recommendation data with buy/sell counts yet.", 200

        buckets = {
            "under 2x": {"total": 0, "hit_3x": 0},
            "2-5x": {"total": 0, "hit_3x": 0},
            "5-10x": {"total": 0, "hit_3x": 0},
            "over 10x": {"total": 0, "hit_3x": 0},
        }

        for buys, sells, max_mult, hit_3x in rows:
            if sells == 0:
                ratio = float(buys) if buys else 0
            else:
                ratio = buys / sells

            if ratio < 2:
                key = "under 2x"
            elif ratio < 5:
                key = "2-5x"
            elif ratio < 10:
                key = "5-10x"
            else:
                key = "over 10x"

            buckets[key]["total"] += 1
            hit = bool(hit_3x) or (max_mult and max_mult >= 3)
            if hit:
                buckets[key]["hit_3x"] += 1

        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"

        lines = [f"<b>Buy/sell ratio at recommendation vs outcome:</b>{range_label}<br>"]
        for bucket, d in buckets.items():
            total = d["total"]
            hits = d["hit_3x"]
            rate = f"{hits/total*100:.1f}%" if total else "n/a"
            lines.append(f"<br>Ratio {bucket}: {hits}/{total} hit 3x+ ({rate})")

        lines.append(
            "<br><br>If 'over 10x' shows meaningfully LOWER hit rate than "
            "the other buckets, that validates a buy/sell ratio gate."
        )
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_buysell_ratio_vs_outcome error: {e}", 500

    finally:
        put_conn(conn)


@app.route("/check-buysell-ratio-vs-rug-rate")
def check_buysell_ratio_vs_rug_rate():
    since_param, until_param = get_date_filter_params()
    conn = get_conn()
    try:
        c = conn.cursor()

        query = """
            SELECT s.buys_5m, s.sells_5m,
                   h.max_drawdown_seen
            FROM wallet_token_history h
            JOIN token_scan_log s
                ON s.wallet = h.wallet AND s.token_mint = h.token_mint
                AND s.momentum_alert_fired = TRUE
            WHERE h.momentum_alerted = TRUE
            AND s.buys_5m IS NOT NULL AND s.sells_5m IS NOT NULL
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
            return "No recommendation data with buy/sell counts yet.", 200

        buckets = {
            "under 2x": {"total": 0, "rugged": 0},
            "2-5x": {"total": 0, "rugged": 0},
            "5-10x": {"total": 0, "rugged": 0},
            "over 10x": {"total": 0, "rugged": 0},
        }

        for buys, sells, max_drawdown in rows:
            if sells == 0:
                ratio = float(buys) if buys else 0
            else:
                ratio = buys / sells

            if ratio < 2:
                key = "under 2x"
            elif ratio < 5:
                key = "2-5x"
            elif ratio < 10:
                key = "5-10x"
            else:
                key = "over 10x"

            buckets[key]["total"] += 1
            rugged = max_drawdown is not None and float(max_drawdown) >= 0.8
            if rugged:
                buckets[key]["rugged"] += 1

        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"

        lines = [f"<b>Buy/sell ratio at recommendation vs RUG rate (80%+ drawdown):</b>{range_label}<br>"]
        for bucket, d in buckets.items():
            total = d["total"]
            rugged = d["rugged"]
            rate = f"{rugged/total*100:.1f}%" if total else "n/a"
            lines.append(f"<br>Ratio {bucket}: {rugged}/{total} rugged ({rate})")

        lines.append(
            "<br><br>If 'over 10x' shows meaningfully HIGHER rug rate than "
            "the other buckets, that confirms extreme buy/sell skew is a "
            "genuine manipulation/collapse signal."
        )
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_buysell_ratio_vs_rug_rate error: {e}", 500

    finally:
        put_conn(conn)


@app.route("/check-never-recommended-winners")
def check_never_recommended_winners():
    since_param, until_param = get_date_filter_params()
    hours = request.args.get("hours", "1")
    try:
        hours = float(hours)
    except (TypeError, ValueError):
        hours = 1.0

    limit_param = request.args.get("limit", "30")
    try:
        result_limit = int(limit_param)
    except (TypeError, ValueError):
        result_limit = 30

    conn = get_conn()
    try:
        c = conn.cursor()

        query = """
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
            held_tokens AS (
                SELECT q.wallet, q.token_mint, q.peak_mult
                FROM qualifying q
                JOIN latest l ON l.wallet = q.wallet AND l.token_mint = q.token_mint
                WHERE l.latest_at >= q.peak_at + (INTERVAL '1 hour' * %s)
                AND l.latest_mult >= q.peak_mult * 0.5
            )
            SELECT h.wallet, h.token_mint, h.peak_mult, w.first_seen_at
            FROM held_tokens h
            JOIN wallet_token_history w
                ON w.wallet = h.wallet AND w.token_mint = h.token_mint
            WHERE w.momentum_alerted = FALSE
        """
        params = [hours]
        if since_param:
            query += " AND w.first_seen_at >= %s"
            params.append(since_param)
        if until_param:
            query += " AND w.first_seen_at < %s"
            params.append(until_param)
        query += " ORDER BY h.peak_mult DESC"

        c.execute(query, params)
        winners = c.fetchall()
        c.close()
        put_conn(conn)
        conn = None

        if not winners:
            return f"No never-recommended tokens that both peaked 3x+ AND held 50%+ after {hours}h in this range.", 200

        conn2 = get_conn()
        c2 = conn2.cursor()

        lines = []
        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"
        lines.append(f"<b>Never-recommended tokens that peaked 3x+ AND held 50%+ after {hours}h:</b>{range_label}<br>")

        for wallet, mint, peak_mult, first_seen in winners[:result_limit]:
            c2.execute("""
                SELECT MAX(momentum_score)
                FROM token_scan_log
                WHERE wallet = %s AND token_mint = %s
            """, (wallet, mint))
            peak_score_row = c2.fetchone()
            peak_score = peak_score_row[0] if peak_score_row else None

            c2.execute(
                "SELECT block_reason_at_last_attempt FROM wallet_token_history WHERE wallet=%s AND token_mint=%s",
                (wallet, mint)
            )
            block_reason_row = c2.fetchone()
            block_reason = block_reason_row[0] if block_reason_row else None

            if peak_score is not None and peak_score >= 70:
                if block_reason:
                    reason = f"⛔ BLOCKED: {block_reason}"
                else:
                    reason = "⛔ BLOCKED BY A HARD GATE (crossed 70 before this fix was deployed — reason not recorded)"
            else:
                reason = f"📉 Score never crossed 70 (peak: {peak_score})"

            lines.append(
                f"<br><code>{mint}</code> — peaked {float(peak_mult):.2f}x, HELD 50%+ after {hours}h<br>"
                f"{reason}"
            )

        c2.close()
        put_conn(conn2)

        if len(winners) > result_limit:
            lines.append(f"<br><br>Showing top {result_limit} of {len(winners)} total. Use ?limit= to see more.")

        lines.append(
            "<br><br>These are genuine sustained winners (not just brief spikes), "
            "so any near-miss here is a real, meaningful research lead — "
            "worth pulling full scan history to see what held them back."
        )
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_never_recommended_winners error: {e}", 500

    finally:
        if conn:
            put_conn(conn)


@app.route("/check-post-recommendation-decline")
def check_post_recommendation_decline():
    since_param, until_param = get_date_filter_params()
    conn = get_conn()
    try:
        c = conn.cursor()

        query = """
            SELECT h.wallet, h.token_mint, h.recommended_at
            FROM wallet_token_history h
            WHERE h.momentum_alerted = TRUE
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
        recs = c.fetchall()
        c.close()
        put_conn(conn)
        conn = None

        if not recs:
            return "No recommendation data yet.", 200

        checkpoints = {"1h": [], "3h": [], "6h": []}
        checkpoint_hours = {"1h": 1, "3h": 3, "6h": 6}

        conn2 = get_conn()
        c2 = conn2.cursor()

        for wallet, mint, recommended_at in recs:
            for label, hrs in checkpoint_hours.items():
                c2.execute("""
                    SELECT multiplier_since_recommendation
                    FROM token_scan_log
                    WHERE wallet = %s AND token_mint = %s
                    AND scanned_at >= %s
                    AND multiplier_since_recommendation IS NOT NULL
                    AND suspect_data IS NOT TRUE
                    ORDER BY scanned_at ASC
                    LIMIT 1
                """, (wallet, mint, recommended_at + datetime.timedelta(hours=hrs)))
                row = c2.fetchone()
                if row and row[0] is not None:
                    checkpoints[label].append(float(row[0]))

        c2.close()
        put_conn(conn2)

        def median(vals):
            s = sorted(vals)
            n = len(s)
            if n == 0:
                return None
            mid = n // 2
            return (s[mid - 1] + s[mid]) / 2 if n % 2 == 0 else s[mid]

        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"

        lines = [f"<b>Post-recommendation multiplier distribution:</b>{range_label}<br>"]
        for label, vals in checkpoints.items():
            if not vals:
                lines.append(f"<br>{label}: no data")
                continue
            n = len(vals)
            avg = sum(vals) / n
            med = median(vals)
            below_50pct = sum(1 for v in vals if v < 0.5) / n * 100
            below_70pct = sum(1 for v in vals if v < 0.7) / n * 100
            lines.append(
                f"<br><b>{label} after recommendation</b> (n={n})<br>"
                f"avg: {avg:.2f}x, median: {med:.2f}x<br>"
                f"Below 0.5x (down 50%+): {below_50pct:.1f}%<br>"
                f"Below 0.7x (down 30%+): {below_70pct:.1f}%"
            )

        lines.append(
            "<br><br>Use this to pick a sensible decline-alert threshold — "
            "if e.g. 40% of tokens are already below 0.7x by 1h normally, "
            "alerting at that level would be mostly noise. Look for a "
            "threshold that's genuinely rare/abnormal, not routine."
        )
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_post_recommendation_decline error: {e}", 500

    finally:
        if conn:
            put_conn(conn)


@app.route("/check-decline-then-outcome")
def check_decline_then_outcome():
    since_param, until_param = get_date_filter_params()
    conn = get_conn()
    try:
        c = conn.cursor()

        query = """
            SELECT h.wallet, h.token_mint, h.recommended_at,
                   h.max_multiplier_since_recommendation,
                   h.pumped_since_recommendation_alerted
            FROM wallet_token_history h
            WHERE h.momentum_alerted = TRUE
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
        recs = c.fetchall()
        c.close()
        put_conn(conn)
        conn = None

        if not recs:
            return "No recommendation data yet.", 200

        conn2 = get_conn()
        c2 = conn2.cursor()

        declined_early = {"total": 0, "eventually_hit_3x": 0}
        did_not_decline = {"total": 0, "eventually_hit_3x": 0}

        for wallet, mint, recommended_at, max_mult, hit_3x in recs:
            c2.execute("""
                SELECT multiplier_since_recommendation
                FROM token_scan_log
                WHERE wallet = %s AND token_mint = %s
                AND scanned_at >= %s
                AND multiplier_since_recommendation IS NOT NULL
                AND suspect_data IS NOT TRUE
                ORDER BY scanned_at ASC
                LIMIT 1
            """, (wallet, mint, recommended_at + datetime.timedelta(hours=1)))
            row = c2.fetchone()
            if not row or row[0] is None:
                continue

            mult_at_1h = float(row[0])
            eventually_hit = bool(hit_3x) or (max_mult and max_mult >= 3)

            if mult_at_1h < 0.7:
                declined_early["total"] += 1
                if eventually_hit:
                    declined_early["eventually_hit_3x"] += 1
            else:
                did_not_decline["total"] += 1
                if eventually_hit:
                    did_not_decline["eventually_hit_3x"] += 1

        c2.close()
        put_conn(conn2)

        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"

        lines = [f"<b>Down 30%+ at 1h vs eventual 3x+ outcome:</b>{range_label}<br>"]
        for label, d in [("DOWN 30%+ AT 1H", declined_early), ("NOT DOWN 30%+ AT 1H", did_not_decline)]:
            total, hits = d["total"], d["eventually_hit_3x"]
            rate = f"{hits/total*100:.1f}%" if total else "n/a"
            lines.append(f"<br><b>{label}</b>: {hits}/{total} eventually hit 3x+ ({rate})")

        lines.append(
            "<br><br>If 'DOWN 30%+ AT 1H' shows a MUCH lower eventual hit "
            "rate than the other group, that validates the eye-test — "
            "early decline genuinely predicts a dead token, and a decline "
            "alert (flagging these specifically, not all dips) is "
            "a real, non-noisy signal."
        )
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_decline_then_outcome error: {e}", 500

    finally:
        if conn:
            put_conn(conn)


@app.route("/check-score-threshold-finer")
def check_score_threshold_finer():
    since_param, until_param = get_date_filter_params()
    hours = request.args.get("hours", "1")
    try:
        hours = float(hours)
    except (TypeError, ValueError):
        hours = 1.0

    conn = get_conn()
    try:
        c = conn.cursor()

        query = """
            SELECT h.wallet, h.token_mint, s.momentum_score,
                   h.max_multiplier_since_recommendation,
                   h.pumped_since_recommendation_alerted
            FROM wallet_token_history h
            JOIN token_scan_log s
                ON s.wallet = h.wallet AND s.token_mint = h.token_mint
                AND s.momentum_alert_fired = TRUE
            WHERE h.momentum_alerted = TRUE
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
        put_conn(conn)
        conn = None

        if not rows:
            return "No recommendation data yet.", 200

        conn2 = get_conn()
        c2 = conn2.cursor()
        c2.execute("""
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
            SELECT q.wallet, q.token_mint,
                   CASE WHEN l.latest_mult >= q.peak_mult * 0.5 THEN TRUE ELSE FALSE END AS held
            FROM qualifying q
            JOIN latest l ON l.wallet = q.wallet AND l.token_mint = q.token_mint
            WHERE l.latest_at >= q.peak_at + (INTERVAL '1 hour' * %s)
        """, (hours,))
        held_rows = c2.fetchall()
        c2.close()
        put_conn(conn2)
        held_map = {(w, m): held for w, m, held in held_rows}

        buckets = {
            "70-74": {"touched_total": 0, "touched_hit": 0, "held_total": 0, "held_hit": 0},
            "75-79": {"touched_total": 0, "touched_hit": 0, "held_total": 0, "held_hit": 0},
            "80-84": {"touched_total": 0, "touched_hit": 0, "held_total": 0, "held_hit": 0},
            "85-89": {"touched_total": 0, "touched_hit": 0, "held_total": 0, "held_hit": 0},
            "90-94": {"touched_total": 0, "touched_hit": 0, "held_total": 0, "held_hit": 0},
            "95-100": {"touched_total": 0, "touched_hit": 0, "held_total": 0, "held_hit": 0},
        }

        for wallet, mint, score, max_mult, hit_3x in rows:
            if score is None:
                continue
            if 70 <= score < 75:
                key = "70-74"
            elif 75 <= score < 80:
                key = "75-79"
            elif 80 <= score < 85:
                key = "80-84"
            elif 85 <= score < 90:
                key = "85-89"
            elif 90 <= score < 95:
                key = "90-94"
            elif score >= 95:
                key = "95-100"
            else:
                continue

            touched = bool(hit_3x) or (max_mult and max_mult >= 3)
            buckets[key]["touched_total"] += 1
            if touched:
                buckets[key]["touched_hit"] += 1

            if (wallet, mint) in held_map:
                buckets[key]["held_total"] += 1
                if held_map[(wallet, mint)]:
                    buckets[key]["held_hit"] += 1

        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"

        lines = [f"<b>Finer score threshold check (5-point bands):</b>{range_label}<br>"]
        for bucket, d in buckets.items():
            t_total, t_hit = d["touched_total"], d["touched_hit"]
            h_total, h_hit = d["held_total"], d["held_hit"]
            t_rate = f"{t_hit/t_total*100:.1f}%" if t_total else "n/a"
            h_rate = f"{h_hit/h_total*100:.1f}%" if h_total else "n/a"
            lines.append(
                f"<br><b>Score {bucket}</b><br>"
                f"Touched 3x+: {t_hit}/{t_total} ({t_rate})<br>"
                f"Held 50%+ after {hours}h: {h_hit}/{h_total} ({h_rate})"
            )

        lines.append(
            "<br><br>Look for where held-rate meaningfully improves. If "
            "70-74 performs similarly to 90-95, 70 isn't a special cutoff "
            "— just where you happened to draw the line. If there's a "
            "real jump somewhere (e.g. 80+), that threshold might be worth "
            "adopting instead. Treat bands under ~15-20 samples cautiously."
        )
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_score_threshold_finer error: {e}", 500

    finally:
        if conn:
            put_conn(conn)


@app.route("/check-loser-decline-timing")
def check_loser_decline_timing():
    since_param, until_param = get_date_filter_params()
    conn = get_conn()
    try:
        c = conn.cursor()

        query = """
            SELECT h.wallet, h.token_mint, h.recommended_at,
                   h.max_multiplier_since_recommendation,
                   h.pumped_since_recommendation_alerted
            FROM wallet_token_history h
            WHERE h.momentum_alerted = TRUE
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
        recs = c.fetchall()
        c.close()
        put_conn(conn)
        conn = None

        if not recs:
            return "No recommendation data yet.", 200

        conn2 = get_conn()
        c2 = conn2.cursor()

        loser_decline_times = []

        for wallet, mint, recommended_at, max_mult, hit_3x in recs:
            eventually_hit = bool(hit_3x) or (max_mult and max_mult >= 3)
            if eventually_hit:
                continue

            c2.execute("""
                SELECT scanned_at, multiplier_since_recommendation
                FROM token_scan_log
                WHERE wallet = %s AND token_mint = %s
                AND scanned_at > %s
                AND multiplier_since_recommendation IS NOT NULL
                AND suspect_data IS NOT TRUE
                ORDER BY scanned_at ASC
            """, (wallet, mint, recommended_at))
            scans = c2.fetchall()

            for scanned_at, mult in scans:
                if float(mult) < 0.9:
                    minutes_elapsed = (scanned_at - recommended_at).total_seconds() / 60
                    loser_decline_times.append(minutes_elapsed)
                    break

        c2.close()
        put_conn(conn2)

        if not loser_decline_times:
            return "No losers with a confirmed 10%+ decline found yet.", 200

        loser_decline_times.sort()
        n = len(loser_decline_times)

        def percentile(p):
            idx = int(n * p)
            idx = min(idx, n - 1)
            return loser_decline_times[idx]

        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"

        lines = [
            f"<b>Time until losers first drop 10%+ below recommendation price</b> (n={n}){range_label}<br>",
            f"<br>Median: {percentile(0.5):.1f} min",
            f"25th percentile: {percentile(0.25):.1f} min",
            f"75th percentile: {percentile(0.75):.1f} min",
            f"90th percentile: {percentile(0.90):.1f} min",
            "<br><br>If the median/25th percentile is well under your current "
            "scan cycle time, a one-cycle confirmation wait would catch most "
            "losers early. If it's longer than your cycle time, waiting one "
            "cycle wouldn't help much — losers reveal themselves too slowly "
            "for a single delayed check to catch."
        ]
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_loser_decline_timing error: {e}", 500

    finally:
        if conn:
            put_conn(conn)


@app.route("/paper-trades")
def paper_trades_report():
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute("""
            SELECT wallet, token_mint, entry_price, entry_time, peak_price,
                   remaining_pct, status, close_reason, closed_at, realized_return_pct
            FROM paper_trades
            ORDER BY entry_time DESC
        """)
        rows = c.fetchall()
        c.close()

        if not rows:
            return "No paper trades yet.", 200

        open_trades = [r for r in rows if r[6] == "open"]
        closed_trades = [r for r in rows if r[6] == "closed"]

        lines = ["<b>Paper Trading Report</b><br>"]
        lines.append(f"<br>Open positions: {len(open_trades)}")
        lines.append(f"Closed positions: {len(closed_trades)}")

        if closed_trades:
            returns = [float(r[9]) / 100 for r in closed_trades if r[9] is not None]
            if returns:
                avg_return = sum(returns) / len(returns)
                wins = sum(1 for r in returns if r > 1.0)
                win_rate = wins / len(returns) * 100
                lines.append(f"<br>Average realized return: {avg_return:.2f}x")
                lines.append(f"Win rate (return &gt; 1.0x): {wins}/{len(returns)} ({win_rate:.1f}%)")

                by_reason = {}
                for r in closed_trades:
                    reason = r[7] or "unknown"
                    by_reason[reason] = by_reason.get(reason, 0) + 1
                lines.append("<br><br><b>Closed by reason:</b>")
                for reason, count in by_reason.items():
                    lines.append(f"<br>{reason}: {count}")

        lines.append("<br><br><b>Open positions:</b>")
        if not open_trades:
            lines.append("<br>None")
        for wallet, mint, entry_price, entry_time, peak_price, remaining_pct, status, close_reason, closed_at, realized in open_trades[:20]:
            lines.append(
                f"<br><code>{mint}</code><br>"
                f"Entry: ${entry_price} at {entry_time} | Peak: ${peak_price} | "
                f"Remaining: {remaining_pct}%"
            )

        lines.append("<br><br><b>Recent closed positions:</b>")
        for wallet, mint, entry_price, entry_time, peak_price, remaining_pct, status, close_reason, closed_at, realized in closed_trades[:20]:
            realized_mult = float(realized) / 100 if realized is not None else 0
            lines.append(
                f"<br><code>{mint}</code><br>"
                f"Entry: ${entry_price} | Peak: ${peak_price} | "
                f"Closed: {close_reason} at {closed_at} | "
                f"Realized: {realized_mult:.2f}x"
            )

        return "<br>".join(lines), 200

    except Exception as e:
        return f"paper_trades_report error: {e}", 500

    finally:
        put_conn(conn)


@app.route("/check-gate-false-positive-rate")
def check_gate_false_positive_rate():
    """
    For tokens blocked SOLELY by historical_peak_ratio or holder_pct (no
    other block reason), checks what fraction went on to become genuine
    sustained winners (peaked 3x+ AND held 50%+ after N hours). This is
    the direct test of whether these two gates are costing more real wins
    than they're preventing real losses — pulled together with the
    never-recommended-winners logic already built tonight.
    """
    since_param, until_param = get_date_filter_params()
    hours = request.args.get("hours", "1")
    try:
        hours = float(hours)
    except (TypeError, ValueError):
        hours = 1.0

    conn = get_conn()
    try:
        c = conn.cursor()
        query = """
            SELECT wallet, token_mint, block_reason_at_last_attempt
            FROM wallet_token_history
            WHERE block_reason_at_last_attempt IS NOT NULL
            AND momentum_alerted = FALSE
        """
        params = []
        if since_param:
            query += " AND first_seen_at >= %s"
            params.append(since_param)
        if until_param:
            query += " AND first_seen_at < %s"
            params.append(until_param)

        c.execute(query, params)
        blocked_rows = c.fetchall()
        c.close()
        put_conn(conn)
        conn = None

        # Only tokens blocked SOLELY by one of these two reasons (no other
        # gate also triggered) — isolates the specific gate's impact
        historical_only = []
        holder_only = []
        for wallet, mint, reason in blocked_rows:
            reasons = [r.strip().split("(")[0] for r in reason.split(",")]
            if reasons == ["historical_peak_ratio"]:
                historical_only.append((wallet, mint))
            elif reasons == ["holder_pct"]:
                holder_only.append((wallet, mint))

        held_map = get_held_map(hours)

        def evaluate(pairs):
            total = len(pairs)
            held = sum(1 for wm in pairs if held_map.get(wm) is True)
            touched = sum(1 for wm in pairs if wm in held_map)
            return total, touched, held

        hist_total, hist_touched, hist_held = evaluate(historical_only)
        holder_total, holder_touched, holder_held = evaluate(holder_only)

        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"

        lines = [f"<b>Gate false-positive check (tokens blocked SOLELY by one gate):</b>{range_label}<br>"]

        lines.append(f"<br><b>HISTORICAL_PEAK_RATIO-only blocks</b> (n={hist_total})")
        lines.append(f"Peaked 3x+ AND still trackable for held-check: {hist_touched}")
        lines.append(f"Of those, genuinely held 50%+ after {hours}h: {hist_held}")
        hist_rate = f"{hist_held/hist_touched*100:.1f}%" if hist_touched else "n/a"
        lines.append(f"→ Held rate among blocked tokens that reached peak-check: {hist_rate}")

        lines.append(f"<br><br><b>HOLDER_PCT-only blocks</b> (n={holder_total})")
        lines.append(f"Peaked 3x+ AND still trackable for held-check: {holder_touched}")
        lines.append(f"Of those, genuinely held 50%+ after {hours}h: {holder_held}")
        holder_rate = f"{holder_held/holder_touched*100:.1f}%" if holder_touched else "n/a"
        lines.append(f"→ Held rate among blocked tokens that reached peak-check: {holder_rate}")

        lines.append(
            "<br><br>Compare these held-rates against your baseline "
            "(check /check-combined-signal-vs-outcome or /stats for context). "
            "If either rate is comparable to or higher than tokens that DID "
            "get recommended, that's real evidence the gate is net-costly. "
            "If it's near zero, the gate is correctly filtering — remember "
            "n needs to be 15-20+ before trusting this either way."
        )
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_gate_false_positive_rate error: {e}", 500

    finally:
        if conn:
            put_conn(conn)


@app.route("/check-gate-blocked-otherwise-clean")
def check_gate_blocked_otherwise_clean():
    """
    For tokens blocked SOLELY by historical_peak_ratio or holder_pct,
    splits them into "otherwise clean" (RugCheck <=15 AND the other of
    the two concentration/ratio signals also looks fine) vs "otherwise
    marginal" — to test whether a token that's clean everywhere else
    still benefits meaningfully from being gated on the one remaining
    risky signal, or whether the other signals already capture the risk.
    """
    since_param, until_param = get_date_filter_params()
    hours = request.args.get("hours", "1")
    try:
        hours = float(hours)
    except (TypeError, ValueError):
        hours = 1.0

    conn = get_conn()
    try:
        c = conn.cursor()
        query = """
            SELECT wallet, token_mint, block_reason_at_last_attempt
            FROM wallet_token_history
            WHERE block_reason_at_last_attempt IS NOT NULL
            AND momentum_alerted = FALSE
        """
        params = []
        if since_param:
            query += " AND first_seen_at >= %s"
            params.append(since_param)
        if until_param:
            query += " AND first_seen_at < %s"
            params.append(until_param)

        c.execute(query, params)
        blocked_rows = c.fetchall()
        c.close()
        put_conn(conn)
        conn = None

        # Isolate solely-blocked tokens for each gate
        historical_only = [(w, m, r) for w, m, r in blocked_rows
                            if len([x.strip() for x in r.split(",")]) == 1
                            and r.strip().startswith("historical_peak_ratio")]
        holder_only = [(w, m, r) for w, m, r in blocked_rows
                        if len([x.strip() for x in r.split(",")]) == 1
                        and r.strip().startswith("holder_pct")]

        held_map = get_held_map(hours)

        conn2 = get_conn()
        c2 = conn2.cursor()

        def classify_and_evaluate(triples, other_signal_check):
            """other_signal_check(wallet, mint) -> True if 'otherwise clean'"""
            clean = []
            marginal = []
            for wallet, mint, reason in triples:
                # Pull the token's last known RugCheck score / holder pct
                # from the last real scan before it was blocked, via
                # token_scan_log momentum_score peak as a proxy for "did
                # it ever look genuinely strong elsewhere"
                c2.execute("""
                    SELECT MAX(momentum_score)
                    FROM token_scan_log
                    WHERE wallet = %s AND token_mint = %s
                """, (wallet, mint))
                peak_row = c2.fetchone()
                peak_score = peak_row[0] if peak_row and peak_row[0] is not None else 0

                is_clean = other_signal_check(peak_score)
                if is_clean:
                    clean.append((wallet, mint))
                else:
                    marginal.append((wallet, mint))
            return clean, marginal

        # "Otherwise clean" proxy: peak momentum score was itself high
        # (85+) despite the one gate — i.e. everything else about it
        # looked strong, not just barely crossing 70.
        hist_clean, hist_marginal = classify_and_evaluate(
            historical_only, lambda score: score >= 85
        )
        holder_clean, holder_marginal = classify_and_evaluate(
            holder_only, lambda score: score >= 85
        )

        c2.close()
        put_conn(conn2)

        def evaluate(pairs):
            total = len(pairs)
            touched = sum(1 for wm in pairs if wm in held_map)
            held = sum(1 for wm in pairs if held_map.get(wm) is True)
            rate = f"{held/touched*100:.1f}%" if touched else "n/a"
            return total, touched, held, rate

        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"

        lines = [f"<b>Gate-blocked tokens: otherwise-clean (score 85+) vs otherwise-marginal:</b>{range_label}<br>"]

        lines.append("<br><b>HISTORICAL_PEAK_RATIO-only blocks</b>")
        t, tr, h, r = evaluate(hist_clean)
        lines.append(f"Otherwise clean (peak score 85+): n={t}, trackable={tr}, held={h} ({r})")
        t, tr, h, r = evaluate(hist_marginal)
        lines.append(f"Otherwise marginal (peak score &lt;85): n={t}, trackable={tr}, held={h} ({r})")

        lines.append("<br><br><b>HOLDER_PCT-only blocks</b>")
        t, tr, h, r = evaluate(holder_clean)
        lines.append(f"Otherwise clean (peak score 85+): n={t}, trackable={tr}, held={h} ({r})")
        t, tr, h, r = evaluate(holder_marginal)
        lines.append(f"Otherwise marginal (peak score &lt;85): n={t}, trackable={tr}, held={h} ({r})")

        lines.append(
            "<br><br>If 'otherwise clean' held-rate is meaningfully higher than "
            "'otherwise marginal' AND comparable to your normal recommended-token "
            "baseline, that supports the theory that other signals already "
            "capture the risk for genuinely strong tokens — worth loosening "
            "the gate for high-scoring cases specifically. If both subsets "
            "look similarly low, the gate is catching something independent "
            "of overall score quality, and shouldn't be loosened based on "
            "score alone. Treat any group under 15-20 samples cautiously."
        )
        return "<br>".join(lines), 200

    except Exception as e:
        return f"check_gate_blocked_otherwise_clean error: {e}", 500

    finally:
        if conn:
            put_conn(conn)


@app.route("/check-lowcap-quality-interaction")
def check_lowcap_quality_interaction():
    """
    Splits under-300K-market-cap recommendations into "also scored 85+"
    vs "scored 70-84" to test whether the market cap signal is real on
    its own, or only meaningful when combined with an already-strong
    score elsewhere.
    """
    since_param, until_param = get_date_filter_params()
    hours = request.args.get("hours", "1")
    try:
        hours = float(hours)
    except (TypeError, ValueError):
        hours = 1.0

    conn = get_conn()
    try:
        c = conn.cursor()
        query = """
            SELECT h.wallet, h.token_mint, s.momentum_score,
                   h.max_multiplier_since_recommendation,
                   h.pumped_since_recommendation_alerted
            FROM wallet_token_history h
            JOIN token_scan_log s
                ON s.wallet = h.wallet AND s.token_mint = h.token_mint
                AND s.momentum_alert_fired = TRUE
            WHERE h.momentum_alerted = TRUE
            AND h.market_cap_at_recommendation IS NOT NULL
            AND h.market_cap_at_recommendation < 300000
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
        put_conn(conn)
        conn = None

        if not rows:
            return "No under-300K recommendation data yet.", 200

        held_map = get_held_map(hours)

        buckets = bucketize_outcome(
            rows,
            bucket_fn=lambda rest: "high score (85+)" if rest[2] is not None and rest[2] >= 85 else "lower score (70-84)",
            bucket_order=["high score (85+)", "lower score (70-84)"],
            held_map=held_map,
            wallet_mint_fn=lambda row: (row[0], row[1])
        )

        range_label = ""
        if since_param or until_param:
            range_label = f"<br>Filtered: since={since_param or 'start'}, until={until_param or 'now'}<br>"

        return format_bucket_report(
            "Under-300K market cap: high score vs lower score",
            buckets, ["high score (85+)", "lower score (70-84)"],
            "If 'high score' shows meaningfully better held-rate than 'lower "
            "score', the market cap bonus should be conditional on already "
            "scoring well, not flat. If both are similar, the flat bonus "
            "already applied is correctly designed.",
            range_label=range_label
        ), 200

    except Exception as e:
        return f"check_lowcap_quality_interaction error: {e}", 500

    finally:
        if conn:
            put_conn(conn)


@app.route("/export-shadow-dataset")
def export_shadow_dataset():
    since_param, until_param = get_date_filter_params()
    hours = request.args.get("hours", "1")
    try:
        hours = float(hours)
    except (TypeError, ValueError):
        hours = 1.0

    conn = get_conn()
    try:
        c = conn.cursor()
        query = """
            SELECT
                h.wallet, h.token_mint,
                s.momentum_score,
                h.market_cap_at_recommendation,
                h.rugcheck_score_at_recommendation,
                h.top1_holder_pct_at_recommendation,
                h.historical_peak_ratio_at_recommendation,
                h.buy_trajectory_at_recommendation,
                h.buy_count_at_recommendation,
                h.cluster_count_at_recommendation,
                h.clean_signal_tier_at_recommendation,
                h.conviction_tier_at_recommendation,
                h.liquidity_trend_points_at_recommendation,
                h.liquidity_level_points_at_recommendation,
                h.price_window_points_at_recommendation,
                h.volume_sanity_points_at_recommendation,
                h.sellable_check_result,
                h.recommended_at,
                h.max_multiplier_since_recommendation,
                h.pumped_since_recommendation_alerted
            FROM wallet_token_history h
            JOIN token_scan_log s
                ON s.wallet = h.wallet AND s.token_mint = h.token_mint
                AND s.momentum_alert_fired = TRUE
            WHERE h.momentum_alerted = TRUE
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
        put_conn(conn)
        conn = None

        if not rows:
            return {"rows": [], "count": 0}

        held_map = get_held_map(hours)

        columns = [
            "wallet", "token_mint", "score", "market_cap", "rugcheck_score",
            "top1_holder_pct", "historical_peak_ratio",
            "buy_trajectory", "buy_count", "cluster_count", "clean_signal_tier",
            "conviction_tier", "liquidity_trend_points", "liquidity_level_points",
            "price_window_points", "volume_sanity_points", "sellable_check_result",
            "recommended_at", "max_multiplier_since_recommendation",
            "pumped_since_recommendation_alerted"
        ]

        dataset = []
        for row in rows:
            wallet, mint = row[0], row[1]
            record = dict(zip(columns, row))

            touched_3x = bool(record["pumped_since_recommendation_alerted"]) or (
                record["max_multiplier_since_recommendation"] is not None
                and float(record["max_multiplier_since_recommendation"]) >= 3
            )
            held = held_map.get((wallet, mint))

            record["touched_3x"] = touched_3x
            record["held_50pct"] = held if held is not None else None

            for k, v in record.items():
                if hasattr(v, "isoformat"):
                    record[k] = v.isoformat()
                elif hasattr(v, "__float__") and not isinstance(v, (bool, int)):
                    try:
                        record[k] = float(v)
                    except (TypeError, ValueError):
                        pass

            dataset.append(record)

        return {"rows": dataset, "count": len(dataset)}

    except Exception as e:
        return {"error": str(e)}, 500

    finally:
        if conn:
            put_conn(conn)


@app.route("/recommendation/<mint>")
def recommendation_lookup(mint):
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute("""
            SELECT wallet, price_at_recommendation, recommended_at,
                   max_multiplier_since_recommendation,
                   pumped_since_recommendation_alerted,
                   market_cap_at_recommendation,
                   rugcheck_score_at_recommendation,
                   top1_holder_pct_at_recommendation,
                   buy_count_at_recommendation,
                   clean_signal_tier_at_recommendation,
                   conviction_tier_at_recommendation,
                   sellable_check_result,
                   liquidity_trend_points_at_recommendation,
                   liquidity_level_points_at_recommendation,
                   price_window_points_at_recommendation,
                   volume_sanity_points_at_recommendation,
                   historical_peak_ratio_at_recommendation
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
         paid_off, market_cap_at_rec, rug_score, top1_pct,
         buy_count, clean_tier, conv_tier, sellable_result,
         liq_trend_pts, liq_level_pts, price_window_pts, vol_sanity_pts,
         historical_peak) = row

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
            f"RugCheck score: {rug_score if rug_score is not None else 'n/a'}",
            f"Top holder %: {top1_pct if top1_pct is not None else 'n/a'}",
            f"Buy count: {buy_count if buy_count is not None else 'n/a'}",
            f"Clean signal tier: {clean_tier or 'n/a'}",
            f"Conviction tier: {conv_tier or 'n/a'}",
            f"Sellable check (Jupiter): {sellable_result or 'n/a'}",
            f"Historical peak vol/liq ratio: {f'{historical_peak:.2f}x' if historical_peak is not None else 'n/a'}",
            "",
            f"<b>Score component breakdown:</b>",
            f"Liquidity trend points (max 45): {liq_trend_pts if liq_trend_pts is not None else 'n/a'}",
            f"Liquidity level points (max 25): {liq_level_pts if liq_level_pts is not None else 'n/a'}",
            f"Price window points (max 20): {price_window_pts if price_window_pts is not None else 'n/a'}",
            f"Volume sanity points (-10 to +10): {vol_sanity_pts if vol_sanity_pts is not None else 'n/a'}",
            "",
            f"<b>ALL-TIME HIGH since recommendation: {f'{max_mult:.2f}x' if max_mult else 'n/a'}</b>",
            f"Current multiplier: {f'{current_mult:.2f}x' if current_mult else 'n/a'} (price now: ${current_price if current_price else 'n/a'})",
            f"3x confirmation fired: {'Yes' if paid_off else 'No'}",
        ]
        return "<br>".join(lines), 200

    except Exception as e:
        return f"recommendation_lookup error: {e}", 500

    finally:
        put_conn(conn)


@app.route("/debug-token/<mint>")
def debug_token(mint):
    conn = get_conn()
    try:
        c = conn.cursor()
        c.execute("""
            SELECT wallet, token_mint, first_seen_at, buy_count,
                   price_at_first_buy, pumped_3x_alerted, momentum_alerted,
                   pumped_since_recommendation_alerted, last_checked_at,
                   block_reason_at_last_attempt
            FROM wallet_token_history
            WHERE token_mint = %s
        """, (mint,))
        rows = c.fetchall()
        c.close()

        if not rows:
            return f"No row found in wallet_token_history for {mint} — the INSERT never actually created a row, despite any 'FIRST BUY DETECTED' log line.", 200

        lines = [f"<b>Raw wallet_token_history row(s) for {mint}:</b><br>"]
        for wallet, token_mint, first_seen_at, buy_count, price_at_first_buy, pumped_3x, momentum_alerted, pumped_since_rec, last_checked, block_reason in rows:
            lines.append(
                f"<br>Wallet: <code>{wallet}</code><br>"
                f"first_seen_at: {first_seen_at}<br>"
                f"buy_count: {buy_count}<br>"
                f"price_at_first_buy: {price_at_first_buy}<br>"
                f"pumped_3x_alerted: {pumped_3x}<br>"
                f"momentum_alerted: {momentum_alerted}<br>"
                f"pumped_since_recommendation_alerted: {pumped_since_rec}<br>"
                f"last_checked_at: {last_checked}<br>"
                f"block_reason_at_last_attempt: {block_reason or 'none recorded'}<br>"
            )

        return "<br>".join(lines), 200

    except Exception as e:
        return f"debug_token error: {e}", 500

    finally:
        put_conn(conn)


@app.route("/debug-token-buyers/<mint>")
def debug_token_buyers(mint):
    """
    Pulls historical transaction data for a specific mint from Helius
    and returns buyer wallets within a given time window. Built for
    researching who bought a specific token before it graduated to a
    DEX (so DexScreener/Solscan-based lookups don't apply), using exact
    UTC timestamps rather than a UI time filter that could be ambiguous
    about timezone or edge cases.

    Params:
      start: ISO 8601 datetime (UTC), e.g. 2026-08-09T22:58:00
      end: ISO 8601 datetime (UTC), e.g. 2026-08-09T23:08:00
    """
    if not HELIUS_API_KEY:
        return "HELIUS_API_KEY not configured.", 500

    start_str = request.args.get("start")
    end_str = request.args.get("end")
    if not start_str or not end_str:
        return "Provide ?start=...&end=... as ISO 8601 UTC datetimes (e.g. 2026-08-09T22:58:00)", 400

    try:
        start_dt = datetime.datetime.fromisoformat(start_str).replace(tzinfo=datetime.timezone.utc)
        end_dt = datetime.datetime.fromisoformat(end_str).replace(tzinfo=datetime.timezone.utc)
    except ValueError:
        return "Invalid start/end format — use ISO 8601, e.g. 2026-08-09T22:58:00", 400

    start_ts = int(start_dt.timestamp())
    end_ts = int(end_dt.timestamp())

    buyers = []
    before_sig = None
    pages_fetched = 0
    max_pages = 20
    oldest_ts_this_page = None

    try:
        while pages_fetched < max_pages:
            url = f"https://api.helius.xyz/v0/addresses/{mint}/transactions"
            params = {"api-key": HELIUS_API_KEY, "limit": 100}
            if before_sig:
                params["before"] = before_sig

            resp = requests.get(url, params=params, timeout=10)
            if resp.status_code != 200:
                return f"Helius API error: HTTP {resp.status_code} — {resp.text[:300]}", 500

            txs = resp.json()
            if not txs:
                break

            pages_fetched += 1
            oldest_ts_this_page = None

            for tx in txs:
                tx_ts = tx.get("timestamp")
                if tx_ts is None:
                    continue
                if oldest_ts_this_page is None or tx_ts < oldest_ts_this_page:
                    oldest_ts_this_page = tx_ts

                if start_ts <= tx_ts <= end_ts:
                    for transfer in tx.get("tokenTransfers", []) or []:
                        if transfer.get("mint") != mint:
                            continue
                        to_account = transfer.get("toUserAccount")
                        amount = transfer.get("tokenAmount")
                        if to_account:
                            buyers.append({
                                "wallet": to_account,
                                "amount": amount,
                                "timestamp": tx_ts,
                                "signature": tx.get("signature"),
                            })

            before_sig = txs[-1].get("signature")

            if oldest_ts_this_page is not None and oldest_ts_this_page < start_ts:
                break

        if not buyers:
            oldest_reached = (
                datetime.datetime.utcfromtimestamp(oldest_ts_this_page).isoformat()
                if oldest_ts_this_page is not None else "unknown"
            )
            return (
                f"No token transfers found for {mint} between {start_str} and {end_str} "
                f"UTC (checked {pages_fetched} pages of Helius history, reaching back to "
                f"{oldest_reached} UTC). If that date is more recent than your requested "
                f"start time, the {max_pages}-page cap was hit before reaching your window "
                f"— try increasing max_pages, or the token had less activity than expected "
                f"between now and your target window.",
                200
            )

        lines = [f"<b>Buyers of {mint}</b> between {start_str} and {end_str} UTC:<br>"]
        for b in buyers:
            lines.append(
                f"<br>Wallet: <code>{b['wallet']}</code><br>"
                f"Amount: {b['amount']}<br>"
                f"Time: {datetime.datetime.utcfromtimestamp(b['timestamp']).isoformat()} UTC<br>"
                f"Tx: <code>{b['signature']}</code>"
            )

        return "<br>".join(lines), 200

    except Exception as e:
        return f"debug_token_buyers error: {e}", 500


@app.route("/token/<mint>")
def token_history(mint):
    limit_param = request.args.get("limit", "30")
    try:
        limit = int(limit_param)
    except (TypeError, ValueError):
        limit = 30

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
            ORDER BY scanned_at DESC
            LIMIT %s
        """, (mint, limit))
        rows = list(reversed(c.fetchall()))
        c.close()

        if not rows:
            return f"No scan history found for {mint}", 200

        lines = [f"<b>Scan history for {mint}</b> (showing last {len(rows)} scans)<br>"]
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
                f"Liquidity: {f'${liquidity:,.0f}' if liquidity is not None else 'n/a'} "
                f"(Δ {f'{liq_delta*100:.1f}%' if liq_delta is not None else 'n/a'})<br>"
                f"Volume 5m/1h: {f'${vol_5m:,.0f}' if vol_5m is not None else 'n/a'} / "
                f"{f'${vol_h1:,.0f}' if vol_h1 is not None else 'n/a'}<br>"
                f"Price change 5m/1h/6h: {pc_5m}% / {pc_h1}% / {pc_h6}%<br>"
                f"Buys/Sells (5m): {buys_5m}/{sells_5m}<br>"
                f"Momentum score: {score}"
            )

        return "<br>".join(lines), 200

    except Exception as e:
        return f"token_history error: {e}", 500

    finally:
        put_conn(conn)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
