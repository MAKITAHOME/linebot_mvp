# app.py (FULL REWRITE - STABLE & EVOLVED)
# - No f-strings for HTML (prevents SyntaxError from leaked JS)
# - No JS template literals (no `...` / ${...})
# - Analyze: OpenAI Responses JSON Schema -> temp/intent/next_goal (DB truth)
# - Followup: LLM + fallback, A/B variant
# - Timing learning: pref_hour_jst
# - perma_cold + silence_score + /jobs/auto_cold
# - Manual WIN (C) + KPI + Executive dashboard
#
# Required env:
#   LINE_CHANNEL_SECRET, LINE_CHANNEL_ACCESS_TOKEN, OPENAI_API_KEY, DATABASE_URL, DASHBOARD_KEY, ADMIN_API_KEY
#
# Optional env:
#   SHOP_ID=tokyo_01
#   OPENAI_MODEL_ASSISTANT/ANALYZE/FOLLOWUP/COLD (default gpt-4o-mini)
#   ANALYZE_TIMEOUT_SEC=8.5, FOLLOWUP_LLM_TIMEOUT_SEC=8.5, AUTO_COLD_LLM_TIMEOUT_SEC=8.5
#   FOLLOWUP_ENABLED=1, FOLLOWUP_AFTER_MINUTES=180, FOLLOWUP_MIN_LEVEL=8, FOLLOWUP_LIMIT=50
#   FOLLOWUP_JST_FROM=10, FOLLOWUP_JST_TO=20
#   FOLLOWUP_TIME_MATCH_HOURS=1, FOLLOWUP_FORCE_SEND_AFTER_HOURS=12
#   PREF_HOUR_LOOKBACK_DAYS=60, PREF_HOUR_MIN_SAMPLES=3
#   AUTO_COLD_USE_LLM=1, AUTO_COLD_LIMIT=80
#   DASHBOARD_REFRESH_SEC_DEFAULT=30

import os
import json
import hmac
import hashlib
import base64
import ssl
import secrets
import asyncio
import re
from datetime import datetime, timedelta, timezone
from collections import deque, defaultdict
from typing import Any, Dict, List, Optional, Tuple

import certifi
import httpx
import pg8000

from fastapi import FastAPI, Request, Header, HTTPException, Query, Depends, BackgroundTasks
from fastapi.responses import JSONResponse, HTMLResponse


# ============================================================
# Config
# ============================================================

LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "")
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
SHOP_ID = os.getenv("SHOP_ID", "tokyo_01")

DASHBOARD_KEY = os.getenv("DASHBOARD_KEY", "").strip()
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "").strip()

DASHBOARD_REFRESH_SEC_DEFAULT = int(os.getenv("DASHBOARD_REFRESH_SEC_DEFAULT", "30"))

FAST_REPLY_TIMEOUT_SEC = float(os.getenv("FAST_REPLY_TIMEOUT_SEC", "3.0"))
ANALYZE_HISTORY_LIMIT = int(os.getenv("ANALYZE_HISTORY_LIMIT", "10"))
SHORT_TEXT_MAX_LEN = int(os.getenv("SHORT_TEXT_MAX_LEN", "2"))

FOLLOWUP_ENABLED = os.getenv("FOLLOWUP_ENABLED", "0").strip() == "1"
FOLLOWUP_AFTER_MINUTES = int(os.getenv("FOLLOWUP_AFTER_MINUTES", "180"))
FOLLOWUP_MIN_LEVEL = int(os.getenv("FOLLOWUP_MIN_LEVEL", "8"))
FOLLOWUP_LIMIT = int(os.getenv("FOLLOWUP_LIMIT", "50"))
FOLLOWUP_LOCK_TTL_SEC = int(os.getenv("FOLLOWUP_LOCK_TTL_SEC", "180"))

FOLLOWUP_JST_FROM = int(os.getenv("FOLLOWUP_JST_FROM", "10"))
FOLLOWUP_JST_TO = int(os.getenv("FOLLOWUP_JST_TO", "20"))

FOLLOWUP_TIME_MATCH_HOURS = int(os.getenv("FOLLOWUP_TIME_MATCH_HOURS", "1"))
FOLLOWUP_FORCE_SEND_AFTER_HOURS = int(os.getenv("FOLLOWUP_FORCE_SEND_AFTER_HOURS", "12"))
PREF_HOUR_LOOKBACK_DAYS = int(os.getenv("PREF_HOUR_LOOKBACK_DAYS", "60"))
PREF_HOUR_MIN_SAMPLES = int(os.getenv("PREF_HOUR_MIN_SAMPLES", "3"))

FOLLOWUP_AB_ENABLED = os.getenv("FOLLOWUP_AB_ENABLED", "1").strip() == "1"
FOLLOWUP_SECOND_TOUCH_AFTER_HOURS = int(os.getenv("FOLLOWUP_SECOND_TOUCH_AFTER_HOURS", "48"))
FOLLOWUP_SECOND_TOUCH_LIMIT = int(os.getenv("FOLLOWUP_SECOND_TOUCH_LIMIT", "50"))
FOLLOWUP_ATTRIBUTION_WINDOW_HOURS = int(os.getenv("FOLLOWUP_ATTRIBUTION_WINDOW_HOURS", "72"))

VISIT_DAYS_AHEAD = int(os.getenv("VISIT_DAYS_AHEAD", "3"))
VISIT_SLOT_HOURS = os.getenv("VISIT_SLOT_HOURS", "11,14,17").strip()

FOLLOWUP_USE_LLM = os.getenv("FOLLOWUP_USE_LLM", "1").strip() != "0"
AUTO_COLD_USE_LLM = os.getenv("AUTO_COLD_USE_LLM", "1").strip() != "0"
AUTO_COLD_LIMIT = int(os.getenv("AUTO_COLD_LIMIT", "80"))

OPENAI_MODEL_ASSISTANT = os.getenv("OPENAI_MODEL_ASSISTANT", "gpt-4o-mini").strip()
OPENAI_MODEL_ANALYZE = os.getenv("OPENAI_MODEL_ANALYZE", "gpt-4o-mini").strip()
OPENAI_MODEL_FOLLOWUP = os.getenv("OPENAI_MODEL_FOLLOWUP", "gpt-4o-mini").strip()
OPENAI_MODEL_COLD = os.getenv("OPENAI_MODEL_COLD", "gpt-4o-mini").strip()

ANALYZE_TIMEOUT_SEC = float(os.getenv("ANALYZE_TIMEOUT_SEC", "8.5"))
FOLLOWUP_LLM_TIMEOUT_SEC = float(os.getenv("FOLLOWUP_LLM_TIMEOUT_SEC", "8.5"))
AUTO_COLD_LLM_TIMEOUT_SEC = float(os.getenv("AUTO_COLD_LLM_TIMEOUT_SEC", "8.5"))

RESPONSES_API_URL = "https://api.openai.com/v1/responses"
OPENAI_CHAT_URL = "https://api.openai.com/v1/chat/completions"

JST = timezone(timedelta(hours=9))

CHAT_HISTORY: Dict[str, deque] = defaultdict(lambda: deque(maxlen=40))
TEMP_HISTORY: Dict[str, deque] = defaultdict(lambda: deque(maxlen=5))

app = FastAPI(title="linebot_mvp", version="3.0.0")


# ============================================================
# Patterns
# ============================================================

CANCEL_PATTERNS = [
    r"やっぱ(り)?(なし|やめ|辞め|やめます)",
    r"(今回は|今は).*(いい|結構|不要)",
    r"不要です|いりません|連絡(不要|いらない)",
    r"興味(ない|ありません)",
    r"他(で|の).*(決(めた|まりました)|決まりました)|決まりました",
    r"キャンセル|取り消し|中止",
    r"また今度|またの機会",
    r"検討(やめます|しません)|やめときます",
]

OPTOUT_PATTERNS = [
    r"連絡(不要|いらない)|もう連絡(しないで|いりません)",
    r"配信停止|停止して|ブロックする",
    r"\bstop\b|\bunsubscribe\b",
]

VISIT_CHANGE_PATTERNS = [
    r"別日|他の日|別の?日|別時間|他の時間|時間変えて|日程変えて|調整したい",
]

LOST_REVIVE_PATTERNS = [
    r"やっぱ(り)?(探す|探したい|探します)",
    r"再開|もう一回|もう一度|改めて",
    r"戻ってきた|復活",
    r"やっぱ(り)?お願い|お願いします",
    r"再度(お願いします|探したい)",
]


# ============================================================
# Auth
# ============================================================

def require_dashboard_key(
    x_dashboard_key: Optional[str] = Header(default=None, alias="X-Dashboard-Key"),
    key: Optional[str] = Query(default=None),
) -> None:
    expected = (DASHBOARD_KEY or "").strip()
    provided = (x_dashboard_key or key or "").strip()
    if not expected:
        raise HTTPException(status_code=500, detail="DASHBOARD_KEY not set")
    if not provided or not secrets.compare_digest(expected, provided):
        raise HTTPException(status_code=401, detail="Unauthorized")


def require_admin_key(x_admin_key: Optional[str] = Header(default=None, alias="x-admin-key")) -> None:
    if not ADMIN_API_KEY:
        raise HTTPException(status_code=500, detail="ADMIN_API_KEY not set")
    if not x_admin_key or not secrets.compare_digest(ADMIN_API_KEY, x_admin_key.strip()):
        raise HTTPException(status_code=401, detail="Unauthorized")


# ============================================================
# Time helpers
# ============================================================

def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def now_jst() -> datetime:
    return datetime.now(JST)


def is_within_jst_window(dt: Optional[datetime] = None) -> bool:
    d = dt or now_jst()
    h = d.hour
    start = max(0, min(23, int(FOLLOWUP_JST_FROM)))
    end = max(0, min(23, int(FOLLOWUP_JST_TO)))
    if start < end:
        return start <= h < end
    if start > end:
        return (h >= start) or (h < end)
    return False


def within_hour_band(now_hour: int, target_hour: int, band: int) -> bool:
    diff = abs(now_hour - target_hour)
    diff = min(diff, 24 - diff)
    return diff <= max(0, band)


# ============================================================
# DB helpers
# ============================================================

def parse_database_url(url: str) -> Dict[str, Any]:
    if not url:
        raise ValueError("DATABASE_URL is empty")

    if url.startswith("postgres://"):
        rest = url[len("postgres://"):]
    elif url.startswith("postgresql://"):
        rest = url[len("postgresql://"):]
    else:
        raise ValueError("DATABASE_URL must start with postgres:// or postgresql://")

    params: Dict[str, str] = {}
    if "?" in rest:
        rest, query = rest.split("?", 1)
        for kv in query.split("&"):
            if "=" in kv:
                k, v = kv.split("=", 1)
                params[k] = v

    creds, host_db = rest.split("@", 1)
    user, password = creds.split(":", 1)
    host_port, database = host_db.split("/", 1)

    if ":" in host_port:
        host, port_s = host_port.split(":", 1)
        port = int(port_s)
    else:
        host = host_port
        port = 5432

    return {"user": user, "password": password, "host": host, "port": port, "database": database, "params": params}


def create_db_ssl_context(verify: bool = True) -> ssl.SSLContext:
    ctx = ssl.create_default_context(cafile=certifi.where())
    if not verify:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    return ctx


def connect_db(verify_ssl: bool = True):
    cfg = parse_database_url(DATABASE_URL)
    ssl_context = create_db_ssl_context(verify=verify_ssl)
    conn = pg8000.connect(
        user=cfg["user"],
        password=cfg["password"],
        host=cfg["host"],
        port=cfg["port"],
        database=cfg["database"],
        ssl_context=ssl_context,
        timeout=10,
    )
    conn.autocommit = True
    return conn


def _connect_db_with_fallback():
    try:
        return connect_db(verify_ssl=True)
    except ssl.SSLError as e:
        print("[DB] SSL verify failed, fallback disable:", repr(e))
        return connect_db(verify_ssl=False)


def db_execute(sql: str, args: Tuple[Any, ...] = ()) -> None:
    conn = _connect_db_with_fallback()
    cur = conn.cursor()
    cur.execute(sql, args)
    cur.close()
    conn.close()


def db_fetchall(sql: str, args: Tuple[Any, ...] = ()) -> List[Tuple[Any, ...]]:
    conn = _connect_db_with_fallback()
    cur = conn.cursor()
    cur.execute(sql, args)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


# ============================================================
# DB schema
# ============================================================

def ensure_tables_and_columns() -> None:
    if not DATABASE_URL:
        return

    conn = _connect_db_with_fallback()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS customers (
          id BIGSERIAL PRIMARY KEY,
          shop_id TEXT NOT NULL,
          conv_key TEXT NOT NULL,
          updated_at TIMESTAMPTZ DEFAULT now(),
          UNIQUE (shop_id, conv_key)
        );
        """
    )

    # core
    cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS user_id TEXT;")
    cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS last_user_text TEXT;")
    cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS temp_level_raw INT;")
    cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS temp_level_stable INT;")
    cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS confidence REAL;")
    cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS intent TEXT;")
    cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS next_goal TEXT;")

    # status
    cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS status TEXT;")
    cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS opt_out BOOLEAN DEFAULT FALSE;")
    cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS opt_out_at TIMESTAMPTZ;")

    # slots
    cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS visit_slot_selected TEXT;")
    cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS visit_slot_selected_at TIMESTAMPTZ;")
    cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS slot_budget TEXT;")
    cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS slot_area TEXT;")
    cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS slot_move_in TEXT;")
    cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS slot_layout TEXT;")
    cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS slots_json TEXT;")

    # need_reply
    cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS need_reply BOOLEAN DEFAULT FALSE;")
    cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS need_reply_reason TEXT;")
    cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS need_reply_updated_at TIMESTAMPTZ;")

    # timing
    cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS pref_hour_jst INT;")
    cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS pref_hour_samples INT;")
    cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS pref_hour_updated_at TIMESTAMPTZ;")

    # perma cold
    cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS silence_score INT DEFAULT 0;")
    cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS perma_cold BOOLEAN DEFAULT FALSE;")

    # manual win
    cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS won BOOLEAN DEFAULT FALSE;")
    cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS won_at TIMESTAMPTZ;")
    cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS won_by TEXT;")

    # messages
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS messages (
          id BIGSERIAL PRIMARY KEY,
          shop_id TEXT NOT NULL,
          conv_key TEXT NOT NULL,
          role TEXT NOT NULL,
          content TEXT NOT NULL,
          created_at TIMESTAMPTZ DEFAULT now()
        );
        """
    )
    cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS temp_level_raw INT;")
    cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS temp_level_stable INT;")
    cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS confidence REAL;")
    cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS intent TEXT;")
    cur.execute("ALTER TABLE messages ADD COLUMN IF NOT EXISTS next_goal TEXT;")

    # job locks
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS job_locks (
          key TEXT PRIMARY KEY,
          locked_until TIMESTAMPTZ NOT NULL
        );
        """
    )

    # followups
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS followup_logs (
          id BIGSERIAL PRIMARY KEY,
          shop_id TEXT NOT NULL,
          conv_key TEXT NOT NULL,
          user_id TEXT NOT NULL,
          message TEXT NOT NULL,
          mode TEXT NOT NULL,
          status TEXT NOT NULL,
          error TEXT,
          created_at TIMESTAMPTZ DEFAULT now()
        );
        """
    )
    cur.execute("ALTER TABLE followup_logs ADD COLUMN IF NOT EXISTS variant TEXT;")
    cur.execute("ALTER TABLE followup_logs ADD COLUMN IF NOT EXISTS responded_at TIMESTAMPTZ;")
    cur.execute("ALTER TABLE followup_logs ADD COLUMN IF NOT EXISTS stage INT;")
    cur.execute("ALTER TABLE followup_logs ADD COLUMN IF NOT EXISTS send_hour_jst INT;")

    # indexes
    cur.execute("CREATE INDEX IF NOT EXISTS idx_customers_shop_updated ON customers(shop_id, updated_at DESC);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_customers_need_reply ON customers(shop_id, need_reply, updated_at DESC);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_shop_conv_role_created ON messages(shop_id, conv_key, role, created_at DESC);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_followup_shop_conv_created ON followup_logs(shop_id, conv_key, created_at DESC);")

    cur.execute("UPDATE customers SET need_reply=FALSE WHERE need_reply IS NULL;")

    cur.close()
    conn.close()


@app.on_event("startup")
async def on_startup():
    ensure_tables_and_columns()
    print("[BOOT] tables/columns ensured")


# ============================================================
# Core state helpers
# ============================================================

def stable_from_history(conv_key: str, raw_level: int) -> int:
    hist = TEMP_HISTORY[conv_key]
    hist.append(raw_level)
    s = sorted(hist)
    return s[len(s) // 2]


def ensure_customer_row(shop_id: str, conv_key: str, user_id: str) -> None:
    db_execute(
        """
        INSERT INTO customers (shop_id, conv_key, user_id, updated_at, status, need_reply, need_reply_reason, need_reply_updated_at)
        VALUES (%s,%s,%s,now(),'ACTIVE',FALSE,'',now())
        ON CONFLICT (shop_id, conv_key)
        DO UPDATE SET user_id=EXCLUDED.user_id, updated_at=now()
        """,
        (shop_id, conv_key, user_id),
    )


def is_inactive(shop_id: str, conv_key: str) -> bool:
    rows = db_fetchall(
        """
        SELECT COALESCE(opt_out,FALSE), COALESCE(perma_cold,FALSE), COALESCE(won,FALSE), COALESCE(status,'ACTIVE')
        FROM customers
        WHERE shop_id=%s AND conv_key=%s
        """,
        (shop_id, conv_key),
    )
    if not rows:
        return False
    opt, cold, won, st = rows[0]
    st = (st or "ACTIVE").upper()
    return bool(opt or cold or won or st in ("OPTOUT", "LOST", "WON"))


def upsert_customer_state(
    shop_id: str,
    conv_key: str,
    user_id: str,
    last_user_text: str,
    raw_level: int,
    stable_level: int,
    confidence: float,
    intent: str,
    next_goal: str,
) -> None:
    db_execute(
        """
        UPDATE customers
        SET user_id=%s,
            last_user_text=%s,
            temp_level_raw=%s,
            temp_level_stable=%s,
            confidence=%s,
            intent=%s,
            next_goal=%s,
            updated_at=now()
        WHERE shop_id=%s AND conv_key=%s
        """,
        (user_id, last_user_text, raw_level, stable_level, confidence, intent, next_goal, shop_id, conv_key),
    )


def save_message(
    shop_id: str,
    conv_key: str,
    role: str,
    content: str,
    temp_level_raw: Optional[int] = None,
    temp_level_stable: Optional[int] = None,
    confidence: Optional[float] = None,
    intent: Optional[str] = None,
    next_goal: Optional[str] = None,
) -> None:
    db_execute(
        """
        INSERT INTO messages (shop_id, conv_key, role, content, temp_level_raw, temp_level_stable, confidence, intent, next_goal)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (shop_id, conv_key, role, content, temp_level_raw, temp_level_stable, confidence, intent, next_goal),
    )


def get_recent_conversation(shop_id: str, conv_key: str, limit: int) -> List[Dict[str, str]]:
    rows = db_fetchall(
        """
        SELECT role, content
        FROM messages
        WHERE shop_id=%s AND conv_key=%s AND role IN ('user','assistant')
        ORDER BY created_at DESC
        LIMIT %s
        """,
        (shop_id, conv_key, max(2, min(30, int(limit)))),
    )
    rows = list(reversed(rows))
    return [{"role": r[0], "content": (r[1] or "")[:1200]} for r in rows]


def get_recent_conversation_for_followup(shop_id: str, conv_key: str, limit: int = 12) -> List[Dict[str, str]]:
    return get_recent_conversation(shop_id, conv_key, limit=max(6, min(20, int(limit))))


# ============================================================
# Manual WIN (C)
# ============================================================

def mark_won(shop_id: str, conv_key: str, by: str = "admin") -> None:
    db_execute(
        """
        UPDATE customers
        SET won=TRUE, won_at=now(), won_by=%s,
            status='WON',
            perma_cold=TRUE,
            need_reply=FALSE,
            updated_at=now()
        WHERE shop_id=%s AND conv_key=%s
        """,
        (by, shop_id, conv_key),
    )


def unmark_won(shop_id: str, conv_key: str) -> None:
    db_execute(
        """
        UPDATE customers
        SET won=FALSE, won_at=NULL, won_by=NULL,
            status='ACTIVE',
            perma_cold=FALSE,
            updated_at=now()
        WHERE shop_id=%s AND conv_key=%s
        """,
        (shop_id, conv_key),
    )


# ============================================================
# Slots + visit
# ============================================================

def extract_slots(text: str) -> Dict[str, str]:
    t = text or ""
    slots: Dict[str, str] = {}

    m = re.search(r"(\d{1,3})(?:\.(\d))?\s*(?:万円|万)", t)
    if m:
        slots["budget"] = m.group(0)

    m = re.search(r"(ワンルーム|1R|\d\s*(?:LDK|DK|K))", t, re.IGNORECASE)
    if m:
        slots["layout"] = m.group(1)

    for kw in ["今月", "来月", "再来月", "すぐ", "早め", "急ぎ", "春", "夏", "秋", "冬"]:
        if kw in t:
            slots["move_in"] = kw
            break
    m = re.search(r"(\d{1,2})\s*月", t)
    if m:
        slots.setdefault("move_in", m.group(0))

    for kw in ["渋谷", "新宿", "品川", "池袋", "目黒", "中目黒", "恵比寿", "吉祥寺", "横浜", "川崎", "浦和"]:
        if kw in t:
            slots["area"] = kw
            break

    return slots


def merge_slots(prev: Dict[str, str], new: Dict[str, str]) -> Dict[str, str]:
    out = dict(prev or {})
    for k, v in (new or {}).items():
        if v and (k not in out or not out[k]):
            out[k] = v
    return out


def get_customer_slots(shop_id: str, conv_key: str) -> Dict[str, str]:
    rows = db_fetchall("SELECT slots_json FROM customers WHERE shop_id=%s AND conv_key=%s", (shop_id, conv_key))
    if not rows or not rows[0][0]:
        return {}
    try:
        return json.loads(rows[0][0])
    except Exception:
        return {}


def set_customer_slots(shop_id: str, conv_key: str, slots: Dict[str, str]) -> None:
    sj = json.dumps(slots, ensure_ascii=False)
    db_execute(
        """
        UPDATE customers
        SET slot_budget=%s, slot_area=%s, slot_move_in=%s, slot_layout=%s, slots_json=%s, updated_at=now()
        WHERE shop_id=%s AND conv_key=%s
        """,
        (slots.get("budget"), slots.get("area"), slots.get("move_in"), slots.get("layout"), sj, shop_id, conv_key),
    )


def set_visit_slot(shop_id: str, conv_key: str, slot_text: str) -> None:
    db_execute(
        "UPDATE customers SET visit_slot_selected=%s, visit_slot_selected_at=now(), updated_at=now() WHERE shop_id=%s AND conv_key=%s",
        (slot_text, shop_id, conv_key),
    )


def parse_slot_hours() -> List[int]:
    out: List[int] = []
    for part in (VISIT_SLOT_HOURS or "").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            h = int(part)
            if 0 <= h <= 23:
                out.append(h)
        except Exception:
            pass
    return out or [11, 14, 17]


def upcoming_visit_slots_jst(days_ahead: int = 3) -> List[str]:
    hours = parse_slot_hours()
    slots: List[str] = []
    for d in range(1, max(1, min(14, days_ahead)) + 1):
        label = "明日" if d == 1 else ("明後日" if d == 2 else f"{d}日後")
        for h in hours:
            slots.append(f"{label} {h:02d}:00-{(h+1)%24:02d}:00")
    return slots[:6]


def parse_slot_selection(text: str) -> Optional[int]:
    t = (text or "").strip()
    circ_map = {"①": 1, "②": 2, "③": 3, "④": 4, "⑤": 5, "⑥": 6}
    if t in circ_map:
        return circ_map[t]
    m = re.match(r"^\s*([1-6])\s*$", t)
    if m:
        return int(m.group(1))
    m2 = re.search(r"([1-6])\s*(?:番|で|がいい|希望|お願いします)?", t)
    if m2:
        return int(m2.group(1))
    return None


def is_visit_change_request(text: str) -> bool:
    return any(re.search(p, text or "") for p in VISIT_CHANGE_PATTERNS)


# ============================================================
# need_reply
# ============================================================

def set_need_reply(shop_id: str, conv_key: str, need: bool, reason: str = "") -> None:
    db_execute(
        """
        UPDATE customers
        SET need_reply=%s, need_reply_reason=%s, need_reply_updated_at=now(), updated_at=now()
        WHERE shop_id=%s AND conv_key=%s
        """,
        (bool(need), (reason or "")[:120], shop_id, conv_key),
    )


def get_customer_flags(shop_id: str, conv_key: str) -> Dict[str, Any]:
    rows = db_fetchall(
        """
        SELECT visit_slot_selected, slot_budget, slot_area, slot_move_in, slot_layout,
               COALESCE(status,'ACTIVE'),
               COALESCE(opt_out,FALSE),
               COALESCE(won,FALSE),
               COALESCE(perma_cold,FALSE)
        FROM customers
        WHERE shop_id=%s AND conv_key=%s
        """,
        (shop_id, conv_key),
    )
    if not rows:
        return {}
    vsel, b, a, m, l, st, opt, won, cold = rows[0]
    return {
        "visit_slot_selected": vsel,
        "slot_budget": b,
        "slot_area": a,
        "slot_move_in": m,
        "slot_layout": l,
        "status": (st or "ACTIVE"),
        "opt_out": bool(opt),
        "won": bool(won),
        "perma_cold": bool(cold),
    }


def compute_need_reply(next_goal: str, flags: Dict[str, Any], assistant_text: str = "") -> Tuple[bool, str]:
    goal = (next_goal or "").strip()
    st = (flags.get("status") or "ACTIVE").upper()
    if flags.get("opt_out") or flags.get("won") or flags.get("perma_cold") or st in ("OPTOUT", "LOST", "WON"):
        return False, "inactive"

    visit = flags.get("visit_slot_selected")
    budget = flags.get("slot_budget")
    area = flags.get("slot_area")
    move_in = flags.get("slot_move_in")
    layout = flags.get("slot_layout")

    if any(k in goal for k in ["内見", "候補日", "日程"]):
        if not visit or visit == "REQUEST_CHANGE":
            return True, "need_visit_slot"
        return False, "visit_ok"

    if "予算" in goal and not budget:
        return True, "need_budget"
    if ("エリア" in goal or "沿線" in goal) and not area:
        return True, "need_area"
    if ("入居" in goal or "時期" in goal) and not move_in:
        return True, "need_move_in"
    if "間取り" in goal and not layout:
        return True, "need_layout"

    if "？" in (assistant_text or "") or "?" in (assistant_text or ""):
        return True, "assistant_question"

    return False, "no_need"


# ============================================================
# Preferred send hour learning
# ============================================================

def _to_jst(dt: datetime) -> datetime:
    try:
        return dt.astimezone(JST)
    except Exception:
        return dt


def learn_pref_hour_from_messages(shop_id: str, conv_key: str) -> Tuple[Optional[int], int]:
    since = utcnow() - timedelta(days=max(7, min(365, PREF_HOUR_LOOKBACK_DAYS)))
    rows = db_fetchall(
        """
        SELECT created_at
        FROM messages
        WHERE shop_id=%s AND conv_key=%s AND role='user' AND created_at >= %s
        ORDER BY created_at DESC
        LIMIT 80
        """,
        (shop_id, conv_key, since),
    )
    if not rows:
        return None, 0

    hours: List[int] = []
    for (dt,) in rows:
        if not dt:
            continue
        hours.append(int(_to_jst(dt).hour))

    samples = len(hours)
    if samples < PREF_HOUR_MIN_SAMPLES:
        return None, samples

    counts = [0] * 24
    for h in hours:
        if 0 <= h <= 23:
            counts[h] += 1

    best_h = max(range(24), key=lambda x: counts[x])
    return best_h, samples


def update_customer_pref_hour(shop_id: str, conv_key: str) -> None:
    pref, samples = learn_pref_hour_from_messages(shop_id, conv_key)
    if pref is None:
        return
    db_execute(
        """
        UPDATE customers
        SET pref_hour_jst=%s, pref_hour_samples=%s, pref_hour_updated_at=now(), updated_at=now()
        WHERE shop_id=%s AND conv_key=%s
        """,
        (int(pref), int(samples), shop_id, conv_key),
    )


def choose_send_hour_jst(pref_hour: Optional[int]) -> int:
    if pref_hour is None:
        return 19
    try:
        h = int(pref_hour)
        return h if 0 <= h <= 23 else 19
    except Exception:
        return 19


# ============================================================
# Followup AB
# ============================================================

def pick_ab_variant(conv_key: str) -> str:
    if not FOLLOWUP_AB_ENABLED:
        return "A"
    h = hashlib.sha256(conv_key.encode("utf-8")).hexdigest()
    return "A" if (int(h[:2], 16) % 2 == 0) else "B"


# ============================================================
# Status ops
# ============================================================

def mark_opt_out(shop_id: str, conv_key: str, user_id: str) -> None:
    db_execute(
        """
        UPDATE customers
        SET opt_out=TRUE, opt_out_at=now(), status='OPTOUT', user_id=%s,
            need_reply=FALSE, need_reply_reason='optout', need_reply_updated_at=now(),
            perma_cold=TRUE,
            updated_at=now()
        WHERE shop_id=%s AND conv_key=%s
        """,
        (user_id, shop_id, conv_key),
    )


def mark_lost(shop_id: str, conv_key: str) -> None:
    db_execute(
        """
        UPDATE customers
        SET status='LOST',
            need_reply=FALSE, need_reply_reason='lost', need_reply_updated_at=now(),
            perma_cold=TRUE,
            updated_at=now()
        WHERE shop_id=%s AND conv_key=%s
        """,
        (shop_id, conv_key),
    )


def revive_if_lost_by_keywords(shop_id: str, conv_key: str, text: str) -> bool:
    rows = db_fetchall(
        "SELECT COALESCE(opt_out,FALSE), COALESCE(status,'ACTIVE'), COALESCE(won,FALSE) FROM customers WHERE shop_id=%s AND conv_key=%s",
        (shop_id, conv_key),
    )
    if not rows:
        return False
    if bool(rows[0][0]) or bool(rows[0][2]):
        return False
    if (rows[0][1] or "ACTIVE").upper() != "LOST":
        return False

    for pat in LOST_REVIVE_PATTERNS:
        if re.search(pat, text or ""):
            db_execute(
                "UPDATE customers SET status='ACTIVE', perma_cold=FALSE, updated_at=now() WHERE shop_id=%s AND conv_key=%s",
                (shop_id, conv_key),
            )
            return True
    return False


# ============================================================
# LINE signature verify + send
# ============================================================

def verify_signature(body: bytes, signature: str) -> bool:
    if not LINE_CHANNEL_SECRET:
        return False
    mac = hmac.new(LINE_CHANNEL_SECRET.encode("utf-8"), body, hashlib.sha256).digest()
    expected = base64.b64encode(mac).decode("utf-8")
    return hmac.compare_digest(expected, signature or "")


async def reply_line(reply_token: str, text: str) -> None:
    if not LINE_CHANNEL_ACCESS_TOKEN or not reply_token:
        return
    url = "https://api.line.me/v2/bot/message/reply"
    headers = {"Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}", "Content-Type": "application/json"}
    payload = {"replyToken": reply_token, "messages": [{"type": "text", "text": (text or "")[:4900]}]}
    try:
        async with httpx.AsyncClient(timeout=10, verify=certifi.where()) as client:
            r = await client.post(url, headers=headers, json=payload)
            if r.status_code >= 400:
                print("[LINE] reply failed:", r.status_code, r.text[:200])
    except Exception as e:
        print("[LINE] reply exception:", repr(e))


async def push_line(user_id: str, text: str) -> None:
    if not LINE_CHANNEL_ACCESS_TOKEN:
        return
    if not user_id or user_id == "unknown":
        return
    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}", "Content-Type": "application/json"}
    payload = {"to": user_id, "messages": [{"type": "text", "text": (text or "")[:4900]}]}
    try:
        async with httpx.AsyncClient(timeout=10, verify=certifi.where()) as client:
            r = await client.post(url, headers=headers, json=payload)
            if r.status_code >= 400:
                print("[LINE] push failed:", r.status_code, r.text[:200])
    except Exception as e:
        print("[LINE] push exception:", repr(e))


# ============================================================
# OpenAI prompts + schemas
# ============================================================

SYSTEM_PROMPT_ANALYZE = """
あなたは不動産仲介SaaSの「顧客温度判定AI」です。
会話履歴と最新発言から、成約に近い順に 1〜10 で温度を判定します。
同時に「意図(intent)」と「次に聞くべきこと(next_goal)」を決めてください。

【出力はJSONのみ（それ以外禁止）】
{
  "temp_level_raw": 1,
  "confidence": 0.50,
  "intent": "rent|buy|invest|research|other",
  "next_goal": "短い日本語",
  "reasons": ["根拠1","根拠2","根拠3"]
}

【過大評価防止】
- 条件が全く出ていない場合は最大でも Lv5
- 入居時期が半年以上先なら最大でも Lv6
- 返信が短い/曖昧な場合はLvを上げすぎない
""".strip()

SYSTEM_PROMPT_ASSISTANT = """
あなたは不動産仲介の優秀な営業アシスタントです。
ユーザーに対して丁寧で簡潔、次の行動につながる返信を日本語で作ってください。
・質問は最大2つ
・押し売り感を出さない
""".strip()

ANALYZE_JSON_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "temp_level_raw": {"type": "integer", "minimum": 1, "maximum": 10},
        "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
        "intent": {"type": "string", "enum": ["rent", "buy", "invest", "research", "other"]},
        "next_goal": {"type": "string", "maxLength": 80},
        "reasons": {"type": "array", "items": {"type": "string", "maxLength": 60}, "minItems": 0, "maxItems": 3},
    },
    "required": ["temp_level_raw", "confidence", "intent", "next_goal", "reasons"],
}

FOLLOWUP_JSON_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {"message": {"type": "string", "maxLength": 900}},
    "required": ["message"],
}

AUTO_COLD_JSON_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "perma_cold": {"type": "boolean"},
        "silence_score_delta": {"type": "integer", "minimum": -2, "maximum": 5},
        "reason": {"type": "string", "maxLength": 120},
    },
    "required": ["perma_cold", "silence_score_delta", "reason"],
}


async def openai_chat(messages: List[Dict[str, str]], temperature: float = 0.2, timeout_sec: float = 12.0) -> str:
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY missing")
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    payload = {"model": OPENAI_MODEL_ASSISTANT, "messages": messages, "temperature": temperature}
    async with httpx.AsyncClient(timeout=timeout_sec, verify=certifi.where()) as client:
        r = await client.post(OPENAI_CHAT_URL, headers=headers, json=payload)
        r.raise_for_status()
        data = r.json()
        return data["choices"][0]["message"]["content"]


def _responses_extract_text(data: Dict[str, Any]) -> Optional[str]:
    chunks: List[str] = []
    for item in (data.get("output") or []):
        if item.get("type") == "message":
            for c in (item.get("content") or []):
                if c.get("type") == "output_text" and isinstance(c.get("text"), str):
                    chunks.append(c["text"])
    return "\n".join(chunks).strip() if chunks else None


async def openai_responses_json(
    model: str,
    instructions: str,
    input_msgs: List[Dict[str, str]],
    schema: Dict[str, Any],
    timeout_sec: float,
) -> Optional[Dict[str, Any]]:
    if not OPENAI_API_KEY:
        return None
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": model,
        "instructions": instructions,
        "input": input_msgs,
        "text": {"format": {"type": "json_schema", "strict": True, "schema": schema}},
    }
    try:
        async with httpx.AsyncClient(timeout=timeout_sec, verify=certifi.where()) as client:
            r = await client.post(RESPONSES_API_URL, headers=headers, json=payload)
            if r.status_code >= 400:
                print("[OPENAI] responses failed:", r.status_code, (r.text or "")[:200])
                return None
            data = r.json()
        out = _responses_extract_text(data)
        if not out:
            return None
        return json.loads(out)
    except Exception as e:
        print("[OPENAI] responses exception:", repr(e))
        return None


def coerce_level(v: Any) -> int:
    try:
        return max(1, min(10, int(float(str(v).strip()))))
    except Exception:
        return 5


def coerce_conf(v: Any) -> float:
    try:
        f = float(v)
        if 1.0 < f <= 100.0:
            f = f / 100.0
        return max(0.0, min(1.0, f))
    except Exception:
        return 0.6


def coerce_intent(v: Any) -> str:
    s = str(v or "").strip().lower()
    return s if s in ("rent", "buy", "invest", "research", "other") else "other"


def coerce_goal(v: Any) -> str:
    s = str(v or "").strip()
    return s[:80] if s else "要件確認"


def _intent_label(intent: str) -> str:
    return {"rent": "賃貸", "buy": "購入", "invest": "投資", "research": "情報収集", "other": "不明"}.get(intent, "不明")


async def generate_followup_message_llm(shop_id: str, conv_key: str, stage: int, variant: str, customer: Dict[str, Any]) -> Optional[str]:
    if not FOLLOWUP_USE_LLM or not OPENAI_API_KEY:
        return None
    instructions = (
        "あなたは不動産/投資のトップ営業です。追客LINE文を1通だけ作ってください。"
        "押し売り禁止。質問は最大2つ。返信しやすい形。短め。出力はJSONのみ。"
    )

    intent = (customer.get("intent") or "other").strip().lower()
    next_goal = (customer.get("next_goal") or "").strip()
    last_user_text = (customer.get("last_user_text") or "").strip()

    ctx = {
        "stage": stage,
        "variant": variant,
        "tone": ("やさしく丁寧" if variant == "A" else "短く選択肢"),
        "intent": intent,
        "intent_label": _intent_label(intent),
        "next_goal": next_goal,
        "last_user_text": last_user_text[:160],
    }

    history = get_recent_conversation_for_followup(shop_id, conv_key, 12)
    input_msgs = [{"role": "user", "content": "条件: " + json.dumps(ctx, ensure_ascii=False)}]
    input_msgs.append({"role": "user", "content": "会話:"})
    for m in history[-12:]:
        input_msgs.append({"role": m["role"], "content": m["content"][:800]})

    j = await openai_responses_json(
        model=OPENAI_MODEL_FOLLOWUP,
        instructions=instructions,
        input_msgs=input_msgs,
        schema=FOLLOWUP_JSON_SCHEMA,
        timeout_sec=FOLLOWUP_LLM_TIMEOUT_SEC,
    )
    if not j:
        return None
    msg = str(j.get("message") or "").strip()
    msg = re.sub(r"\n{3,}", "\n\n", msg).strip()
    return msg[:900] if msg else None


def followup_fallback(stage: int, intent: str, goal: str) -> str:
    if stage == 2:
        return "その後いかがでしょうか？急ぎでなければ大丈夫です。必要なら一言だけ返信ください🙂"
    if intent == "research":
        return "参考資料のご希望ありますか？必要になったら一言だけで大丈夫です🙂"
    if any(k in (goal or "") for k in ["内見", "日程", "候補日"]):
        return "内見希望でしたら、希望の曜日や時間帯（例：土日午後/平日夜など）を教えてください。"
    return "その後いかがでしょうか？ご希望条件（エリア・予算・入居時期）だけでも教えてください。"


async def auto_cold_judge(shop_id: str, conv_key: str, history: List[Dict[str, str]]) -> Optional[Dict[str, Any]]:
    if not AUTO_COLD_USE_LLM or not OPENAI_API_KEY:
        return None
    instructions = (
        "あなたは営業責任者。追客を継続すべきか判断する。"
        "perma_cold=true なら追客停止。silence_score_deltaは-2〜+5。出力はJSONのみ。"
    )
    input_msgs = [{"role": "user", "content": "会話履歴:"}]
    for m in history[-12:]:
        input_msgs.append({"role": m["role"], "content": m["content"][:800]})
    return await openai_responses_json(
        model=OPENAI_MODEL_COLD,
        instructions=instructions,
        input_msgs=input_msgs,
        schema=AUTO_COLD_JSON_SCHEMA,
        timeout_sec=AUTO_COLD_LLM_TIMEOUT_SEC,
    )


def apply_silence_update(shop_id: str, conv_key: str, delta: int, perma: bool) -> None:
    rows = db_fetchall("SELECT COALESCE(silence_score,0) FROM customers WHERE shop_id=%s AND conv_key=%s", (shop_id, conv_key))
    cur = int(rows[0][0] or 0) if rows else 0
    nxt = max(0, min(999, cur + int(delta)))
    db_execute(
        "UPDATE customers SET silence_score=%s, perma_cold=%s, updated_at=now() WHERE shop_id=%s AND conv_key=%s",
        (nxt, bool(perma), shop_id, conv_key),
    )


# ============================================================
# Followup logs + attribution
# ============================================================

def save_followup_log(shop_id: str, conv_key: str, user_id: str, message: str, mode: str, status: str,
                      error: Optional[str] = None, variant: Optional[str] = None, stage: Optional[int] = None,
                      send_hour_jst: Optional[int] = None) -> None:
    db_execute(
        """
        INSERT INTO followup_logs (shop_id, conv_key, user_id, message, mode, status, error, variant, stage, send_hour_jst)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (shop_id, conv_key, user_id, message, mode, status, error, variant, stage, send_hour_jst),
    )


def attribute_followup_response(shop_id: str, conv_key: str) -> None:
    window_since = utcnow() - timedelta(hours=FOLLOWUP_ATTRIBUTION_WINDOW_HOURS)
    rows = db_fetchall(
        """
        SELECT id
        FROM followup_logs
        WHERE shop_id=%s AND conv_key=%s
          AND status='sent'
          AND responded_at IS NULL
          AND created_at >= %s
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (shop_id, conv_key, window_since),
    )
    if not rows:
        return
    fid = rows[0][0]
    db_execute("UPDATE followup_logs SET responded_at=now() WHERE id=%s", (fid,))


# ============================================================
# Analyze + fast reply
# ============================================================

async def analyze_only(shop_id: str, conv_key: str, user_text: str) -> Tuple[int, float, str, str, List[str], str]:
    t = (user_text or "").strip()

    for pat in OPTOUT_PATTERNS:
        if re.search(pat, t, flags=re.IGNORECASE):
            return 1, 0.95, "other", "関係終了確認", ["optout"], "OPTOUT"

    for pat in CANCEL_PATTERNS:
        if re.search(pat, t):
            return 2, 0.90, "other", "関係終了確認", ["lost"], "LOST"

    hist = get_recent_conversation(shop_id, conv_key, ANALYZE_HISTORY_LIMIT)
    if not hist or hist[-1].get("content") != user_text:
        hist.append({"role": "user", "content": user_text})

    j = await openai_responses_json(
        model=OPENAI_MODEL_ANALYZE,
        instructions=SYSTEM_PROMPT_ANALYZE,
        input_msgs=hist[-max(2, ANALYZE_HISTORY_LIMIT):],
        schema=ANALYZE_JSON_SCHEMA,
        timeout_sec=ANALYZE_TIMEOUT_SEC,
    )

    if j:
        lvl = coerce_level(j.get("temp_level_raw", 5))
        conf = coerce_conf(j.get("confidence", 0.6))
        intent = coerce_intent(j.get("intent", "other"))
        goal = coerce_goal(j.get("next_goal", "要件確認"))
        reasons = j.get("reasons", [])
        if not isinstance(reasons, list):
            reasons = []
        reasons = [str(x).strip()[:60] for x in reasons][:3]
        return lvl, conf, intent, goal, reasons, ""

    if len(t) <= SHORT_TEXT_MAX_LEN:
        return 3, 0.75, "other", "要件確認", ["短文"], ""

    return 5, 0.6, "other", "要件確認", ["fallback"], ""


async def generate_reply_only(user_id: str, user_text: str) -> str:
    history = CHAT_HISTORY[user_id]
    ctx = [{"role": "system", "content": SYSTEM_PROMPT_ASSISTANT}]
    for role, content in list(history)[-10:]:
        ctx.append({"role": role, "content": content})
    ctx.append({"role": "user", "content": user_text})

    reply_text = ""
    try:
        reply_text = await openai_chat(ctx, temperature=0.35, timeout_sec=FAST_REPLY_TIMEOUT_SEC)
        reply_text = (reply_text or "").strip()
    except Exception:
        reply_text = ""

    if not reply_text:
        reply_text = "ありがとうございます。条件をもう少し教えてください（エリア/予算/間取り/入居時期など）。"

    history.append(("user", user_text))
    history.append(("assistant", reply_text))
    return reply_text


# ============================================================
# Job lock
# ============================================================

def acquire_job_lock(key: str, ttl_sec: int) -> bool:
    now = utcnow()
    until = now + timedelta(seconds=max(10, int(ttl_sec)))
    rows = db_fetchall("SELECT locked_until FROM job_locks WHERE key=%s", (key,))
    if rows and rows[0][0] and rows[0][0] > now:
        return False
    db_execute(
        """
        INSERT INTO job_locks (key, locked_until)
        VALUES (%s,%s)
        ON CONFLICT (key) DO UPDATE SET locked_until=EXCLUDED.locked_until
        """,
        (key, until),
    )
    return True


# ============================================================
# Routes: basics
# ============================================================

@app.get("/")
async def root():
    return {"ok": True, "shop_id": SHOP_ID}


@app.get("/healthz")
async def healthz():
    return {"ok": True, "ts": int(utcnow().timestamp())}


# ============================================================
# API: customers list / dist / customer detail
# ============================================================

@app.get("/api/hot")
async def api_hot(
    _: None = Depends(require_dashboard_key),
    shop_id: str = Query(default=SHOP_ID),
    min_level: int = Query(default=1, ge=1, le=10),
    limit: int = Query(default=120, ge=1, le=300),
):
    rows = db_fetchall(
        """
        SELECT conv_key, user_id, last_user_text, temp_level_stable, confidence,
               COALESCE(intent,'other'), COALESCE(next_goal,''),
               updated_at,
               COALESCE(status,'ACTIVE'),
               COALESCE(need_reply,FALSE), COALESCE(need_reply_reason,''),
               COALESCE(won,FALSE), COALESCE(perma_cold,FALSE), COALESCE(silence_score,0),
               pref_hour_jst, pref_hour_samples
        FROM customers
        WHERE shop_id=%s AND COALESCE(temp_level_stable,0) >= %s
        ORDER BY need_reply DESC, updated_at DESC
        LIMIT %s
        """,
        (shop_id, min_level, limit),
    )
    return JSONResponse([
        {
            "conv_key": r[0],
            "user_id": r[1],
            "message": r[2],
            "temp_level_stable": r[3],
            "confidence": float(r[4]) if r[4] is not None else None,
            "intent": r[5],
            "next_goal": r[6],
            "ts": r[7].isoformat() if r[7] else None,
            "status": r[8],
            "need_reply": bool(r[9]),
            "need_reply_reason": r[10],
            "won": bool(r[11]),
            "perma_cold": bool(r[12]),
            "silence_score": int(r[13] or 0),
            "pref_hour_jst": r[14],
            "pref_hour_samples": r[15],
        }
        for r in rows
    ])


@app.get("/api/stats/level_dist")
async def api_stats_level_dist(
    _: None = Depends(require_dashboard_key),
    shop_id: str = Query(default=SHOP_ID),
):
    rows = db_fetchall(
        """
        SELECT temp_level_stable, COUNT(*)
        FROM customers
        WHERE shop_id=%s AND temp_level_stable BETWEEN 1 AND 10
        GROUP BY temp_level_stable
        """,
        (shop_id,),
    )
    dist = {str(i): 0 for i in range(1, 11)}
    for lv, cnt in rows:
        if lv is None:
            continue
        lv_i = int(lv)
        if 1 <= lv_i <= 10:
            dist[str(lv_i)] = int(cnt or 0)
    return JSONResponse(dist)


@app.get("/api/customer/detail")
async def api_customer_detail(
    _: None = Depends(require_dashboard_key),
    shop_id: str = Query(default=SHOP_ID),
    conv_key: str = Query(...),
    msg_limit: int = Query(default=180, ge=10, le=400),
    followup_limit: int = Query(default=80, ge=10, le=200),
):
    crow = db_fetchall(
        """
        SELECT conv_key, user_id, last_user_text, temp_level_stable, confidence,
               COALESCE(intent,'other'), COALESCE(next_goal,''),
               updated_at,
               COALESCE(status,'ACTIVE'),
               COALESCE(opt_out,FALSE),
               COALESCE(need_reply,FALSE), COALESCE(need_reply_reason,''),
               COALESCE(perma_cold,FALSE), COALESCE(silence_score,0),
               COALESCE(won,FALSE), won_at, won_by,
               pref_hour_jst, pref_hour_samples
        FROM customers
        WHERE shop_id=%s AND conv_key=%s
        LIMIT 1
        """,
        (shop_id, conv_key),
    )
    customer = None
    if crow:
        r = crow[0]
        customer = {
            "conv_key": r[0],
            "user_id": r[1],
            "last_user_text": r[2],
            "temp_level_stable": r[3],
            "confidence": float(r[4]) if r[4] is not None else None,
            "intent": r[5],
            "next_goal": r[6],
            "updated_at": r[7].isoformat() if r[7] else None,
            "status": r[8],
            "opt_out": bool(r[9]),
            "need_reply": bool(r[10]),
            "need_reply_reason": r[11],
            "perma_cold": bool(r[12]),
            "silence_score": int(r[13] or 0),
            "won": bool(r[14]),
            "won_at": r[15].isoformat() if r[15] else None,
            "won_by": r[16],
            "pref_hour_jst": r[17],
            "pref_hour_samples": r[18],
        }

    msgs = db_fetchall(
        """
        SELECT role, content, created_at, temp_level_stable, confidence, COALESCE(intent,'other'), COALESCE(next_goal,'')
        FROM messages
        WHERE shop_id=%s AND conv_key=%s
        ORDER BY created_at DESC
        LIMIT %s
        """,
        (shop_id, conv_key, int(msg_limit)),
    )
    msgs = list(reversed(msgs))
    messages = [
        {
            "role": m[0],
            "content": m[1],
            "ts": m[2].isoformat() if m[2] else None,
            "temp_level_stable": m[3],
            "confidence": float(m[4]) if m[4] is not None else None,
            "intent": m[5],
            "next_goal": m[6],
        }
        for m in msgs
    ]

    fls = db_fetchall(
        """
        SELECT variant, mode, status, stage, message, error, responded_at, send_hour_jst, created_at
        FROM followup_logs
        WHERE shop_id=%s AND conv_key=%s
        ORDER BY created_at DESC
        LIMIT %s
        """,
        (shop_id, conv_key, int(followup_limit)),
    )
    followups = [
        {
            "variant": f[0],
            "mode": f[1],
            "status": f[2],
            "stage": f[3],
            "message": f[4],
            "error": f[5],
            "responded_at": f[6].isoformat() if f[6] else None,
            "send_hour_jst": f[7],
            "ts": f[8].isoformat() if f[8] else None,
        }
        for f in fls
    ]

    return JSONResponse({"ok": True, "customer": customer, "messages": messages, "followups": followups})


# ============================================================
# KPI + Executive
# ============================================================

def get_kpi_summary(shop_id: str, days: int = 30) -> Dict[str, Any]:
    since = utcnow() - timedelta(days=max(1, min(365, int(days))))

    total = db_fetchall("SELECT COUNT(*) FROM customers WHERE shop_id=%s AND updated_at >= %s", (shop_id, since))[0][0]
    won = db_fetchall("SELECT COUNT(*) FROM customers WHERE shop_id=%s AND COALESCE(won,FALSE)=TRUE AND won_at >= %s", (shop_id, since))[0][0]
    ai_touched = db_fetchall("SELECT COUNT(DISTINCT conv_key) FROM messages WHERE shop_id=%s AND role='assistant' AND created_at >= %s", (shop_id, since))[0][0]

    intent_rows = db_fetchall(
        """
        SELECT COALESCE(intent,'other'), COUNT(*)
        FROM customers
        WHERE shop_id=%s AND COALESCE(won,FALSE)=TRUE AND won_at >= %s
        GROUP BY COALESCE(intent,'other')
        """,
        (shop_id, since),
    )
    intent_win = {str(r[0] or "other"): int(r[1] or 0) for r in intent_rows}

    return {
        "period_days": int(days),
        "customers": int(total),
        "won": int(won),
        "win_rate": (won / total) if total else 0.0,
        "ai_touched": int(ai_touched),
        "ai_touch_rate": (ai_touched / total) if total else 0.0,
        "intent_win": intent_win,
    }


@app.get("/api/kpi")
async def api_kpi(
    _: None = Depends(require_dashboard_key),
    shop_id: str = Query(default=SHOP_ID),
    days: int = Query(default=30, ge=1, le=365),
):
    return JSONResponse(get_kpi_summary(shop_id, days))


# ============================================================
# Admin APIs
# ============================================================

@app.post("/api/customer/mark_won")
async def api_mark_won(
    _: None = Depends(require_admin_key),
    shop_id: str = Query(default=SHOP_ID),
    conv_key: str = Query(...),
):
    mark_won(shop_id, conv_key, by="admin")
    return {"ok": True}


@app.post("/api/customer/unmark_won")
async def api_unmark_won(
    _: None = Depends(require_admin_key),
    shop_id: str = Query(default=SHOP_ID),
    conv_key: str = Query(...),
):
    unmark_won(shop_id, conv_key)
    return {"ok": True}


@app.post("/api/customer/clear_perma_cold")
async def api_clear_perma_cold(
    _: None = Depends(require_admin_key),
    shop_id: str = Query(default=SHOP_ID),
    conv_key: str = Query(...),
):
    db_execute("UPDATE customers SET perma_cold=FALSE, silence_score=0, updated_at=now() WHERE shop_id=%s AND conv_key=%s", (shop_id, conv_key))
    return {"ok": True}


@app.post("/api/customer/send_followup_now")
async def api_customer_send_followup_now(
    _: None = Depends(require_admin_key),
    shop_id: str = Query(default=SHOP_ID),
    conv_key: str = Query(...),
    stage: int = Query(default=1, ge=1, le=2),
):
    crows = db_fetchall(
        """
        SELECT user_id, COALESCE(intent,'other'), COALESCE(next_goal,''), COALESCE(last_user_text,''), COALESCE(temp_level_stable,0),
               COALESCE(opt_out,FALSE), COALESCE(perma_cold,FALSE), COALESCE(won,FALSE), COALESCE(status,'ACTIVE')
        FROM customers
        WHERE shop_id=%s AND conv_key=%s
        LIMIT 1
        """,
        (shop_id, conv_key),
    )
    if not crows:
        return {"ok": True, "sent": False, "reason": "not_found"}

    user_id, intent, goal, last_text, lvl, opt, cold, won, st = crows[0]
    if opt or cold or won or (st or "ACTIVE").upper() in ("OPTOUT", "LOST", "WON"):
        return {"ok": True, "sent": False, "reason": "inactive"}
    if not user_id or user_id == "unknown":
        return {"ok": True, "sent": False, "reason": "no_user_id"}

    variant = pick_ab_variant(conv_key)
    customer = {"intent": intent, "next_goal": goal, "last_user_text": last_text, "level": int(lvl or 0)}
    msg = await generate_followup_message_llm(shop_id, conv_key, stage, variant, customer)
    mode = "llm"
    if not msg:
        msg = followup_fallback(stage, intent or "other", goal or "")
        mode = "template"

    try:
        await push_line(user_id, msg)
        save_followup_log(shop_id, conv_key, user_id, msg, mode, "sent", None, variant, stage, send_hour_jst=now_jst().hour)
        return {"ok": True, "sent": True, "mode": mode}
    except Exception as e:
        save_followup_log(shop_id, conv_key, user_id, msg, mode, "failed", str(e)[:200], variant, stage, send_hour_jst=now_jst().hour)
        return {"ok": True, "sent": False, "error": str(e)[:200]}


# ============================================================
# Jobs
# ============================================================

@app.post("/jobs/followup")
async def job_followup(_: None = Depends(require_admin_key)):
    if not FOLLOWUP_ENABLED:
        return {"ok": True, "enabled": False, "reason": "FOLLOWUP_ENABLED!=1"}
    if not is_within_jst_window():
        return {"ok": True, "enabled": True, "skipped": True, "reason": "out_of_time_window"}
    if not acquire_job_lock("followup", FOLLOWUP_LOCK_TTL_SEC):
        return {"ok": True, "enabled": True, "skipped": True, "reason": "locked"}

    threshold = utcnow() - timedelta(minutes=FOLLOWUP_AFTER_MINUTES)
    rows = db_fetchall(
        """
        SELECT conv_key, user_id, COALESCE(temp_level_stable,0), COALESCE(next_goal,''), COALESCE(last_user_text,''),
               COALESCE(intent,'other'),
               pref_hour_jst, updated_at,
               COALESCE(opt_out,FALSE), COALESCE(perma_cold,FALSE), COALESCE(won,FALSE), COALESCE(status,'ACTIVE')
        FROM customers
        WHERE shop_id=%s
          AND COALESCE(temp_level_stable,0) >= %s
          AND updated_at < %s
          AND COALESCE(user_id,'') <> ''
        ORDER BY updated_at ASC
        LIMIT %s
        """,
        (SHOP_ID, FOLLOWUP_MIN_LEVEL, threshold, FOLLOWUP_LIMIT),
    )

    sent = 0
    skipped_time = 0
    failed = 0
    now_hour = now_jst().hour

    for (conv_key, user_id, lvl, goal, last_text, intent, pref_hour, updated_at, opt, cold, won, st) in rows:
        st_u = (st or "ACTIVE").upper()
        if opt or cold or won or st_u in ("OPTOUT", "LOST", "WON"):
            continue

        age_h = (utcnow() - updated_at).total_seconds() / 3600.0 if updated_at else 999
        if age_h < FOLLOWUP_FORCE_SEND_AFTER_HOURS:
            target = choose_send_hour_jst(pref_hour)
            if not within_hour_band(now_hour, target, FOLLOWUP_TIME_MATCH_HOURS):
                skipped_time += 1
                continue

        customer = {"intent": intent, "next_goal": goal, "last_user_text": last_text, "level": int(lvl or 0)}
        variant = pick_ab_variant(conv_key)
        msg = await generate_followup_message_llm(SHOP_ID, conv_key, 1, variant, customer)
        mode = "llm"
        if not msg:
            msg = followup_fallback(1, intent or "other", goal or "")
            mode = "template"

        try:
            await push_line(user_id, msg)
            save_followup_log(SHOP_ID, conv_key, user_id, msg, mode, "sent", None, variant, 1, send_hour_jst=now_hour)
            sent += 1
        except Exception as e:
            save_followup_log(SHOP_ID, conv_key, user_id, msg, mode, "failed", str(e)[:200], variant, 1, send_hour_jst=now_hour)
            failed += 1

    return {"ok": True, "sent": sent, "skipped_time": skipped_time, "failed": failed, "now_hour_jst": now_hour}


@app.post("/jobs/auto_cold")
async def job_auto_cold(_: None = Depends(require_admin_key)):
    if not acquire_job_lock("auto_cold", 300):
        return {"ok": True, "skipped": "locked"}

    rows = db_fetchall(
        """
        SELECT conv_key
        FROM customers
        WHERE shop_id=%s
          AND COALESCE(won,FALSE)=FALSE
          AND COALESCE(opt_out,FALSE)=FALSE
          AND COALESCE(perma_cold,FALSE)=FALSE
        ORDER BY updated_at ASC
        LIMIT %s
        """,
        (SHOP_ID, AUTO_COLD_LIMIT),
    )

    judged = 0
    for (conv_key,) in rows:
        hist = get_recent_conversation(SHOP_ID, conv_key, 12)
        if not hist:
            continue
        j = await auto_cold_judge(SHOP_ID, conv_key, hist)
        if not j:
            continue
        apply_silence_update(SHOP_ID, conv_key, int(j.get("silence_score_delta", 0)), bool(j.get("perma_cold", False)))
        judged += 1

    return {"ok": True, "judged": judged}


@app.post("/jobs/kpi_snapshot")
async def job_kpi_snapshot(_: None = Depends(require_admin_key)):
    return {"ok": True, "kpi": get_kpi_summary(SHOP_ID, 30)}


# ============================================================
# HTML templates (NO f-string) using replace
# ============================================================

DASHBOARD_HTML = r"""<!doctype html>
<html lang="ja">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
  body{font-family:system-ui;margin:16px;background:#0b1020;color:#fff}
  .row{display:flex;gap:12px;flex-wrap:wrap;align-items:center}
  .card{background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.12);border-radius:14px;padding:12px}
  table{width:100%;border-collapse:collapse}
  th,td{border-bottom:1px solid rgba(255,255,255,.12);padding:8px;font-size:12px;vertical-align:top}
  th{color:rgba(255,255,255,.7);text-align:left}
  .mono{font-family:ui-monospace,Menlo,Monaco,Consolas,monospace}
  a{color:#8ab4f8}
</style>
</head>
<body>
  <div class="row">
    <div class="card"><b>SHOP</b> <span class="mono">__SHOP__</span></div>
    <div class="card"><a href="/dashboard/executive?shop_id=__SHOP__&days=30&key=__KEY__">Executive KPI →</a></div>
  </div>

  <div class="row" style="margin-top:12px;">
    <div class="card" style="flex:1;min-width:320px;">
      <div style="display:flex;justify-content:space-between;align-items:center;">
        <b>温度分布（全体）</b><span class="mono">/api/stats/level_dist</span>
      </div>
      <canvas id="chart" height="110"></canvas>
    </div>
    <div class="card" style="flex:2;min-width:520px;">
      <div style="display:flex;justify-content:space-between;align-items:center;">
        <b>顧客（need_reply優先）</b><span class="mono">/api/hot</span>
      </div>
      <div style="margin-top:8px;overflow:auto;max-height:520px;">
        <table>
          <thead><tr>
            <th>更新</th><th>Lv</th><th>intent</th><th>need</th><th>goal</th><th>pref</th><th>flags</th><th>user</th><th>msg</th>
          </tr></thead>
          <tbody id="rows"><tr><td colspan="9">loading...</td></tr></tbody>
        </table>
      </div>
    </div>
  </div>

<script>
  const KEY = "__KEY__";
  const SHOP = "__SHOP__";
  const REFRESH = __REFRESH__;

  function esc(s) {
    return (s ?? "").toString().replace(/[&<>"]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;"}[c]));
  }
  function fmt(iso) {
    if (!iso) return "-";
    try { return new Date(iso).toLocaleString(); } catch(e) { return iso; }
  }
  async function fetchJson(url) { const r = await fetch(url); return await r.json(); }

  function flagsCell(r){
    let parts=[];
    if (r.won) parts.push("WON");
    if (r.perma_cold) parts.push("COLD");
    if (r.silence_score != null && r.silence_score >= 5) parts.push("SIL+" + r.silence_score);
    return parts.length ? parts.join(",") : "-";
  }

  let chart=null;

  async function renderDist(){
    const url = "/api/stats/level_dist?shop_id=" + encodeURIComponent(SHOP) + "&key=" + encodeURIComponent(KEY);
    const dist = await fetchJson(url);
    const labels = ["1","2","3","4","5","6","7","8","9","10"];
    const data = labels.map(k => Number(dist[k] || 0));
    const ctx = document.getElementById("chart");
    if (chart){
      chart.data.labels = labels;
      chart.data.datasets[0].data = data;
      chart.update();
      return;
    }
    chart = new Chart(ctx, {type:"bar", data:{labels:labels, datasets:[{label:"count", data:data, borderWidth:1}]}, options:{responsive:true}});
  }

  async function renderRows(){
    const url = "/api/hot?shop_id=" + encodeURIComponent(SHOP) + "&min_level=1&limit=120&key=" + encodeURIComponent(KEY);
    const rows = await fetchJson(url);
    const tbody = document.getElementById("rows");
    if (!rows.length){ tbody.innerHTML = "<tr><td colspan='9'>no data</td></tr>"; return; }

    tbody.innerHTML = rows.map(r => {
      const link = "/dashboard/customer?shop_id=" + encodeURIComponent(SHOP) + "&conv_key=" + encodeURIComponent(r.conv_key) + "&key=" + encodeURIComponent(KEY);
      const pref = (r.pref_hour_jst == null) ? "-" : (String(r.pref_hour_jst) + "(" + String(r.pref_hour_samples || 0) + ")");
      const need = r.need_reply ? (r.need_reply_reason || "need") : "-";
      return "<tr>"
        + "<td class='mono'>" + esc(fmt(r.ts)) + "</td>"
        + "<td class='mono'>" + esc(r.temp_level_stable) + "</td>"
        + "<td class='mono'>" + esc(r.intent || "-") + "</td>"
        + "<td>" + esc(need) + "</td>"
        + "<td>" + esc(r.next_goal || "-") + "</td>"
        + "<td class='mono'>" + esc(pref) + "</td>"
        + "<td class='mono'>" + esc(flagsCell(r)) + "</td>"
        + "<td class='mono'><a href='" + link + "'>" + esc(r.user_id || "") + "</a></td>"
        + "<td>" + esc((r.message || "").slice(0,140)) + "</td>"
        + "</tr>";
    }).join("");
  }

  async function tick(){ await Promise.all([renderDist(), renderRows()]); }
  tick().catch(console.error);
  if (REFRESH > 0) setInterval(() => tick().catch(console.error), REFRESH * 1000);
</script>
</body>
</html>
"""

EXEC_HTML = r"""<!doctype html>
<html lang="ja">
<head>
<meta charset="utf-8"/>
<title>Executive</title>
<style>
 body{font-family:system-ui;background:#0b1020;color:#fff;margin:20px}
 .card{background:rgba(255,255,255,.08);border-radius:14px;padding:14px;margin-bottom:14px}
 .row{display:flex;gap:14px;flex-wrap:wrap}
 .kpi{font-size:28px;font-weight:700}
 .mono{font-family:ui-monospace}
 a{color:#8ab4f8}
</style>
</head>
<body>
  <div class="card">
    <a href="/dashboard?shop_id=__SHOP__&key=__KEY__">← back</a>
    <h2>Executive KPI（__DAYS__日）</h2>
  </div>

  <div id="kpis" class="row"></div>

<script>
  const KEY = "__KEY__";
  const SHOP = "__SHOP__";
  const DAYS = __DAYS__;

  async function fetchJson(url){ const r = await fetch(url); return await r.json(); }

  (async () => {
    const url = "/api/kpi?shop_id=" + encodeURIComponent(SHOP)
      + "&days=" + encodeURIComponent(String(DAYS))
      + "&key=" + encodeURIComponent(KEY);
    const k = await fetchJson(url);

    document.getElementById("kpis").innerHTML =
      "<div class='card'><div>顧客数</div><div class='kpi'>" + k.customers + "</div></div>"
      + "<div class='card'><div>成約数</div><div class='kpi'>" + k.won + "</div></div>"
      + "<div class='card'><div>成約率</div><div class='kpi'>" + (k.win_rate*100).toFixed(1) + "%</div></div>"
      + "<div class='card'><div>AI関与率</div><div class='kpi'>" + (k.ai_touch_rate*100).toFixed(1) + "%</div></div>"
      + "<div class='card'><div>intent別成約</div><pre class='mono'>" + JSON.stringify(k.intent_win,null,2) + "</pre></div>";
  })().catch(console.error);
</script>
</body>
</html>
"""

CUSTOMER_HTML = r"""<!doctype html>
<html lang="ja">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Customer</title>
<style>
  body{font-family:system-ui;background:#0b1020;color:#fff;margin:16px}
  .card{background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.12);border-radius:14px;padding:12px;margin-bottom:12px}
  .mono{font-family:ui-monospace,Menlo,Monaco,Consolas,monospace}
  a{color:#8ab4f8}
  button{padding:8px 10px;border-radius:10px;border:1px solid rgba(255,255,255,.18);background:rgba(255,255,255,.06);color:#fff;cursor:pointer}
  .btnrow{display:flex;gap:10px;flex-wrap:wrap}
  pre{white-space:pre-wrap}
  .msg{padding:8px;border-bottom:1px solid rgba(255,255,255,.12)}
</style>
</head>
<body>
  <div class="card">
    <a href="/dashboard?shop_id=__SHOP__&key=__KEY__">← back</a>
    <div class="mono" style="margin-top:8px;">__CONV__</div>
  </div>

  <div class="card" id="cust">loading...</div>

  <div class="card">
    <div class="btnrow">
      <button onclick="markWon()">🟢 成約（WIN）</button>
      <button onclick="unmarkWon()">↩︎ 成約取消</button>
      <button onclick="clearCold()">🧊 perma_cold解除</button>
      <button onclick="runAutoCold()">🤖 auto_cold</button>
      <button onclick="sendFollowup(1)">📨 followup(1)</button>
      <button onclick="sendFollowup(2)">📨 followup(2)</button>
    </div>
  </div>

  <div class="card"><b>Messages</b><div id="msgs">loading...</div></div>
  <div class="card"><b>Followups</b><div id="fls">loading...</div></div>

<script>
  const KEY = "__KEY__";
  const SHOP = "__SHOP__";
  const CONV = "__CONV__";

  function esc(s) {
    return (s ?? "").toString().replace(/[&<>"]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;"}[c]));
  }
  async function fetchJson(url, opts){ const r = await fetch(url, opts || undefined); return await r.json(); }
  function adminKey(){ return localStorage.getItem("dashboard_admin_key") || ""; }

  async function reload(){
    const url = "/api/customer/detail?shop_id=" + encodeURIComponent(SHOP)
      + "&conv_key=" + encodeURIComponent(CONV)
      + "&key=" + encodeURIComponent(KEY);

    const d = await fetchJson(url);
    const c = d.customer || {};

    document.getElementById("cust").innerHTML =
      "<div><b>User</b>: <span class='mono'>" + esc(c.user_id || "-") + "</span></div>"
      + "<div><b>Status</b>: <span class='mono'>" + esc(c.status || "-") + "</span>"
      + " / <b>Lv</b>: <span class='mono'>" + esc(c.temp_level_stable || "-") + "</span>"
      + " / <b>conf</b>: <span class='mono'>" + esc(c.confidence==null? "-" : Number(c.confidence).toFixed(2)) + "</span></div>"
      + "<div><b>Intent</b>: <span class='mono'>" + esc(c.intent || "-") + "</span>"
      + " / <b>Goal</b>: " + esc(c.next_goal || "-") + "</div>"
      + "<div><b>need_reply</b>: <span class='mono'>" + esc(c.need_reply ? "TRUE" : "FALSE") + "</span> (" + esc(c.need_reply_reason || "") + ")</div>"
      + "<div><b>perma_cold</b>: <span class='mono'>" + esc(c.perma_cold ? "TRUE" : "FALSE") + "</span>"
      + " / <b>silence_score</b>: <span class='mono'>" + esc(String(c.silence_score ?? 0)) + "</span></div>"
      + "<div><b>won</b>: <span class='mono'>" + esc(c.won ? "TRUE" : "FALSE") + "</span>"
      + " / <b>won_at</b>: <span class='mono'>" + esc(c.won_at || "-") + "</span>"
      + " / <b>won_by</b>: <span class='mono'>" + esc(c.won_by || "-") + "</span></div>";

    const msgs = d.messages || [];
    document.getElementById("msgs").innerHTML = msgs.length ? msgs.map(m => {
      return "<div class='msg'>"
        + "<div class='mono'>" + esc(m.role) + " / " + esc(m.ts || "") + " / Lv:" + esc(m.temp_level_stable || "-")
        + " / intent:" + esc(m.intent || "-") + " / goal:" + esc(m.next_goal || "-") + "</div>"
        + "<pre>" + esc(m.content || "") + "</pre>"
        + "</div>";
    }).join("") : "no messages";

    const fls = d.followups || [];
    document.getElementById("fls").innerHTML = fls.length ? fls.map(f => {
      return "<div class='msg'>"
        + "<div class='mono'>ts:" + esc(f.ts || "") + " / stage:" + esc(String(f.stage ?? "-"))
        + " / mode:" + esc(f.mode || "-") + " / status:" + esc(f.status || "-")
        + " / send_hour:" + esc(String(f.send_hour_jst ?? "-")) + "</div>"
        + "<pre>" + esc(f.message || "") + "</pre>"
        + "<div class='mono'>responded:" + esc(f.responded_at || "-") + " / err:" + esc(f.error || "-") + "</div>"
        + "</div>";
    }).join("") : "no followups";
  }

  async function markWon(){
    const url = "/api/customer/mark_won?shop_id=" + encodeURIComponent(SHOP) + "&conv_key=" + encodeURIComponent(CONV);
    await fetchJson(url, {method:"POST", headers:{"x-admin-key": adminKey()}});
    await reload();
  }
  async function unmarkWon(){
    const url = "/api/customer/unmark_won?shop_id=" + encodeURIComponent(SHOP) + "&conv_key=" + encodeURIComponent(CONV);
    await fetchJson(url, {method:"POST", headers:{"x-admin-key": adminKey()}});
    await reload();
  }
  async function clearCold(){
    const url = "/api/customer/clear_perma_cold?shop_id=" + encodeURIComponent(SHOP) + "&conv_key=" + encodeURIComponent(CONV);
    await fetchJson(url, {method:"POST", headers:{"x-admin-key": adminKey()}});
    await reload();
  }
  async function runAutoCold(){
    const url = "/jobs/auto_cold";
    await fetchJson(url, {method:"POST", headers:{"x-admin-key": adminKey()}});
    await reload();
  }
  async function sendFollowup(stage){
    const url = "/api/customer/send_followup_now?shop_id=" + encodeURIComponent(SHOP)
      + "&conv_key=" + encodeURIComponent(CONV)
      + "&stage=" + encodeURIComponent(String(stage));
    await fetchJson(url, {method:"POST", headers:{"x-admin-key": adminKey()}});
    await reload();
  }

  if (!localStorage.getItem("dashboard_admin_key")) {
    const k = prompt("ADMIN_API_KEY を入れてください（このブラウザに保存）");
    if (k) localStorage.setItem("dashboard_admin_key", k.trim());
  }

  reload().catch(console.error);
</script>
</body>
</html>
"""


# ============================================================
# Dashboards (render templates)
# ============================================================

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(
    _: None = Depends(require_dashboard_key),
    shop_id: str = Query(default=SHOP_ID),
    key: Optional[str] = Query(default=None),
):
    key_q = (key or "").strip()
    html = DASHBOARD_HTML
    html = html.replace("__SHOP__", str(shop_id)).replace("__KEY__", key_q).replace("__REFRESH__", str(int(DASHBOARD_REFRESH_SEC_DEFAULT)))
    return HTMLResponse(html)


@app.get("/dashboard/executive", response_class=HTMLResponse)
async def dashboard_executive(
    _: None = Depends(require_dashboard_key),
    shop_id: str = Query(default=SHOP_ID),
    days: int = Query(default=30, ge=1, le=365),
    key: Optional[str] = Query(default=None),
):
    key_q = (key or "").strip()
    html = EXEC_HTML
    html = html.replace("__SHOP__", str(shop_id)).replace("__KEY__", key_q).replace("__DAYS__", str(int(days)))
    return HTMLResponse(html)


@app.get("/dashboard/customer", response_class=HTMLResponse)
async def dashboard_customer(
    _: None = Depends(require_dashboard_key),
    shop_id: str = Query(default=SHOP_ID),
    conv_key: str = Query(...),
    key: Optional[str] = Query(default=None),
):
    key_q = (key or "").strip()
    html = CUSTOMER_HTML
    html = html.replace("__SHOP__", str(shop_id)).replace("__KEY__", key_q).replace("__CONV__", str(conv_key))
    return HTMLResponse(html)


# ============================================================
# LINE webhook
# ============================================================

@app.post("/line/webhook")
async def line_webhook(
    request: Request,
    background: BackgroundTasks,
    x_line_signature: str = Header(default="", alias="X-Line-Signature"),
):
    body = await request.body()
    if not verify_signature(body, x_line_signature):
        raise HTTPException(status_code=401, detail="invalid signature")

    payload = json.loads(body.decode("utf-8"))
    events = payload.get("events", []) or []

    for ev in events:
        if ev.get("type") != "message":
            continue
        message = ev.get("message", {}) or {}
        if message.get("type") != "text":
            continue

        user_id = (ev.get("source") or {}).get("userId", "unknown")
        reply_token = ev.get("replyToken", "")
        user_text = (message.get("text") or "").strip()
        if not user_text:
            continue

        conv_key = "user:" + str(user_id)
        ensure_customer_row(SHOP_ID, conv_key, user_id)

        # save inbound
        try:
            save_message(SHOP_ID, conv_key, "user", user_text)
        except Exception as e:
            print("[DB] save user:", repr(e))

        # attribution + clear need
        try:
            attribute_followup_response(SHOP_ID, conv_key)
        except Exception:
            pass
        try:
            set_need_reply(SHOP_ID, conv_key, False, "user_replied")
        except Exception:
            pass

        # learn pref hour
        try:
            update_customer_pref_hour(SHOP_ID, conv_key)
        except Exception:
            pass

        # revive lost
        try:
            revive_if_lost_by_keywords(SHOP_ID, conv_key, user_text)
        except Exception:
            pass

        # stop if inactive
        if is_inactive(SHOP_ID, conv_key):
            await reply_line(reply_token, "承知しました。必要になったらいつでもご連絡ください。")
            continue

        # optout/lost
        for pat in OPTOUT_PATTERNS:
            if re.search(pat, user_text, flags=re.IGNORECASE):
                mark_opt_out(SHOP_ID, conv_key, user_id)
                await reply_line(reply_token, "承知しました。今後こちらからのご連絡は停止します。")
                return {"ok": True}

        for pat in CANCEL_PATTERNS:
            if re.search(pat, user_text):
                mark_lost(SHOP_ID, conv_key)
                await reply_line(reply_token, "承知しました。必要になったらまたいつでもご連絡ください。")
                return {"ok": True}

        # visit change
        if is_visit_change_request(user_text):
            set_visit_slot(SHOP_ID, conv_key, "REQUEST_CHANGE")
            await reply_line(reply_token, "承知しました。ご希望の曜日や時間帯（例：平日夜/土日午後など）を教えてください。")
            continue

        # visit selection
        sel = parse_slot_selection(user_text)
        if sel is not None:
            slots = upcoming_visit_slots_jst(VISIT_DAYS_AHEAD)
            if 1 <= sel <= len(slots):
                picked = slots[sel - 1]
                set_visit_slot(SHOP_ID, conv_key, picked)
                await reply_line(reply_token, "ありがとうございます！内見希望枠は「" + picked + "」で承りました。")
            else:
                await reply_line(reply_token, "番号は 1〜6 の範囲でお願いします。")
            continue

        # fast reply
        fast = ""
        try:
            fast = await asyncio.wait_for(generate_reply_only(user_id, user_text), timeout=FAST_REPLY_TIMEOUT_SEC)
        except Exception:
            fast = ""

        if fast:
            await reply_line(reply_token, fast)

            async def bg():
                lvl_raw, conf, intent, goal, reasons, override = await analyze_only(SHOP_ID, conv_key, user_text)
                lvl_stable = stable_from_history(conv_key, lvl_raw)

                if override == "OPTOUT":
                    mark_opt_out(SHOP_ID, conv_key, user_id)
                    return
                if override == "LOST":
                    mark_lost(SHOP_ID, conv_key)
                    return

                prev = get_customer_slots(SHOP_ID, conv_key)
                merged = merge_slots(prev, extract_slots(user_text))
                if merged and merged != prev:
                    set_customer_slots(SHOP_ID, conv_key, merged)

                upsert_customer_state(SHOP_ID, conv_key, user_id, user_text, lvl_raw, lvl_stable, conf, intent, goal)
                save_message(SHOP_ID, conv_key, "assistant", fast, lvl_raw, lvl_stable, conf, intent, goal)

                flags = get_customer_flags(SHOP_ID, conv_key)
                need, reason = compute_need_reply(goal, flags, assistant_text=fast)
                set_need_reply(SHOP_ID, conv_key, need, reason)

            background.add_task(bg)
        else:
            await reply_line(reply_token, "ありがとうございます！内容を確認しています。少々お待ちください😊")

    return {"ok": True}
