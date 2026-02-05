# app.py (FULL REWRITE)
# Features included:
# - Fast reply (no "確認中" unless AI slow/fail)
# - Temp scoring w/ hard rules (cancel/optout/short text)
# - Followup A/B + visit slot proposals + slot selection (1-6)
# - Followup attribution (responded_at)
# - NEW(1): no-response second touch (stage=2) after 48h
# - NEW(2): status automation (ACTIVE/COLD/LOST/OPTOUT)
# - NEW(3): slot completeness meter (budget/area/move_in/layout)
#
# Auth:
# - Dashboard: DASHBOARD_KEY (?key= or header X-Dashboard-Key)
# - Jobs: ADMIN_API_KEY (header x-admin-key)

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
FOLLOWUP_DRYRUN = os.getenv("FOLLOWUP_DRYRUN", "0").strip() == "1"
FOLLOWUP_LOCK_TTL_SEC = int(os.getenv("FOLLOWUP_LOCK_TTL_SEC", "180"))
FOLLOWUP_MIN_HOURS_BETWEEN = int(os.getenv("FOLLOWUP_MIN_HOURS_BETWEEN", "24"))
FOLLOWUP_JST_FROM = int(os.getenv("FOLLOWUP_JST_FROM", "10"))
FOLLOWUP_JST_TO = int(os.getenv("FOLLOWUP_JST_TO", "20"))

FOLLOWUP_AB_ENABLED = os.getenv("FOLLOWUP_AB_ENABLED", "1").strip() == "1"
VISIT_DAYS_AHEAD = int(os.getenv("VISIT_DAYS_AHEAD", "3"))
VISIT_SLOT_HOURS = os.getenv("VISIT_SLOT_HOURS", "11,14,17").strip()
FOLLOWUP_ATTRIBUTION_WINDOW_HOURS = int(os.getenv("FOLLOWUP_ATTRIBUTION_WINDOW_HOURS", "72"))

# NEW: second touch timing
FOLLOWUP_SECOND_TOUCH_AFTER_HOURS = int(os.getenv("FOLLOWUP_SECOND_TOUCH_AFTER_HOURS", "48"))
FOLLOWUP_SECOND_TOUCH_LIMIT = int(os.getenv("FOLLOWUP_SECOND_TOUCH_LIMIT", "50"))

CHAT_HISTORY: Dict[str, deque] = defaultdict(lambda: deque(maxlen=40))
TEMP_HISTORY: Dict[str, deque] = defaultdict(lambda: deque(maxlen=5))

JST = timezone(timedelta(hours=9))

app = FastAPI(title="linebot_mvp", version="0.7.0")


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


# ============================================================
# Auth
# ============================================================

def require_dashboard_key(
    x_dashboard_key: Optional[str] = Header(default=None, alias="X-Dashboard-Key"),
    key: Optional[str] = Query(default=None),
) -> None:
    expected = (DASHBOARD_KEY or "").strip()
    if not expected:
        raise HTTPException(status_code=500, detail="DASHBOARD_KEY is not configured")
    provided = (x_dashboard_key or key or "").strip()
    if not provided or not secrets.compare_digest(provided, expected):
        raise HTTPException(status_code=401, detail="Unauthorized")


def require_admin_key(x_admin_key: Optional[str] = Header(default=None, alias="x-admin-key")) -> None:
    if not ADMIN_API_KEY:
        raise HTTPException(status_code=500, detail="ADMIN_API_KEY is not configured")
    if not x_admin_key or not secrets.compare_digest(x_admin_key.strip(), ADMIN_API_KEY):
        raise HTTPException(status_code=401, detail="Unauthorized")


# ============================================================
# DB
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

    if "?" in rest:
        rest, query = rest.split("?", 1)
        params = dict([kv.split("=", 1) for kv in query.split("&") if "=" in kv])
    else:
        params = {}

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
    sslmode = (cfg["params"].get("sslmode", "") or "").lower()
    use_ssl = sslmode in ("require", "verify-full", "verify-ca") or True
    ssl_context = create_db_ssl_context(verify=verify_ssl) if use_ssl else None

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
        print("[DB] SSL verify failed, fallback to disable:", repr(e))
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


def ensure_tables_and_columns() -> None:
    if not DATABASE_URL:
        return

    conn = _connect_db_with_fallback()
    cur = conn.cursor()

    # customers
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
    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS user_id TEXT;""")
    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS last_user_text TEXT;""")
    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS temp_level_raw INT;""")
    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS temp_level_stable INT;""")
    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS confidence REAL;""")
    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS next_goal TEXT;""")

    # NEW(2): status automation
    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS status TEXT;""")  # ACTIVE/COLD/LOST/OPTOUT

    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS opt_out BOOLEAN;""")
    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS opt_out_at TIMESTAMPTZ;""")

    # visit slot selection
    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS visit_slot_selected TEXT;""")
    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS visit_slot_selected_at TIMESTAMPTZ;""")

    # NEW(3): slot fields
    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS slot_budget TEXT;""")
    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS slot_area TEXT;""")
    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS slot_move_in TEXT;""")
    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS slot_layout TEXT;""")
    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS slots_json TEXT;""")

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
    cur.execute("""ALTER TABLE messages ADD COLUMN IF NOT EXISTS temp_level_raw INT;""")
    cur.execute("""ALTER TABLE messages ADD COLUMN IF NOT EXISTS temp_level_stable INT;""")
    cur.execute("""ALTER TABLE messages ADD COLUMN IF NOT EXISTS confidence REAL;""")
    cur.execute("""ALTER TABLE messages ADD COLUMN IF NOT EXISTS next_goal TEXT;""")

    # job locks
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS job_locks (
          key TEXT PRIMARY KEY,
          locked_until TIMESTAMPTZ NOT NULL
        );
        """
    )

    # followup logs (A/B + attribution + stage)
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
    cur.execute("""ALTER TABLE followup_logs ADD COLUMN IF NOT EXISTS variant TEXT;""")
    cur.execute("""ALTER TABLE followup_logs ADD COLUMN IF NOT EXISTS responded_at TIMESTAMPTZ;""")
    cur.execute("""ALTER TABLE followup_logs ADD COLUMN IF NOT EXISTS stage INT;""")  # 1 or 2

    # indexes
    cur.execute("""CREATE INDEX IF NOT EXISTS idx_customers_shop_updated ON customers(shop_id, updated_at DESC);""")
    cur.execute("""CREATE INDEX IF NOT EXISTS idx_messages_conv_created ON messages(conv_key, created_at DESC);""")
    cur.execute("""CREATE INDEX IF NOT EXISTS idx_followup_shop_conv_created ON followup_logs(shop_id, conv_key, created_at DESC);""")
    cur.execute("""CREATE INDEX IF NOT EXISTS idx_followup_stage_created ON followup_logs(stage, created_at DESC);""")

    cur.close()
    conn.close()


@app.on_event("startup")
async def on_startup():
    ensure_tables_and_columns()
    print("[BOOT] tables/columns ensured")


# ============================================================
# LINE signature verify
# ============================================================

def verify_signature(body: bytes, signature: str) -> bool:
    if not LINE_CHANNEL_SECRET:
        return False
    mac = hmac.new(LINE_CHANNEL_SECRET.encode("utf-8"), body, hashlib.sha256).digest()
    expected = base64.b64encode(mac).decode("utf-8")
    return hmac.compare_digest(expected, signature or "")


# ============================================================
# LINE send
# ============================================================

async def reply_message(reply_token: str, text: str) -> None:
    if not LINE_CHANNEL_ACCESS_TOKEN or not reply_token:
        return
    url = "https://api.line.me/v2/bot/message/reply"
    headers = {"Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}", "Content-Type": "application/json"}
    payload = {"replyToken": reply_token, "messages": [{"type": "text", "text": (text or "")[:4900]}]}
    try:
        async with httpx.AsyncClient(timeout=10, verify=certifi.where()) as client:
            r = await client.post(url, headers=headers, json=payload)
            if r.status_code >= 400:
                print("[LINE] reply failed:", r.status_code, r.text[:300])
    except Exception as e:
        print("[LINE] reply exception:", repr(e))


async def push_message(user_id: str, text: str) -> None:
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
                print("[LINE] push failed:", r.status_code, r.text[:300])
    except Exception as e:
        print("[LINE] push exception:", repr(e))


# ============================================================
# OpenAI
# ============================================================

OPENAI_API_URL = "https://api.openai.com/v1/chat/completions"

SYSTEM_PROMPT_ANALYZE = """
あなたは不動産仲介SaaSの「顧客温度判定AI」です。
会話履歴と最新発言から、成約に近い順に 1〜10 で温度を判定します。

【出力はJSONのみ（それ以外禁止）】
{
  "temp_level_raw": 1,
  "confidence": 0.50,
  "next_goal": "短い日本語",
  "reasons": ["根拠1","根拠2","根拠3"]
}

【重要：レベル基準（厳守）】
Lv10: 申込/審査/契約の話が明確、または内見日程が具体的に確定
Lv9 : 内見したい＋日程調整に入っている（候補日が出ている等）
Lv8 : 条件がほぼ確定（エリア/予算/入居時期が揃う）＋内見意思が強い
Lv7 : 条件がかなり具体（エリアor沿線、予算、入居時期のうち2つ以上）＋前向きな質問
Lv6 : 条件が一部具体（上のうち1つ）＋検討継続が明確
Lv5 : 一般質問中心、条件が曖昧、温度不明
Lv4 : 情報収集段階が明確（比較中/とりあえず）で条件未確定
Lv3 : 反応が薄い/曖昧/先すぎる（半年以上先など）/冷めている
Lv2 : 冷やかし/雑談/要件なし/関係ない
Lv1 : 明確に不要、拒否、ブロック示唆

【過大評価防止（最重要）】
- 「内見」「良さそう」等があっても、予算・入居時期・エリアが不明なら Lv8以上にしない
- 入居時期が半年以上先なら最大でも Lv6
- 条件が全く出ていない場合は最大でも Lv5
- 返信が短い/曖昧な場合はLvを上げすぎない
"""

SYSTEM_PROMPT_ASSISTANT = """
あなたは不動産仲介の優秀な営業アシスタントです。
ユーザーに対して丁寧で簡潔、次の行動につながる返信を日本語で作ってください。
・質問は最大2つ
・押し売り感を出さない
"""


async def openai_chat(messages: List[Dict[str, str]], temperature: float = 0.2, timeout_sec: float = 25.0) -> str:
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is missing")
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    payload = {"model": "gpt-4o-mini", "messages": messages, "temperature": temperature}
    async with httpx.AsyncClient(timeout=timeout_sec, verify=certifi.where()) as client:
        r = await client.post(OPENAI_API_URL, headers=headers, json=payload)
        r.raise_for_status()
        data = r.json()
        return data["choices"][0]["message"]["content"]


def coerce_level(v: Any) -> int:
    try:
        iv = int(float(str(v).strip()))
    except Exception:
        return 5
    return max(1, min(10, iv))


def coerce_confidence(v: Any) -> float:
    if v is None:
        return 0.6
    if isinstance(v, (int, float)):
        fv = float(v)
        if 1.0 < fv <= 100.0:
            fv = fv / 100.0
        return max(0.0, min(1.0, fv))
    s = str(v).strip().lower()
    if s.endswith("%"):
        try:
            return max(0.0, min(1.0, float(s[:-1]) / 100.0))
        except Exception:
            return 0.6
    try:
        fv = float(s)
        if 1.0 < fv <= 100.0:
            fv = fv / 100.0
        return max(0.0, min(1.0, fv))
    except Exception:
        return 0.6


# ============================================================
# Slot extraction (NEW 3)
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


# ============================================================
# Customer state helpers
# ============================================================

def get_customer_slots(shop_id: str, conv_key: str) -> Dict[str, str]:
    if not DATABASE_URL:
        return {}
    rows = db_fetchall(
        "SELECT slots_json FROM customers WHERE shop_id=%s AND conv_key=%s",
        (shop_id, conv_key),
    )
    if not rows or not rows[0][0]:
        return {}
    try:
        return json.loads(rows[0][0])
    except Exception:
        return {}


def set_customer_slots(shop_id: str, conv_key: str, slots: Dict[str, str]) -> None:
    if not DATABASE_URL:
        return
    sj = json.dumps(slots, ensure_ascii=False)
    db_execute(
        """
        UPDATE customers
        SET slot_budget=%s, slot_area=%s, slot_move_in=%s, slot_layout=%s, slots_json=%s, updated_at=now()
        WHERE shop_id=%s AND conv_key=%s
        """,
        (
            slots.get("budget"),
            slots.get("area"),
            slots.get("move_in"),
            slots.get("layout"),
            sj,
            shop_id,
            conv_key,
        ),
    )


def ensure_customer_row(shop_id: str, conv_key: str, user_id: str) -> None:
    if not DATABASE_URL:
        return
    db_execute(
        """
        INSERT INTO customers (shop_id, conv_key, user_id, updated_at, status)
        VALUES (%s, %s, %s, now(), 'ACTIVE')
        ON CONFLICT (shop_id, conv_key)
        DO UPDATE SET user_id = EXCLUDED.user_id, updated_at = now()
        """,
        (shop_id, conv_key, user_id),
    )


def mark_opt_out(shop_id: str, conv_key: str, user_id: str) -> None:
    if not DATABASE_URL:
        return
    db_execute(
        """
        UPDATE customers
        SET opt_out=TRUE, opt_out_at=now(), status='OPTOUT', user_id=%s, updated_at=now()
        WHERE shop_id=%s AND conv_key=%s
        """,
        (user_id, shop_id, conv_key),
    )


def mark_lost(shop_id: str, conv_key: str) -> None:
    if not DATABASE_URL:
        return
    db_execute(
        "UPDATE customers SET status='LOST', updated_at=now() WHERE shop_id=%s AND conv_key=%s",
        (shop_id, conv_key),
    )


def is_opted_out_or_inactive(shop_id: str, conv_key: str) -> bool:
    if not DATABASE_URL:
        return False
    rows = db_fetchall(
        "SELECT COALESCE(opt_out,FALSE), COALESCE(status,'ACTIVE') FROM customers WHERE shop_id=%s AND conv_key=%s",
        (shop_id, conv_key),
    )
    if not rows:
        return False
    opt_out = bool(rows[0][0])
    status_v = (rows[0][1] or "ACTIVE").upper()
    return opt_out or status_v in ("COLD", "LOST", "OPTOUT")


def set_visit_slot(shop_id: str, conv_key: str, slot_text: str) -> None:
    if not DATABASE_URL:
        return
    db_execute(
        """
        UPDATE customers
        SET visit_slot_selected=%s, visit_slot_selected_at=now(), updated_at=now()
        WHERE shop_id=%s AND conv_key=%s
        """,
        (slot_text, shop_id, conv_key),
    )


# ============================================================
# Messages + scoring persistence
# ============================================================

def save_message(
    shop_id: str,
    conv_key: str,
    role: str,
    content: str,
    temp_level_raw: Optional[int] = None,
    temp_level_stable: Optional[int] = None,
    confidence: Optional[float] = None,
    next_goal: Optional[str] = None,
) -> None:
    if not DATABASE_URL:
        return
    db_execute(
        """
        INSERT INTO messages (shop_id, conv_key, role, content, temp_level_raw, temp_level_stable, confidence, next_goal)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (shop_id, conv_key, role, content, temp_level_raw, temp_level_stable, confidence, (next_goal[:120] if next_goal else None)),
    )


def stable_from_history(conv_key: str, raw_level: int) -> int:
    hist = TEMP_HISTORY[conv_key]
    hist.append(raw_level)
    s = sorted(hist)
    return s[len(s) // 2]


def upsert_customer_state(
    shop_id: str,
    conv_key: str,
    user_id: str,
    last_user_text: str,
    raw_level: int,
    stable_level: int,
    confidence: float,
    next_goal: str,
) -> None:
    if not DATABASE_URL:
        return
    db_execute(
        """
        INSERT INTO customers
          (shop_id, conv_key, user_id, last_user_text, temp_level_raw, temp_level_stable, confidence, next_goal, updated_at, status)
        VALUES
          (%s, %s, %s, %s, %s, %s, %s, %s, now(), COALESCE((SELECT status FROM customers WHERE shop_id=%s AND conv_key=%s),'ACTIVE'))
        ON CONFLICT (shop_id, conv_key)
        DO UPDATE SET
          user_id = EXCLUDED.user_id,
          last_user_text = EXCLUDED.last_user_text,
          temp_level_raw = EXCLUDED.temp_level_raw,
          temp_level_stable = EXCLUDED.temp_level_stable,
          confidence = EXCLUDED.confidence,
          next_goal = EXCLUDED.next_goal,
          updated_at = now()
        """,
        (shop_id, conv_key, user_id, last_user_text, raw_level, stable_level, confidence, next_goal, shop_id, conv_key),
    )


# ============================================================
# Followup logs + attribution
# ============================================================

def save_followup_log(
    shop_id: str,
    conv_key: str,
    user_id: str,
    message: str,
    mode: str,
    status: str,
    error: Optional[str] = None,
    variant: Optional[str] = None,
    stage: Optional[int] = None,
) -> None:
    if not DATABASE_URL:
        return
    db_execute(
        """
        INSERT INTO followup_logs (shop_id, conv_key, user_id, message, mode, status, error, variant, stage)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (shop_id, conv_key, user_id, message, mode, status, (error or None), (variant or None), (stage or None)),
    )


def attribute_followup_response(shop_id: str, conv_key: str) -> None:
    if not DATABASE_URL:
        return
    window_since = datetime.now(timezone.utc) - timedelta(hours=FOLLOWUP_ATTRIBUTION_WINDOW_HOURS)
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
# A/B + visit slots (templates)
# ============================================================

def pick_ab_variant(conv_key: str) -> str:
    if not FOLLOWUP_AB_ENABLED:
        return "A"
    h = hashlib.sha256(conv_key.encode("utf-8")).hexdigest()
    return "A" if (int(h[:2], 16) % 2 == 0) else "B"


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
    now = datetime.now(JST)
    slots: List[str] = []
    for d in range(1, max(1, min(14, days_ahead)) + 1):
        label = "明日" if d == 1 else ("明後日" if d == 2 else f"{d}日後")
        for h in hours:
            slots.append(f"{label} {h:02d}:00-{(h+1)%24:02d}:00")
    return slots[:6]


def build_followup_template_ab(variant: str, next_goal: str, last_user_text: str, level: int) -> str:
    goal = (next_goal or "").strip()
    last = (last_user_text or "").strip()

    is_visit = any(k in goal for k in ["内見", "候補日", "日程"])
    slots = upcoming_visit_slots_jst(VISIT_DAYS_AHEAD) if is_visit else []
    slot_lines = ""
    if slots:
        slot_lines = "候補：\n" + "\n".join([f"{i+1}. {s}" for i, s in enumerate(slots)]) + "\n"

    if is_visit:
        q = "上の候補で合う番号（1〜6）を返信ください。別日/別時間ならそのまま書いてOKです。"
    elif "予算" in goal:
        q = "ご予算の上限だけ教えていただけますか？"
    elif "入居" in goal or "時期" in goal:
        q = "ご入居希望はいつ頃ですか？"
    elif "エリア" in goal or "沿線" in goal:
        q = "希望エリア（沿線/駅）はどのあたりが良いですか？"
    else:
        q = "条件を少し整理したいので、希望があれば教えてください。"

    trimmed = last[:40] + ("…" if len(last) > 40 else "") if last else ""

    if variant == "A":
        lead = "その後いかがでしょうか？"
        body = f"{lead}\n{('（直近：'+trimmed+'）\\n') if trimmed else ''}{slot_lines}{q}\n必要なら候補をすぐまとめます😊"
        return body.strip()

    lead = "少しだけ確認です。"
    yn = "①番号でOK ②別日希望 ③一旦ストップ" if is_visit else "①このまま探す ②一旦ストップ ③条件変更"
    body = f"{lead}\n{slot_lines}{q}\n返信は「{yn}」のどれでもOKです。"
    return body.strip()


def build_second_touch_message(next_goal: str) -> str:
    # NEW(1): no-response second touch (soft)
    if any(k in (next_goal or "") for k in ["内見", "候補日", "日程"]):
        return (
            "その後いかがでしょうか？\n"
            "ご都合が合う時で大丈夫なので、内見希望なら「1〜6」か「別日」とだけ返信ください。"
        )
    return (
        "その後いかがでしょうか？\n"
        "急ぎでなければ大丈夫です。必要になったら一言だけ返信ください🙂"
    )


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
    t = text or ""
    return any(re.search(p, t) for p in VISIT_CHANGE_PATTERNS)


# ============================================================
# Scoring + reply
# ============================================================

async def analyze_only(shop_id: str, conv_key: str, user_text: str) -> Tuple[int, float, str, List[str], Dict[str, str], str]:
    """
    return: raw_level, conf, next_goal, reasons, merged_slots, status_override
    status_override: "" | "LOST" | "OPTOUT"
    """
    t = (user_text or "").strip()

    # opt-out
    for pat in OPTOUT_PATTERNS:
        if re.search(pat, t, flags=re.IGNORECASE):
            return 1, 0.95, "関係終了確認", ["配信停止/連絡不要の意思"], {}, "OPTOUT"

    # cancel -> lost
    for pat in CANCEL_PATTERNS:
        if re.search(pat, t):
            return 2, 0.90, "関係終了確認", ["キャンセル/拒否の明確表現"], {}, "LOST"

    # short
    if len(t) <= SHORT_TEXT_MAX_LEN:
        return 3, 0.75, "要件確認", ["短文で情報不足"], {}, ""

    # slot merge
    new_slots = extract_slots(user_text)
    prev_slots = get_customer_slots(shop_id, conv_key)
    merged = merge_slots(prev_slots, new_slots)

    # build analysis messages
    history = get_recent_conversation(shop_id, conv_key, ANALYZE_HISTORY_LIMIT)
    if not history or history[-1].get("content") != user_text:
        history.append({"role": "user", "content": user_text})

    messages: List[Dict[str, str]] = [{"role": "system", "content": SYSTEM_PROMPT_ANALYZE}]
    if merged:
        messages.append({"role": "user", "content": f"抽出スロット(参考): {json.dumps(merged, ensure_ascii=False)}"})
    messages.extend(history[-max(2, ANALYZE_HISTORY_LIMIT):])

    raw_level = 5
    conf = 0.6
    next_goal = "要件確認"
    reasons: List[str] = []

    try:
        raw_json_text = await openai_chat(messages, temperature=0.0, timeout_sec=18.0)
        raw = raw_json_text.strip()
        if raw.startswith("```"):
            parts = raw.split("```")
            raw = parts[1] if len(parts) > 1 else raw
        try:
            j = json.loads(raw)
        except Exception:
            start = raw.find("{")
            end = raw.rfind("}")
            j = json.loads(raw[start:end + 1])

        raw_level = coerce_level(j.get("temp_level_raw", 5))
        conf = coerce_confidence(j.get("confidence", 0.6))
        next_goal = str(j.get("next_goal", "要件確認")).strip()[:80]
        rs = j.get("reasons", [])
        if isinstance(rs, list):
            reasons = [str(x).strip()[:60] for x in rs if str(x).strip()][:3]
    except Exception as e:
        print("[OPENAI] analyze_only failed:", repr(e))

    return raw_level, conf, next_goal, reasons, merged, ""


def get_recent_conversation(shop_id: str, conv_key: str, limit: int) -> List[Dict[str, str]]:
    if not DATABASE_URL:
        return []
    rows = db_fetchall(
        """
        SELECT role, content
        FROM messages
        WHERE shop_id=%s AND conv_key=%s AND role IN ('user','assistant')
        ORDER BY created_at DESC
        LIMIT %s
        """,
        (shop_id, conv_key, max(1, min(30, int(limit)))),
    )
    rows = list(reversed(rows))
    return [{"role": r[0], "content": (r[1] or "")[:1200]} for r in rows]


async def generate_reply_only(user_id: str, user_text: str) -> str:
    history = CHAT_HISTORY[user_id]
    context_msgs = [{"role": "system", "content": SYSTEM_PROMPT_ASSISTANT}]
    for role, content in list(history)[-10:]:
        context_msgs.append({"role": role, "content": content})
    context_msgs.append({"role": "user", "content": user_text})

    reply_text = await openai_chat(context_msgs, temperature=0.35, timeout_sec=FAST_REPLY_TIMEOUT_SEC)
    reply_text = (reply_text or "").strip()
    if not reply_text:
        reply_text = "ありがとうございます。条件をもう少し教えてください（エリア/予算/間取り/入居時期など）。"

    history.append(("user", user_text))
    history.append(("assistant", reply_text))
    return reply_text


# ============================================================
# Status automation (NEW 2)
# ============================================================

def maintenance_update_statuses(shop_id: str) -> Dict[str, int]:
    """
    - opt_out -> OPTOUT
    - Lv<=2 and updated_at < now-7days -> COLD (unless already LOST/OPTOUT)
    """
    if not DATABASE_URL:
        return {"optout": 0, "cold": 0}

    # optout status
    db_execute(
        """
        UPDATE customers
        SET status='OPTOUT'
        WHERE shop_id=%s AND COALESCE(opt_out,FALSE)=TRUE AND COALESCE(status,'') <> 'OPTOUT'
        """,
        (shop_id,),
    )

    # cold rule
    db_execute(
        """
        UPDATE customers
        SET status='COLD'
        WHERE shop_id=%s
          AND COALESCE(status,'ACTIVE')='ACTIVE'
          AND COALESCE(temp_level_stable,0) <= 2
          AND updated_at < (now() - interval '7 days')
        """,
        (shop_id,),
    )

    # counts (rough)
    rows = db_fetchall(
        "SELECT COUNT(*) FROM customers WHERE shop_id=%s AND COALESCE(status,'')='OPTOUT'",
        (shop_id,),
    )
    optc = int(rows[0][0]) if rows else 0
    rows = db_fetchall(
        "SELECT COUNT(*) FROM customers WHERE shop_id=%s AND COALESCE(status,'')='COLD'",
        (shop_id,),
    )
    coldc = int(rows[0][0]) if rows else 0
    return {"optout": optc, "cold": coldc}


# ============================================================
# Followup job helpers
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


def acquire_job_lock(key: str, ttl_sec: int) -> bool:
    now = utcnow()
    until = now + timedelta(seconds=ttl_sec)

    rows = db_fetchall("SELECT locked_until FROM job_locks WHERE key=%s", (key,))
    if rows:
        locked_until = rows[0][0]
        if locked_until and locked_until > now:
            return False

    db_execute(
        """
        INSERT INTO job_locks (key, locked_until)
        VALUES (%s, %s)
        ON CONFLICT (key)
        DO UPDATE SET locked_until = EXCLUDED.locked_until
        """,
        (key, until),
    )
    return True


def get_followup_candidates_stage1() -> List[Dict[str, Any]]:
    if not DATABASE_URL:
        return []
    threshold = utcnow() - timedelta(minutes=FOLLOWUP_AFTER_MINUTES)

    rows = db_fetchall(
        """
        SELECT c.shop_id, c.conv_key, c.user_id, COALESCE(c.temp_level_stable,0), COALESCE(c.next_goal,''), c.updated_at, COALESCE(c.last_user_text,''), COALESCE(c.status,'ACTIVE'), COALESCE(c.opt_out,FALSE)
        FROM customers c
        WHERE c.shop_id=%s
          AND COALESCE(c.temp_level_stable,0) >= %s
          AND c.updated_at < %s
          AND COALESCE(c.user_id,'') <> ''
        ORDER BY c.updated_at ASC
        LIMIT %s
        """,
        (SHOP_ID, FOLLOWUP_MIN_LEVEL, threshold, FOLLOWUP_LIMIT),
    )

    out: List[Dict[str, Any]] = []
    for r in rows:
        status_v = (r[7] or "ACTIVE").upper()
        if bool(r[8]) or status_v in ("COLD", "LOST", "OPTOUT"):
            continue
        out.append({
            "shop_id": r[0],
            "conv_key": r[1],
            "user_id": r[2],
            "level": int(r[3] or 0),
            "next_goal": r[4],
            "updated_at": r[5].isoformat() if r[5] else None,
            "last_user_text": r[6],
        })
    return out


def get_followup_candidates_stage2() -> List[Dict[str, Any]]:
    """
    NEW(1): stage=1 sent, responded_at NULL, older than X hours, no stage=2 sent yet
    """
    if not DATABASE_URL:
        return []
    threshold = utcnow() - timedelta(hours=FOLLOWUP_SECOND_TOUCH_AFTER_HOURS)

    rows = db_fetchall(
        """
        SELECT fl.conv_key, fl.user_id
        FROM followup_logs fl
        WHERE fl.shop_id=%s
          AND fl.status='sent'
          AND COALESCE(fl.stage,1)=1
          AND fl.responded_at IS NULL
          AND fl.created_at < %s
          AND NOT EXISTS (
            SELECT 1 FROM followup_logs fl2
            WHERE fl2.shop_id=fl.shop_id
              AND fl2.conv_key=fl.conv_key
              AND fl2.status='sent'
              AND COALESCE(fl2.stage,1)=2
          )
        ORDER BY fl.created_at ASC
        LIMIT %s
        """,
        (SHOP_ID, threshold, FOLLOWUP_SECOND_TOUCH_LIMIT),
    )

    out: List[Dict[str, Any]] = []
    for conv_key, user_id in rows:
        # exclude inactive
        if is_opted_out_or_inactive(SHOP_ID, conv_key):
            continue
        # get next_goal from customers
        cg = db_fetchall("SELECT COALESCE(next_goal,''), COALESCE(temp_level_stable,0) FROM customers WHERE shop_id=%s AND conv_key=%s", (SHOP_ID, conv_key))
        next_goal = cg[0][0] if cg else ""
        level = int(cg[0][1] or 0) if cg else 0
        out.append({"conv_key": conv_key, "user_id": user_id, "next_goal": next_goal, "level": level})
    return out


# ============================================================
# Background tasks
# ============================================================

async def process_analysis_only_store(shop_id: str, user_id: str, conv_key: str, user_text: str, reply_text: str) -> None:
    try:
        raw_level, conf, next_goal, reasons, merged_slots, status_override = await analyze_only(shop_id, conv_key, user_text)
        stable_level = stable_from_history(conv_key, raw_level)

        # status overrides
        if status_override == "OPTOUT":
            mark_opt_out(shop_id, conv_key, user_id)
        elif status_override == "LOST":
            mark_lost(shop_id, conv_key)

        # persist slots (NEW 3)
        if merged_slots:
            try:
                set_customer_slots(shop_id, conv_key, merged_slots)
            except Exception as e:
                print("[DB] set_customer_slots failed:", repr(e))

        upsert_customer_state(shop_id, conv_key, user_id, user_text, raw_level, stable_level, conf, next_goal)
        save_message(shop_id, conv_key, "assistant", reply_text, raw_level, stable_level, conf, next_goal)

        if reasons:
            print(f"[TEMP] {conv_key} raw={raw_level} stable={stable_level} conf={conf:.2f} goal={next_goal} reasons={reasons}")
    except Exception as e:
        print("[BG] process_analysis_only_store exception:", repr(e))


async def process_ai_and_push_full(shop_id: str, user_id: str, conv_key: str, user_text: str) -> None:
    try:
        raw_level, conf, next_goal, reasons, merged_slots, status_override = await analyze_only(shop_id, conv_key, user_text)
        stable_level = stable_from_history(conv_key, raw_level)

        if status_override == "OPTOUT":
            mark_opt_out(shop_id, conv_key, user_id)
        elif status_override == "LOST":
            mark_lost(shop_id, conv_key)

        if merged_slots:
            try:
                set_customer_slots(shop_id, conv_key, merged_slots)
            except Exception as e:
                print("[DB] set_customer_slots failed:", repr(e))

        try:
            reply_text = await openai_chat(
                [{"role": "system", "content": SYSTEM_PROMPT_ASSISTANT}, {"role": "user", "content": user_text}],
                temperature=0.35,
                timeout_sec=20.0,
            )
            reply_text = (reply_text or "").strip() or "ありがとうございます。条件をもう少し教えてください（エリア/予算/間取り/入居時期など）。"
        except Exception:
            reply_text = "ありがとうございます。条件をもう少し教えてください（エリア/予算/間取り/入居時期など）。"

        upsert_customer_state(shop_id, conv_key, user_id, user_text, raw_level, stable_level, conf, next_goal)
        save_message(shop_id, conv_key, "assistant", reply_text, raw_level, stable_level, conf, next_goal)

        if reasons:
            print(f"[TEMP] {conv_key} raw={raw_level} stable={stable_level} conf={conf:.2f} goal={next_goal} reasons={reasons}")

        if is_opted_out_or_inactive(shop_id, conv_key):
            return

        await push_message(user_id, reply_text)
    except Exception as e:
        print("[BG] process_ai_and_push_full exception:", repr(e))


# ============================================================
# Routes
# ============================================================

@app.get("/")
async def root():
    return {"ok": True}


@app.get("/healthz")
async def healthz():
    return {"ok": True, "ts": int(datetime.now(timezone.utc).timestamp())}


@app.post("/line/webhook")
async def line_webhook(
    request: Request,
    background: BackgroundTasks,
    x_line_signature: str = Header(default="", alias="X-Line-Signature"),
):
    body = await request.body()
    if not verify_signature(body, x_line_signature):
        raise HTTPException(status_code=401, detail="invalid signature")

    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception:
        raise HTTPException(status_code=400, detail="invalid json")

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

        conv_key = f"user:{user_id}"

        # ensure customer row
        try:
            ensure_customer_row(SHOP_ID, conv_key, user_id)
        except Exception as e:
            print("[DB] ensure_customer_row failed:", repr(e))

        # save user message
        try:
            save_message(SHOP_ID, conv_key, "user", user_text)
        except Exception as e:
            print("[DB] save user failed:", repr(e))

        # attribution: user reply after followup
        try:
            attribute_followup_response(SHOP_ID, conv_key)
        except Exception as e:
            print("[DB] attribute_followup_response failed:", repr(e))

        # slot update (NEW 3)
        try:
            prev = get_customer_slots(SHOP_ID, conv_key)
            merged = merge_slots(prev, extract_slots(user_text))
            if merged != prev and merged:
                set_customer_slots(SHOP_ID, conv_key, merged)
        except Exception as e:
            print("[DB] slot merge failed:", repr(e))

        # visit slot selection
        if is_visit_change_request(user_text):
            try:
                set_visit_slot(SHOP_ID, conv_key, "REQUEST_CHANGE")
            except Exception as e:
                print("[DB] set_visit_slot(change) failed:", repr(e))
            await reply_message(reply_token, "承知しました。ご希望の曜日や時間帯（例：平日夜/土日午後など）を教えてください。")
            continue

        sel = parse_slot_selection(user_text)
        if sel is not None:
            slots = upcoming_visit_slots_jst(VISIT_DAYS_AHEAD)
            if 1 <= sel <= len(slots):
                picked = slots[sel - 1]
                try:
                    set_visit_slot(SHOP_ID, conv_key, picked)
                except Exception as e:
                    print("[DB] set_visit_slot(pick) failed:", repr(e))
                await reply_message(reply_token, f"ありがとうございます！内見希望枠は「{picked}」で承りました。")
                continue
            await reply_message(reply_token, "番号は 1〜6 の範囲でお願いします。")
            continue

        # opt-out immediate
        for pat in OPTOUT_PATTERNS:
            if re.search(pat, user_text, flags=re.IGNORECASE):
                try:
                    mark_opt_out(SHOP_ID, conv_key, user_id)
                except Exception as e:
                    print("[DB] mark_opt_out failed:", repr(e))
                await reply_message(reply_token, "承知しました。今後こちらからのご連絡は停止します。")
                return {"ok": True}

        # cancel immediate -> lost (NEW 2)
        for pat in CANCEL_PATTERNS:
            if re.search(pat, user_text):
                try:
                    mark_lost(SHOP_ID, conv_key)
                except Exception:
                    pass
                # 返信は短く
                await reply_message(reply_token, "承知しました。必要になったらまたいつでもご連絡ください。")
                return {"ok": True}

        # Fast AI reply (no "確認中" unless fail/slow)
        fast_reply_text: Optional[str] = None
        try:
            fast_reply_text = await asyncio.wait_for(
                generate_reply_only(user_id=user_id, user_text=user_text),
                timeout=FAST_REPLY_TIMEOUT_SEC,
            )
        except Exception as e:
            print("[FAST_REPLY] failed/timeout:", repr(e))
            fast_reply_text = None

        if fast_reply_text:
            await reply_message(reply_token, fast_reply_text)
            background.add_task(process_analysis_only_store, SHOP_ID, user_id, conv_key, user_text, fast_reply_text)
        else:
            await reply_message(reply_token, "ありがとうございます！内容を確認しています。少々お待ちください😊")
            background.add_task(process_ai_and_push_full, SHOP_ID, user_id, conv_key, user_text)

    return {"ok": True}


# ============================================================
# Dashboard API
# ============================================================

@app.get("/api/hot")
async def api_hot(
    _: None = Depends(require_dashboard_key),
    shop_id: str = Query(default=SHOP_ID),
    min_level: int = Query(default=1, ge=1, le=10),
    limit: int = Query(default=50, ge=1, le=200),
    view: str = Query(default="customers", pattern="^(customers|events|followups|ab_stats)$"),
):
    if not DATABASE_URL:
        return JSONResponse([], status_code=200)

    if view == "customers":
        rows = db_fetchall(
            """
            SELECT conv_key, user_id, last_user_text, temp_level_stable, confidence, next_goal, updated_at,
                   COALESCE(opt_out, FALSE) as opt_out,
                   COALESCE(status,'ACTIVE') as status,
                   visit_slot_selected,
                   slot_budget, slot_area, slot_move_in, slot_layout
            FROM customers
            WHERE shop_id = %s AND COALESCE(temp_level_stable, 0) >= %s
            ORDER BY updated_at DESC
            LIMIT %s
            """,
            (shop_id, min_level, limit),
        )
        return JSONResponse([
            {
                "view": "customers",
                "conv_key": r[0],
                "user_id": r[1],
                "message": r[2],
                "temp_level_stable": r[3],
                "confidence": float(r[4]) if r[4] is not None else None,
                "next_goal": r[5],
                "ts": r[6].isoformat() if r[6] else None,
                "opt_out": bool(r[7]),
                "status": r[8],
                "visit_slot_selected": r[9],
                "slot_budget": r[10],
                "slot_area": r[11],
                "slot_move_in": r[12],
                "slot_layout": r[13],
            }
            for r in rows
        ])

    if view == "events":
        rows = db_fetchall(
            """
            SELECT m.role, c.user_id, m.content, m.created_at, m.temp_level_stable, m.confidence, m.next_goal
            FROM messages m
            LEFT JOIN customers c ON c.shop_id=m.shop_id AND c.conv_key=m.conv_key
            WHERE m.shop_id=%s
            ORDER BY m.created_at DESC
            LIMIT %s
            """,
            (shop_id, limit),
        )
        return JSONResponse([
            {
                "view": "events",
                "role": r[0],
                "user_id": r[1],
                "message": r[2],
                "ts": r[3].isoformat() if r[3] else None,
                "temp_level_stable": r[4],
                "confidence": float(r[5]) if r[5] is not None else None,
                "next_goal": r[6],
            }
            for r in rows
        ])

    if view == "followups":
        rows = db_fetchall(
            """
            SELECT user_id, conv_key, variant, mode, status, stage, message, error, responded_at, created_at
            FROM followup_logs
            WHERE shop_id=%s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (shop_id, limit),
        )
        return JSONResponse([
            {
                "view": "followups",
                "user_id": r[0],
                "conv_key": r[1],
                "variant": r[2],
                "mode": r[3],
                "status": r[4],
                "stage": r[5],
                "message": r[6],
                "error": r[7],
                "responded_at": r[8].isoformat() if r[8] else None,
                "ts": r[9].isoformat() if r[9] else None,
            }
            for r in rows
        ])

    # ab_stats
    rows = db_fetchall(
        """
        SELECT
          COALESCE(variant,'A') as variant,
          COUNT(*) FILTER (WHERE status='sent') as sent_count,
          COUNT(*) FILTER (WHERE status='sent' AND responded_at IS NOT NULL) as responded_count
        FROM followup_logs
        WHERE shop_id=%s
        GROUP BY COALESCE(variant,'A')
        ORDER BY variant
        """,
        (shop_id,),
    )
    out = []
    for v, sent, resp in rows:
        sent = int(sent or 0)
        resp = int(resp or 0)
        rate = (resp / sent) if sent > 0 else 0.0
        out.append({"view": "ab_stats", "variant": v, "sent": sent, "responded": resp, "rate": rate})
    return JSONResponse(out)


# ============================================================
# Dashboard (HTML)
# ============================================================

LEVEL_COLORS = {
    1: "#9aa0a6", 2: "#8ab4f8", 3: "#a7ffeb", 4: "#c6ff00", 5: "#ffd54f",
    6: "#ffab91", 7: "#ff8a80", 8: "#ff5252", 9: "#e040fb", 10: "#7c4dff",
}

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(
    _: None = Depends(require_dashboard_key),
    shop_id: str = Query(default=SHOP_ID),
    view: str = Query(default="customers", pattern="^(customers|events|followups|ab_stats)$"),
    min_level: int = Query(default=1, ge=1, le=10),
    limit: int = Query(default=50, ge=1, le=200),
    refresh: int = Query(default=DASHBOARD_REFRESH_SEC_DEFAULT, ge=0, le=300),
    key: Optional[str] = Query(default=None),
):
    key_q = (key or "").strip()
    api_url = f"/api/hot?shop_id={shop_id}&min_level={min_level}&limit={limit}&view={view}&key={key_q}"

    html = f"""
<!doctype html><html lang="ja"><head>
<meta charset="utf-8" /><meta name="viewport" content="width=device-width, initial-scale=1" />
<title>HOT顧客</title>
<style>
body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Noto Sans JP",sans-serif;background:#fafafa;margin:0;padding:16px;color:#111}}
.wrap{{max-width:1240px;margin:0 auto}}
.card{{background:#fff;border-radius:14px;box-shadow:0 1px 6px rgba(0,0,0,.08);padding:14px 16px;margin-bottom:14px}}
.title{{display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap}}
.meta{{font-size:12px;color:#555}}
.filters{{display:grid;grid-template-columns:1fr 220px 120px 120px 140px;gap:10px;align-items:end}}
@media(max-width:920px){{.filters{{grid-template-columns:1fr 1fr}}}}
label{{font-size:12px;color:#444;display:block;margin-bottom:4px}}
input,select{{width:100%;padding:10px;border:1px solid #ddd;border-radius:10px;font-size:14px;background:#fff}}
button{{padding:10px 12px;border:0;border-radius:10px;background:#111;color:#fff;font-size:14px;cursor:pointer}}
table{{width:100%;border-collapse:collapse;font-size:14px}}
th,td{{padding:10px 8px;border-bottom:1px solid #eee;vertical-align:top}}
th{{text-align:left;font-size:12px;color:#666;font-weight:600}}
.pill{{display:inline-flex;align-items:center;gap:6px;padding:4px 10px;border-radius:999px;font-size:12px;color:#fff;font-weight:700;white-space:nowrap}}
.badge{{display:inline-flex;align-items:center;padding:3px 10px;border-radius:999px;font-size:12px;font-weight:700;white-space:nowrap;border:1px solid #e6e6e6;background:#f7f7f7;color:#333}}
.badge.assistant{{background:#111;color:#fff;border-color:#111}}
.badge.ok{{background:#0b8043;color:#fff;border-color:#0b8043}}
.badge.ng{{background:#b00020;color:#fff;border-color:#b00020}}
.mono{{font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas;font-size:12px}}
.rowmsg{{max-width:760px;word-break:break-word;white-space:pre-wrap}}
.link{{color:#0b57d0;text-decoration:none}}
.link:hover{{text-decoration:underline}}
.search{{display:flex;gap:10px;align-items:center;margin-top:10px}}
.search input{{flex:1}}
.muted{{color:#777;font-size:12px}}
.optout{{color:#b00020;font-weight:700}}
.slot{{display:inline-flex;gap:6px;flex-wrap:wrap}}
.tag{{padding:2px 8px;border-radius:999px;border:1px solid #e6e6e6;background:#f7f7f7;font-size:12px}}
.tag.ok{{background:#0b8043;color:#fff;border-color:#0b8043}}
.tag.ng{{background:#b00020;color:#fff;border-color:#b00020}}
</style></head><body>
<div class="wrap">
  <div class="card title">
    <div>
      <div style="font-size:18px;font-weight:700">HOT顧客 <span class="mono">{shop_id}</span></div>
      <div class="meta">view={view} / min_level={min_level} / limit={limit} / count=<span id="count">-</span> / <span id="now">-</span></div>
      <div class="meta">JSON: <a class="link" href="{api_url}" target="_blank">{api_url}</a></div>
    </div>
    <div><button id="btnRefresh">更新</button></div>
  </div>

  <div class="card">
    <div class="filters">
      <div><label>shop_id</label><input id="shopId" value="{shop_id}" /></div>
      <div>
        <label>view</label>
        <select id="viewSelect">
          <option value="customers" {"selected" if view=="customers" else ""}>customers（最新）</option>
          <option value="events" {"selected" if view=="events" else ""}>events（履歴）</option>
          <option value="followups" {"selected" if view=="followups" else ""}>followups（追客ログ）</option>
          <option value="ab_stats" {"selected" if view=="ab_stats" else ""}>ab_stats（A/B集計）</option>
        </select>
      </div>
      <div><label>min_level</label><input id="minLevel" type="number" min="1" max="10" value="{min_level}" /></div>
      <div><label>limit</label><input id="limit" type="number" min="1" max="200" value="{limit}" /></div>
      <div><label>auto refresh (sec)</label><input id="refreshSec" type="number" min="0" max="300" value="{refresh}" /></div>
    </div>
    <div class="search">
      <div style="flex:1">
        <label>検索（自動） user_id / next_goal / message / status / variant</label>
        <input id="searchBox" placeholder="例: 内見 / failed / A / COLD / 渋谷" />
        <div class="muted">入力すると即フィルタ（サーバー再読込なし）</div>
      </div>
      <div><label>&nbsp;</label><button id="btnApply">反映</button></div>
    </div>
  </div>

  <div class="card">
    <table>
      <thead id="thead"></thead>
      <tbody id="tbody"><tr><td class="muted">Loading...</td></tr></tbody>
    </table>
  </div>
</div>

<script>
const LEVEL_COLORS = {json.dumps(LEVEL_COLORS)};
const DASH_KEY = {json.dumps(key_q)};
let cache = [];

function escapeHtml(s){{return (s||"").replace(/[&<>"]/g,c=>({{"&":"&amp;","<":"&lt;",">":"&gt;","\\"":"&quot;"}}[c]));}}
function fmtTime(iso){{if(!iso) return "-"; try{{return new Date(iso).toLocaleString();}}catch(e){{return iso;}}}}
function pill(level){{const c=LEVEL_COLORS[level]||"#999"; return `<span class="pill" style="background:${{c}}">Lv${{level}}</span>`;}}
function badge(role){{const r=(role||"").toLowerCase(); if(r==="assistant")return `<span class="badge assistant">assistant</span>`; if(r==="user")return `<span class="badge">user</span>`; return `<span class="badge">${{escapeHtml(role||"-")}}</span>`;}}
function badgeStatus(s){{const v=(s||"").toLowerCase(); if(v==="sent")return `<span class="badge ok">sent</span>`; if(v==="failed")return `<span class="badge ng">failed</span>`; return `<span class="badge">${{escapeHtml(s||"-")}}</span>`;}}
function matchesSearch(row,q){{if(!q) return true; q=q.toLowerCase(); const fields=[row.user_id,row.next_goal,row.message,row.role,row.status,row.mode,row.error,row.variant,row.visit_slot_selected,row.slot_budget,row.slot_area,row.slot_move_in,row.slot_layout].map(x=>(x||"").toLowerCase()); return fields.some(f=>f.includes(q));}}

function setHeader(view){{
  const thead=document.getElementById("thead");
  if(view==="customers") {{
    thead.innerHTML=`<tr><th>更新</th><th>温度</th><th>確度</th><th>status</th><th>条件</th><th>次のゴール</th><th class="mono">user_id</th><th>直近メッセ</th><th>内見枠</th></tr>`;
    return;
  }}
  if(view==="followups") {{
    thead.innerHTML=`<tr><th>更新</th><th>status</th><th>stage</th><th>variant</th><th class="mono">user_id</th><th>本文</th><th>返信</th><th>error</th></tr>`;
    return;
  }}
  if(view==="ab_stats") {{
    thead.innerHTML=`<tr><th>variant</th><th>sent</th><th>responded</th><th>rate</th></tr>`;
    return;
  }}
  thead.innerHTML=`<tr><th>更新</th><th>種別</th><th>温度</th><th>確度</th><th>次のゴール</th><th class="mono">user_id</th><th>メッセージ</th></tr>`;
}}

function slotTags(r){{
  const items = [
    ["予算", r.slot_budget],
    ["エリア", r.slot_area],
    ["時期", r.slot_move_in],
    ["間取り", r.slot_layout],
  ];
  return `<span class="slot">` + items.map(([k,v]) => {{
    const ok = (v!=null && v!=="");
    return `<span class="tag ${{ok?"ok":"ng"}}">${{k}}${{ok?"✓":"✗"}}</span>`;
  }}).join("") + `</span>`;
}}

function render(){{
  const view=document.getElementById("viewSelect").value;
  setHeader(view);
  const q=document.getElementById("searchBox").value.trim().toLowerCase();
  const rows=cache.filter(r=>matchesSearch(r,q));
  document.getElementById("count").textContent=rows.length;
  const tbody=document.getElementById("tbody");
  if(!rows.length){{tbody.innerHTML=`<tr><td colspan="9" class="muted">No data</td></tr>`;return;}}

  if(view==="ab_stats") {{
    tbody.innerHTML = rows.map(r=>`<tr><td class="mono">${{escapeHtml(r.variant)}}</td><td class="mono">${{r.sent}}</td><td class="mono">${{r.responded}}</td><td class="mono">${{(r.rate*100).toFixed(1)}}%</td></tr>`).join("");
    return;
  }}

  if(view==="followups") {{
    tbody.innerHTML = rows.map(r=>`<tr>
      <td class="mono">${{escapeHtml(fmtTime(r.ts))}}</td>
      <td>${{badgeStatus(r.status)}}</td>
      <td class="mono">${{escapeHtml(String(r.stage ?? "-"))}}</td>
      <td class="mono">${{escapeHtml(r.variant||"-")}}</td>
      <td class="mono">${{escapeHtml(r.user_id||"")}}</td>
      <td class="rowmsg">${{escapeHtml(r.message||"")}}</td>
      <td class="mono">${{r.responded_at ? "YES" : "-"}}</td>
      <td class="rowmsg mono">${{escapeHtml(r.error||"-")}}</td>
    </tr>`).join("");
    return;
  }}

  if(view==="customers") {{
    tbody.innerHTML = rows.map(r=>{{
      const levelHtml=(r.temp_level_stable==null)?"-":pill(r.temp_level_stable);
      const confHtml=(r.confidence==null)?"-":Number(r.confidence).toFixed(2);
      const goalHtml=(r.next_goal==null||r.next_goal==="")?"-":escapeHtml(r.next_goal);
      const st = escapeHtml(r.status || "ACTIVE");
      const slot = slotTags(r);
      const visit = r.visit_slot_selected ? escapeHtml(r.visit_slot_selected) : "-";
      return `<tr>
        <td class="mono">${{escapeHtml(fmtTime(r.ts))}}</td>
        <td>${{levelHtml}}</td>
        <td class="mono">${{confHtml}}</td>
        <td class="mono">${{st}}</td>
        <td>${{slot}}</td>
        <td>${{goalHtml}}</td>
        <td class="mono">${{escapeHtml(r.user_id||"")}}</td>
        <td class="rowmsg">${{escapeHtml(r.message||"")}}</td>
        <td class="mono">${{visit}}</td>
      </tr>`;
    }}).join("");
    return;
  }}

  tbody.innerHTML = rows.map(r=>{{
    const levelHtml=(r.temp_level_stable==null)?"-":pill(r.temp_level_stable);
    const confHtml=(r.confidence==null)?"-":Number(r.confidence).toFixed(2);
    const goalHtml=(r.next_goal==null||r.next_goal==="")?"-":escapeHtml(r.next_goal);
    return `<tr>
      <td class="mono">${{escapeHtml(fmtTime(r.ts))}}</td>
      <td>${{badge(r.role)}}</td>
      <td>${{levelHtml}}</td>
      <td class="mono">${{confHtml}}</td>
      <td>${{goalHtml}}</td>
      <td class="mono">${{escapeHtml(r.user_id||"")}}</td>
      <td class="rowmsg">${{escapeHtml(r.message||"")}}</td>
    </tr>`;
  }}).join("");
}}

async function fetchData(){{
  const shopId=document.getElementById("shopId").value.trim();
  const view=document.getElementById("viewSelect").value;
  const minLevel=document.getElementById("minLevel").value;
  const limit=document.getElementById("limit").value;
  const url=`/api/hot?shop_id=${{encodeURIComponent(shopId)}}&min_level=${{encodeURIComponent(minLevel)}}&limit=${{encodeURIComponent(limit)}}&view=${{encodeURIComponent(view)}}&key=${{encodeURIComponent(DASH_KEY)}}`;
  const res=await fetch(url, {{credentials:"same-origin"}});
  const data=await res.json();
  cache=Array.isArray(data)?data:[];
  document.getElementById("now").textContent=new Date().toLocaleString();
  render();
}}

document.getElementById("btnApply").addEventListener("click",()=>{{
  const shopId=document.getElementById("shopId").value.trim();
  const view=document.getElementById("viewSelect").value;
  const minLevel=document.getElementById("minLevel").value;
  const limit=document.getElementById("limit").value;
  const refreshSec=document.getElementById("refreshSec").value;
  const qs=new URLSearchParams({{shop_id:shopId,view:view,min_level:minLevel,limit:limit,refresh:refreshSec,key:DASH_KEY}});
  window.location.href=`/dashboard?${{qs.toString()}}`;
}});

document.getElementById("btnRefresh").addEventListener("click",fetchData);
document.getElementById("searchBox").addEventListener("input",()=>render());

let timer=null;
function setupAutoRefresh(){{
  if(timer) clearInterval(timer);
  const sec=parseInt(document.getElementById("refreshSec").value||"0",10);
  if(sec>0) timer=setInterval(fetchData, sec*1000);
}}
document.getElementById("refreshSec").addEventListener("change",setupAutoRefresh);

fetchData().then(setupAutoRefresh).catch(e=>{{
  console.error(e);
  document.getElementById("tbody").innerHTML=`<tr><td class="muted">Error loading</td></tr>`;
}});
</script>
</body></html>
"""
    return HTMLResponse(html)


# ============================================================
# Jobs
# ============================================================

@app.post("/jobs/maintenance")
async def job_maintenance(_: None = Depends(require_admin_key)):
    res = maintenance_update_statuses(SHOP_ID)
    return {"ok": True, "result": res}


@app.post("/jobs/followup")
async def job_followup(_: None = Depends(require_admin_key)):
    if not FOLLOWUP_ENABLED:
        return {"ok": True, "enabled": False, "reason": "FOLLOWUP_ENABLED!=1"}

    # update statuses before selecting
    maintenance_update_statuses(SHOP_ID)

    if not is_within_jst_window():
        return {"ok": True, "enabled": True, "skipped": True, "reason": f"out_of_time_window (JST {FOLLOWUP_JST_FROM}-{FOLLOWUP_JST_TO})"}

    if not acquire_job_lock("followup", FOLLOWUP_LOCK_TTL_SEC):
        return {"ok": True, "enabled": True, "skipped": True, "reason": "locked"}

    # stage 1
    stage1 = get_followup_candidates_stage1()
    # stage 2 (NEW 1)
    stage2 = get_followup_candidates_stage2()

    if FOLLOWUP_DRYRUN:
        return {"ok": True, "enabled": True, "dryrun": True, "stage1": stage1[:50], "stage2": stage2[:50]}

    sent1 = 0
    sent2 = 0
    failed = 0

    # send stage1
    for c in stage1:
        user_id = c["user_id"]
        conv_key = c["conv_key"]
        level = int(c["level"] or 0)
        goal = c.get("next_goal") or ""
        last_text = c.get("last_user_text") or ""

        if is_opted_out_or_inactive(SHOP_ID, conv_key):
            save_followup_log(SHOP_ID, conv_key, user_id, "(skipped inactive)", "template", "skipped", "inactive", variant=None, stage=1)
            continue

        variant = pick_ab_variant(conv_key)
        msg = build_followup_template_ab(variant, goal, last_text, level)

        try:
            await push_message(user_id, msg)
            save_followup_log(SHOP_ID, conv_key, user_id, msg, "template", "sent", None, variant=variant, stage=1)
            sent1 += 1
        except Exception as e:
            err = str(e)[:200]
            save_followup_log(SHOP_ID, conv_key, user_id, msg, "template", "failed", err, variant=variant, stage=1)
            failed += 1

    # send stage2
    for c in stage2:
        user_id = c["user_id"]
        conv_key = c["conv_key"]
        goal = c.get("next_goal") or ""

        if is_opted_out_or_inactive(SHOP_ID, conv_key):
            save_followup_log(SHOP_ID, conv_key, user_id, "(skipped inactive)", "template", "skipped", "inactive", variant=None, stage=2)
            continue

        variant = pick_ab_variant(conv_key)
        msg = build_second_touch_message(goal)

        try:
            await push_message(user_id, msg)
            save_followup_log(SHOP_ID, conv_key, user_id, msg, "template", "sent", None, variant=variant, stage=2)
            sent2 += 1
        except Exception as e:
            err = str(e)[:200]
            save_followup_log(SHOP_ID, conv_key, user_id, msg, "template", "failed", err, variant=variant, stage=2)
            failed += 1

    return {"ok": True, "enabled": True, "sent_stage1": sent1, "sent_stage2": sent2, "failed": failed}


@app.post("/jobs/push_test")
async def job_push_test(
    _: None = Depends(require_admin_key),
    user_id: str = Query(..., description="LINE userId"),
    text: str = Query(..., description="message text"),
):
    await push_message(user_id, text)
    return {"ok": True}
