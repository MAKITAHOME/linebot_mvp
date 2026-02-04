# app.py (FULL REWRITE)
# FastAPI + Render + Postgres(pg8000)
#
# LINE webhook:
#  - Try FAST AI reply within short timeout (no "確認中" message)
#  - If AI is slow/failing: send fallback "確認中" ONLY then, and do background AI + push
#
# Dashboard auth: DASHBOARD_KEY (query ?key=... or header X-Dashboard-Key)
# Jobs auth: ADMIN_API_KEY (header x-admin-key)
#
# Required env:
#   LINE_CHANNEL_SECRET
#   LINE_CHANNEL_ACCESS_TOKEN
#   DATABASE_URL
#   OPENAI_API_KEY
#   DASHBOARD_KEY
#   ADMIN_API_KEY
#
# Optional:
#   SHOP_ID
#   DASHBOARD_REFRESH_SEC_DEFAULT
#   FAST_REPLY_TIMEOUT_SEC (default 3.0)
#   ANALYZE_HISTORY_LIMIT (default 10)
#
# Followup env (recommended):
#   FOLLOWUP_ENABLED=1
#   FOLLOWUP_AFTER_MINUTES=180
#   FOLLOWUP_MIN_LEVEL=8
#   FOLLOWUP_LIMIT=50
#   FOLLOWUP_DRYRUN=1
#   FOLLOWUP_LOCK_TTL_SEC=180
#   FOLLOWUP_MIN_HOURS_BETWEEN=24
#   FOLLOWUP_JST_FROM=10
#   FOLLOWUP_JST_TO=20
#   FOLLOWUP_USE_OPENAI=0

import os
import json
import hmac
import hashlib
import base64
import time
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

from fastapi import FastAPI, Request, Header, HTTPException, Query, Depends, status, BackgroundTasks
from fastapi.responses import JSONResponse, HTMLResponse


# ============================================================
# Config / Environment
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
FOLLOWUP_AFTER_MINUTES = int(os.getenv("FOLLOWUP_AFTER_MINUTES", "60"))
FOLLOWUP_MIN_LEVEL = int(os.getenv("FOLLOWUP_MIN_LEVEL", "8"))
FOLLOWUP_LIMIT = int(os.getenv("FOLLOWUP_LIMIT", "50"))
FOLLOWUP_DRYRUN = os.getenv("FOLLOWUP_DRYRUN", "0").strip() == "1"
FOLLOWUP_LOCK_TTL_SEC = int(os.getenv("FOLLOWUP_LOCK_TTL_SEC", "180"))
FOLLOWUP_MIN_HOURS_BETWEEN = int(os.getenv("FOLLOWUP_MIN_HOURS_BETWEEN", "24"))
FOLLOWUP_JST_FROM = int(os.getenv("FOLLOWUP_JST_FROM", "10"))
FOLLOWUP_JST_TO = int(os.getenv("FOLLOWUP_JST_TO", "20"))
FOLLOWUP_USE_OPENAI = os.getenv("FOLLOWUP_USE_OPENAI", "0").strip() == "1"

CHAT_HISTORY: Dict[str, deque] = defaultdict(lambda: deque(maxlen=40))
TEMP_HISTORY: Dict[str, deque] = defaultdict(lambda: deque(maxlen=5))

JST = timezone(timedelta(hours=9))

app = FastAPI(title="linebot_mvp", version="0.5.0")


# ============================================================
# HARD RULE patterns
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

# opt-out（今後連絡しない）判定：より強い
OPTOUT_PATTERNS = [
    r"連絡(不要|いらない)|もう連絡(しないで|いりません)",
    r"配信停止|停止して|ブロックする",
    r"\bstop\b|\bunsubscribe\b",
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
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="DASHBOARD_KEY is not configured",
        )
    provided = (x_dashboard_key or key or "").strip()
    if not provided or not secrets.compare_digest(provided, expected):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")


def require_admin_key(x_admin_key: Optional[str] = Header(default=None, alias="x-admin-key")) -> None:
    if not ADMIN_API_KEY:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="ADMIN_API_KEY is not configured",
        )
    if not x_admin_key or not secrets.compare_digest(x_admin_key.strip(), ADMIN_API_KEY):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")


# ============================================================
# DB (pg8000)
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

    return {
        "user": user,
        "password": password,
        "host": host,
        "port": port,
        "database": database,
        "params": params,
    }


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
    # ★opt-out
    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS opt_out BOOLEAN;""")
    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS opt_out_at TIMESTAMPTZ;""")

    # messages
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS messages (
          id BIGSERIAL PRIMARY KEY,
          shop_id TEXT NOT NULL,
          conv_key TEXT NOT NULL,
          role TEXT NOT NULL,          -- user|assistant|system
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

    # followup logs
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS followup_logs (
          id BIGSERIAL PRIMARY KEY,
          shop_id TEXT NOT NULL,
          conv_key TEXT NOT NULL,
          user_id TEXT NOT NULL,
          message TEXT NOT NULL,
          mode TEXT NOT NULL,              -- template|openai
          status TEXT NOT NULL,            -- sent|skipped|failed
          error TEXT,
          created_at TIMESTAMPTZ DEFAULT now()
        );
        """
    )

    # Indexes
    cur.execute("""CREATE INDEX IF NOT EXISTS idx_customers_shop_updated ON customers(shop_id, updated_at DESC);""")
    cur.execute("""CREATE INDEX IF NOT EXISTS idx_messages_conv_created ON messages(conv_key, created_at DESC);""")
    cur.execute("""CREATE INDEX IF NOT EXISTS idx_followup_shop_conv_created ON followup_logs(shop_id, conv_key, created_at DESC);""")

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
# LINE reply / push
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

【confidence】
- 根拠が2つ以上揃う → 0.70〜0.90
- 情報不足/どちらとも取れる → 0.40〜0.65
- ほぼ推測 → 0.30〜0.45

【next_goal】
次に営業が達成すべき「1ステップ」を短く。
例：予算確認 / 入居時期確認 / 希望エリア確認 / 内見候補日提示 / 申込意思確認

【reasons】
短い根拠を最大3つ
"""

SYSTEM_PROMPT_ASSISTANT = """
あなたは不動産仲介の優秀な営業アシスタントです。
ユーザーに対して丁寧で簡潔、次の行動につながる返信を日本語で作ってください。
・質問は最大2つ
・押し売り感を出さない
"""

SYSTEM_PROMPT_FOLLOWUP = """
あなたは不動産仲介の追客メッセージ作成AIです。
以下の情報をもとに、押し売り感ゼロで、返信しやすい一通を日本語で作ってください。
- 2〜4行、短め
- 質問は最大2つ
- 絵文字は最大1つ
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


def extract_slots(text: str) -> Dict[str, str]:
    t = text or ""
    slots: Dict[str, str] = {}

    m = re.search(r"(\d{1,3})(?:\.(\d))?\s*(?:万円|万)", t)
    if m:
        slots["budget"] = m.group(0)

    m = re.search(r"(\d)\s*(?:LDK|DK|K)|ワンルーム|1R", t, re.IGNORECASE)
    if m:
        slots["layout"] = m.group(0)

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


def stable_from_history(conv_key: str, raw_level: int) -> int:
    hist = TEMP_HISTORY[conv_key]
    hist.append(raw_level)
    s = sorted(hist)
    return s[len(s) // 2]


# ============================================================
# DB helpers
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
        (
            shop_id,
            conv_key,
            role,
            content,
            temp_level_raw,
            temp_level_stable,
            confidence,
            (next_goal[:120] if next_goal else None),
        ),
    )


def upsert_customer(
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
          (shop_id, conv_key, user_id, last_user_text, temp_level_raw, temp_level_stable, confidence, next_goal, updated_at)
        VALUES
          (%s, %s, %s, %s, %s, %s, %s, %s, now())
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
        (shop_id, conv_key, user_id, last_user_text, raw_level, stable_level, confidence, next_goal),
    )


def mark_opt_out(shop_id: str, conv_key: str, user_id: str) -> None:
    if not DATABASE_URL:
        return
    db_execute(
        """
        INSERT INTO customers (shop_id, conv_key, user_id, updated_at, opt_out, opt_out_at)
        VALUES (%s, %s, %s, now(), TRUE, now())
        ON CONFLICT (shop_id, conv_key)
        DO UPDATE SET opt_out = TRUE, opt_out_at = now(), user_id = EXCLUDED.user_id, updated_at = now()
        """,
        (shop_id, conv_key, user_id),
    )


def save_followup_log(
    shop_id: str,
    conv_key: str,
    user_id: str,
    message: str,
    mode: str,
    status: str,
    error: Optional[str] = None,
) -> None:
    if not DATABASE_URL:
        return
    db_execute(
        """
        INSERT INTO followup_logs (shop_id, conv_key, user_id, message, mode, status, error)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (shop_id, conv_key, user_id, message, mode, status, (error or None)),
    )


def is_opted_out(shop_id: str, conv_key: str) -> bool:
    if not DATABASE_URL:
        return False
    rows = db_fetchall(
        "SELECT COALESCE(opt_out, FALSE) FROM customers WHERE shop_id=%s AND conv_key=%s",
        (shop_id, conv_key),
    )
    if not rows:
        return False
    return bool(rows[0][0])


# ============================================================
# Scoring + Reply
# ============================================================

async def analyze_only(shop_id: str, conv_key: str, user_text: str) -> Tuple[int, float, str, List[str]]:
    t = (user_text or "").strip()

    # opt-out 語が来たら即 opt_out（温度も低く）
    for pat in OPTOUT_PATTERNS:
        if re.search(pat, t, flags=re.IGNORECASE):
            return 1, 0.95, "関係終了確認", ["配信停止/連絡不要の意思"]

    # キャンセル/拒否は強制低温度
    for pat in CANCEL_PATTERNS:
        if re.search(pat, t):
            return 2, 0.90, "関係終了確認", ["キャンセル/拒否の明確表現"]

    # 短文は上がらない
    if len(t) <= SHORT_TEXT_MAX_LEN:
        return 3, 0.75, "要件確認", ["短文で情報不足"]

    slots = extract_slots(user_text)

    history_msgs = get_recent_conversation(shop_id, conv_key, ANALYZE_HISTORY_LIMIT)
    if not history_msgs or history_msgs[-1].get("content") != user_text:
        history_msgs.append({"role": "user", "content": user_text})

    messages: List[Dict[str, str]] = [{"role": "system", "content": SYSTEM_PROMPT_ANALYZE}]
    if slots:
        messages.append({"role": "user", "content": f"抽出スロット(参考): {json.dumps(slots, ensure_ascii=False)}"})
    messages.extend(history_msgs[-max(2, ANALYZE_HISTORY_LIMIT):])

    raw_level = 5
    conf = 0.5
    next_goal = "情報収集を続ける"
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
        next_goal = str(j.get("next_goal", "情報収集を続ける")).strip()[:80]
        rs = j.get("reasons", [])
        if isinstance(rs, list):
            reasons = [str(x).strip()[:60] for x in rs if str(x).strip()][:3]
    except Exception as e:
        print("[OPENAI] analyze_only failed:", repr(e))

    return raw_level, conf, next_goal, reasons


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
# Followup helpers
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


def build_followup_template(next_goal: str, last_user_text: str, level: int) -> str:
    goal = (next_goal or "").strip()
    last = (last_user_text or "").strip()

    if "予算" in goal:
        q = "ご予算の目安（上限）だけ教えていただけますか？"
    elif "入居" in goal or "時期" in goal:
        q = "ご入居希望の時期はいつ頃を想定されていますか？"
    elif "エリア" in goal or "沿線" in goal:
        q = "希望エリア（沿線/駅）はどのあたりが良いですか？"
    elif "内見" in goal or "候補日" in goal:
        q = "もしご都合よければ、今週 or 来週で空いている時間帯はありますか？"
    else:
        q = "条件を少し整理したいので、希望があれば教えてください。"

    lead = "その後いかがでしょうか？" if level >= 8 else "以前の件、その後の状況だけ伺ってもいいですか？"
    last_hint = ""
    if last:
        trimmed = last[:40] + ("…" if len(last) > 40 else "")
        last_hint = f"（直近：{trimmed}）\n"

    msg = f"{lead}\n{last_hint}{q}\n必要なら条件に合う候補をすぐまとめます😊"
    return msg.strip()


async def build_followup_message_openai(next_goal: str, last_user_text: str, level: int) -> str:
    prompt = (
        f"温度レベル: {level}\n"
        f"next_goal: {next_goal}\n"
        f"直近メッセージ: {last_user_text}\n"
    )
    msgs = [
        {"role": "system", "content": SYSTEM_PROMPT_FOLLOWUP},
        {"role": "user", "content": prompt},
    ]
    try:
        out = await openai_chat(msgs, temperature=0.5, timeout_sec=15.0)
        out = (out or "").strip()
        if out.startswith("```"):
            parts = out.split("```")
            out = parts[1] if len(parts) > 1 else out
        return out[:600].strip() or build_followup_template(next_goal, last_user_text, level)
    except Exception as e:
        print("[OPENAI] followup gen failed:", repr(e))
        return build_followup_template(next_goal, last_user_text, level)


def get_followup_candidates() -> List[Dict[str, Any]]:
    if not DATABASE_URL:
        return []

    threshold = utcnow() - timedelta(minutes=FOLLOWUP_AFTER_MINUTES)
    since = utcnow() - timedelta(hours=FOLLOWUP_MIN_HOURS_BETWEEN)

    rows = db_fetchall(
        """
        SELECT
          c.shop_id,
          c.conv_key,
          c.user_id,
          COALESCE(c.temp_level_stable, 0) as lvl,
          COALESCE(c.confidence, 0) as conf,
          COALESCE(c.next_goal, '') as goal,
          c.updated_at,
          COALESCE(c.last_user_text, '') as last_text,
          COALESCE(c.opt_out, FALSE) as opt_out,
          (
            SELECT MAX(fl.created_at)
            FROM followup_logs fl
            WHERE fl.shop_id = c.shop_id
              AND fl.conv_key = c.conv_key
              AND fl.status = 'sent'
          ) AS last_followup_at
        FROM customers c
        WHERE c.shop_id = %s
          AND COALESCE(c.temp_level_stable, 0) >= %s
          AND c.updated_at < %s
          AND COALESCE(c.user_id, '') <> ''
        ORDER BY c.updated_at ASC
        LIMIT %s
        """,
        (SHOP_ID, FOLLOWUP_MIN_LEVEL, threshold, FOLLOWUP_LIMIT),
    )

    candidates: List[Dict[str, Any]] = []
    for r in rows:
        opt_out = bool(r[8])
        if opt_out:
            continue

        last_followup_at = r[9]
        if last_followup_at and last_followup_at > since:
            continue

        candidates.append(
            {
                "shop_id": r[0],
                "conv_key": r[1],
                "user_id": r[2],
                "temp_level_stable": int(r[3] or 0),
                "confidence": float(r[4] or 0.0),
                "next_goal": r[5],
                "updated_at": r[6].isoformat() if r[6] else None,
                "last_user_text": r[7],
                "last_followup_at": last_followup_at.isoformat() if last_followup_at else None,
            }
        )

    return candidates


# ============================================================
# Background tasks
# ============================================================

async def process_analysis_only_store(shop_id: str, user_id: str, conv_key: str, user_text: str, reply_text: str) -> None:
    try:
        raw_level, conf, next_goal, reasons = await analyze_only(shop_id, conv_key, user_text)
        stable_level = stable_from_history(conv_key, raw_level)

        # opt-out 判定が返ったら、DBに反映
        if raw_level == 1 and any("配信停止" in r or "連絡不要" in r for r in reasons):
            try:
                mark_opt_out(shop_id, conv_key, user_id)
            except Exception as e:
                print("[DB] mark_opt_out failed:", repr(e))

        try:
            upsert_customer(shop_id, conv_key, user_id, user_text, raw_level, stable_level, conf, next_goal)
        except Exception as e:
            print("[DB] upsert_customer failed:", repr(e))

        try:
            save_message(shop_id, conv_key, "assistant", reply_text,
                         temp_level_raw=raw_level, temp_level_stable=stable_level, confidence=conf, next_goal=next_goal)
        except Exception as e:
            print("[DB] save assistant failed:", repr(e))

        if reasons:
            print(f"[TEMP] {conv_key} raw={raw_level} stable={stable_level} conf={conf:.2f} goal={next_goal} reasons={reasons}")
    except Exception as e:
        print("[BG] process_analysis_only_store exception:", repr(e))


async def process_ai_and_push_full(shop_id: str, user_id: str, conv_key: str, user_text: str) -> None:
    try:
        raw_level, conf, next_goal, reasons = await analyze_only(shop_id, conv_key, user_text)
        stable_level = stable_from_history(conv_key, raw_level)

        # opt-out
        if raw_level == 1 and any("配信停止" in r or "連絡不要" in r for r in reasons):
            try:
                mark_opt_out(shop_id, conv_key, user_id)
            except Exception as e:
                print("[DB] mark_opt_out failed:", repr(e))

        # 通常返信（bg）
        try:
            reply_text = await openai_chat(
                [{"role": "system", "content": SYSTEM_PROMPT_ASSISTANT}, {"role": "user", "content": user_text}],
                temperature=0.35,
                timeout_sec=20.0,
            )
            reply_text = (reply_text or "").strip() or "ありがとうございます。条件をもう少し教えてください（エリア/予算/間取り/入居時期など）。"
        except Exception as e:
            print("[OPENAI] reply(bg) failed:", repr(e))
            reply_text = "ありがとうございます。条件をもう少し教えてください（エリア/予算/間取り/入居時期など）。"

        try:
            upsert_customer(shop_id, conv_key, user_id, user_text, raw_level, stable_level, conf, next_goal)
        except Exception as e:
            print("[DB] upsert_customer failed:", repr(e))

        try:
            save_message(shop_id, conv_key, "assistant", reply_text,
                         temp_level_raw=raw_level, temp_level_stable=stable_level, confidence=conf, next_goal=next_goal)
        except Exception as e:
            print("[DB] save assistant failed:", repr(e))

        if reasons:
            print(f"[TEMP] {conv_key} raw={raw_level} stable={stable_level} conf={conf:.2f} goal={next_goal} reasons={reasons}")

        # opt-out 直後は push しない（希望なら送ってもいいが安全側で止める）
        if is_opted_out(shop_id, conv_key):
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
    return {"ok": True, "ts": int(time.time())}


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

        # user message save
        try:
            save_message(SHOP_ID, conv_key, "user", user_text)
        except Exception as e:
            print("[DB] save user message failed:", repr(e))

        # 即opt-out（ユーザー体験優先）
        for pat in OPTOUT_PATTERNS:
            if re.search(pat, user_text, flags=re.IGNORECASE):
                try:
                    mark_opt_out(SHOP_ID, conv_key, user_id)
                except Exception as e:
                    print("[DB] mark_opt_out failed:", repr(e))
                await reply_message(reply_token, "承知しました。今後こちらからのご連絡は停止します。")
                return {"ok": True}

        # Try FAST AI reply (no "確認中"). If it fails -> fallback.
        fast_reply_text: Optional[str] = None
        try:
            fast_reply_text = await asyncio.wait_for(
                generate_reply_only(user_id=user_id, user_text=user_text),
                timeout=FAST_REPLY_TIMEOUT_SEC,
            )
        except Exception as e:
            print("[FAST_REPLY] failed or timeout:", repr(e))
            fast_reply_text = None

        if fast_reply_text:
            await reply_message(reply_token, fast_reply_text)
            background.add_task(process_analysis_only_store, SHOP_ID, user_id, conv_key, user_text, fast_reply_text)
        else:
            await reply_message(reply_token, "ありがとうございます！内容を確認しています。少々お待ちください😊")
            background.add_task(process_ai_and_push_full, SHOP_ID, user_id, conv_key, user_text)

    return {"ok": True}


# ============================================================
# API for dashboard
# ============================================================

@app.get("/api/hot")
async def api_hot(
    _: None = Depends(require_dashboard_key),
    shop_id: str = Query(default=SHOP_ID),
    min_level: int = Query(default=1, ge=1, le=10),
    limit: int = Query(default=50, ge=1, le=200),
    view: str = Query(default="events", pattern="^(customers|events|followups)$"),
):
    if not DATABASE_URL:
        return JSONResponse([], status_code=200)

    if view == "customers":
        rows = db_fetchall(
            """
            SELECT conv_key, user_id, last_user_text, temp_level_stable, confidence, next_goal, updated_at,
                   COALESCE(opt_out, FALSE) as opt_out
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
                "role": "state",
                "conv_key": r[0],
                "user_id": r[1],
                "message": r[2],
                "temp_level_stable": r[3],
                "confidence": float(r[4]) if r[4] is not None else None,
                "next_goal": r[5],
                "ts": r[6].isoformat() if r[6] else None,
                "opt_out": bool(r[7]),
            }
            for r in rows
        ])

    if view == "followups":
        rows = db_fetchall(
            """
            SELECT user_id, conv_key, mode, status, message, error, created_at
            FROM followup_logs
            WHERE shop_id = %s
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
                "mode": r[2],
                "status": r[3],
                "message": r[4],
                "error": r[5],
                "ts": r[6].isoformat() if r[6] else None,
            }
            for r in rows
        ])

    # events
    rows = db_fetchall(
        """
        SELECT
          m.role,
          c.user_id,
          m.content,
          m.created_at,
          m.temp_level_stable,
          m.confidence,
          m.next_goal
        FROM messages m
        LEFT JOIN customers c
          ON c.shop_id = m.shop_id AND c.conv_key = m.conv_key
        WHERE m.shop_id = %s
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


# ============================================================
# Dashboard
# ============================================================

LEVEL_COLORS = {
    1: "#9aa0a6",
    2: "#8ab4f8",
    3: "#a7ffeb",
    4: "#c6ff00",
    5: "#ffd54f",
    6: "#ffab91",
    7: "#ff8a80",
    8: "#ff5252",
    9: "#e040fb",
    10: "#7c4dff",
}


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(
    _: None = Depends(require_dashboard_key),
    shop_id: str = Query(default=SHOP_ID),
    view: str = Query(default="events", pattern="^(customers|events|followups)$"),
    min_level: int = Query(default=1, ge=1, le=10),
    limit: int = Query(default=50, ge=1, le=200),
    refresh: int = Query(default=DASHBOARD_REFRESH_SEC_DEFAULT, ge=0, le=300),
    key: Optional[str] = Query(default=None),
):
    key_q = (key or "").strip()
    api_url = f"/api/hot?shop_id={shop_id}&min_level={min_level}&limit={limit}&view={view}&key={key_q}"

    html = f"""
<!doctype html>
<html lang="ja">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>HOT顧客ダッシュボード</title>
<style>
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Noto Sans JP", sans-serif;
    background: #fafafa;
    margin: 0;
    padding: 16px;
    color: #111;
  }}
  .wrap {{ max-width: 1240px; margin: 0 auto; }}
  .card {{
    background: #fff;
    border-radius: 14px;
    box-shadow: 0 1px 6px rgba(0,0,0,.08);
    padding: 14px 16px;
    margin-bottom: 14px;
  }}
  .title {{
    display: flex; justify-content: space-between; align-items: center;
    gap: 12px; flex-wrap: wrap;
  }}
  .title h1 {{ font-size: 18px; margin: 0; }}
  .meta {{ font-size: 12px; color: #555; }}
  .filters {{
    display: grid;
    grid-template-columns: 1fr 220px 120px 120px 140px;
    gap: 10px;
    align-items: end;
  }}
  @media (max-width: 920px) {{ .filters {{ grid-template-columns: 1fr 1fr; }} }}
  label {{ font-size: 12px; color: #444; display: block; margin-bottom: 4px; }}
  input, select {{
    width: 100%;
    padding: 10px 10px;
    border: 1px solid #ddd;
    border-radius: 10px;
    font-size: 14px;
    background: #fff;
  }}
  button {{
    padding: 10px 12px;
    border: 0;
    border-radius: 10px;
    background: #111;
    color: #fff;
    font-size: 14px;
    cursor: pointer;
  }}
  table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
  th, td {{ padding: 10px 8px; border-bottom: 1px solid #eee; vertical-align: top; }}
  th {{ text-align: left; font-size: 12px; color: #666; font-weight: 600; }}
  .pill {{
    display: inline-flex; align-items: center; gap: 6px;
    padding: 4px 10px; border-radius: 999px;
    font-size: 12px; color: #fff; font-weight: 700; white-space: nowrap;
  }}
  .badge {{
    display: inline-flex; align-items: center;
    padding: 3px 10px; border-radius: 999px;
    font-size: 12px; font-weight: 700; white-space: nowrap;
    border: 1px solid #e6e6e6; background: #f7f7f7; color: #333;
  }}
  .badge.assistant {{ background: #111; color: #fff; border-color: #111; }}
  .badge.ok {{ background:#0b8043; color:#fff; border-color:#0b8043; }}
  .badge.ng {{ background:#b00020; color:#fff; border-color:#b00020; }}
  .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas; font-size: 12px; }}
  .muted {{ color: #777; font-size: 12px; }}
  .rowmsg {{ max-width: 720px; word-break: break-word; white-space: pre-wrap; }}
  .link {{ color: #0b57d0; text-decoration: none; }}
  .link:hover {{ text-decoration: underline; }}
  .search {{ display:flex; gap:10px; align-items: center; margin-top: 10px; }}
  .search input {{ flex: 1; }}
  .hint {{ font-size: 12px; color: #666; margin-top: 6px; }}
  .optout {{ color:#b00020; font-weight:700; }}
</style>
</head>
<body>
<div class="wrap">
  <div class="card title">
    <div>
      <h1>HOT顧客 <span class="mono">{shop_id}</span></h1>
      <div class="meta">
        view={view} / min_level={min_level} / limit={limit}
        / count=<span id="count">-</span>
        / <span id="now">-</span>
      </div>
      <div class="meta">
        JSON: <a class="link" href="{api_url}" target="_blank">{api_url}</a>
      </div>
    </div>
    <div><button id="btnRefresh">更新</button></div>
  </div>

  <div class="card">
    <div class="filters">
      <div>
        <label>shop_id</label>
        <input id="shopId" value="{shop_id}" />
      </div>
      <div>
        <label>view</label>
        <select id="viewSelect">
          <option value="events" {"selected" if view=="events" else ""}>events（履歴：user/assistant）</option>
          <option value="customers" {"selected" if view=="customers" else ""}>customers（最新状態）</option>
          <option value="followups" {"selected" if view=="followups" else ""}>followups（追客ログ）</option>
        </select>
      </div>
      <div>
        <label>min_level</label>
        <input id="minLevel" type="number" min="1" max="10" value="{min_level}" />
      </div>
      <div>
        <label>limit</label>
        <input id="limit" type="number" min="1" max="200" value="{limit}" />
      </div>
      <div>
        <label>auto refresh (sec)</label>
        <input id="refreshSec" type="number" min="0" max="300" value="{refresh}" />
      </div>
    </div>

    <div class="search">
      <div style="flex:1">
        <label>検索（自動） user_id / next_goal / message / status</label>
        <input id="searchBox" placeholder="例: 渋谷 / 3LDK / 内見 / Uxxxxxxxx / failed" />
        <div class="hint">入力すると即フィルタ（サーバー再読込なし）</div>
      </div>
      <div>
        <label>&nbsp;</label>
        <button id="btnApply">反映</button>
      </div>
    </div>
  </div>

  <div class="card">
    <table>
      <thead id="thead"></thead>
      <tbody id="tbody">
        <tr><td class="muted">Loading...</td></tr>
      </tbody>
    </table>
  </div>
</div>

<script>
  const LEVEL_COLORS = {json.dumps(LEVEL_COLORS)};
  const DASH_KEY = {json.dumps(key_q)};

  function fmtTime(iso) {{
    if (!iso) return "-";
    try {{ return new Date(iso).toLocaleString(); }} catch (e) {{ return iso; }}
  }}

  function pill(level) {{
    const c = LEVEL_COLORS[level] || "#999";
    return `<span class="pill" style="background:${{c}}">Lv${{level}}</span>`;
  }}

  function badge(role) {{
    const r = (role || "").toLowerCase();
    if (r === "assistant") return `<span class="badge assistant">assistant</span>`;
    if (r === "user") return `<span class="badge">user</span>`;
    return `<span class="badge">${{escapeHtml(role || "-")}}</span>`;
  }}

  function badgeStatus(s) {{
    const v = (s || "").toLowerCase();
    if (v === "sent") return `<span class="badge ok">sent</span>`;
    if (v === "failed") return `<span class="badge ng">failed</span>`;
    return `<span class="badge">${{escapeHtml(s || "-")}}</span>`;
  }}

  function escapeHtml(s) {{
    return (s || "").replace(/[&<>"]/g, c => ({{"&":"&amp;","<":"&lt;",">":"&gt;","\\"":"&quot;"}}[c]));
  }}

  let cache = [];
  function matchesSearch(row, q) {{
    if (!q) return true;
    q = q.toLowerCase();
    const fields = [
      row.user_id, row.next_goal, row.message, row.role, row.status, row.mode, row.error
    ].map(x => (x || "").toLowerCase());
    return fields.some(f => f.includes(q));
  }}

  function setHeader(view) {{
    const thead = document.getElementById("thead");
    if (view === "followups") {{
      thead.innerHTML = `
        <tr>
          <th>更新</th>
          <th>status</th>
          <th>mode</th>
          <th class="mono">user_id</th>
          <th>本文</th>
          <th>error</th>
        </tr>`;
      return;
    }}
    if (view === "customers") {{
      thead.innerHTML = `
        <tr>
          <th>更新</th>
          <th>温度</th>
          <th>確度</th>
          <th>次のゴール</th>
          <th class="mono">user_id</th>
          <th>直近メッセージ</th>
          <th>optout</th>
        </tr>`;
      return;
    }}
    thead.innerHTML = `
      <tr>
        <th>更新</th>
        <th>種別</th>
        <th>温度</th>
        <th>確度</th>
        <th>次のゴール</th>
        <th class="mono">user_id</th>
        <th>メッセージ</th>
      </tr>`;
  }}

  function render() {{
    const view = document.getElementById("viewSelect").value;
    setHeader(view);

    const q = document.getElementById("searchBox").value.trim().toLowerCase();
    const rows = cache.filter(r => matchesSearch(r, q));
    document.getElementById("count").textContent = rows.length;

    const tbody = document.getElementById("tbody");
    if (!rows.length) {{
      tbody.innerHTML = `<tr><td colspan="7" class="muted">No data</td></tr>`;
      return;
    }}

    if (view === "followups") {{
      tbody.innerHTML = rows.map(r => {{
        return `
          <tr>
            <td class="mono">${{escapeHtml(fmtTime(r.ts))}}</td>
            <td>${{badgeStatus(r.status)}}</td>
            <td class="mono">${{escapeHtml(r.mode || "-")}}</td>
            <td class="mono">${{escapeHtml(r.user_id || "")}}</td>
            <td class="rowmsg">${{escapeHtml(r.message || "")}}</td>
            <td class="rowmsg mono">${{escapeHtml(r.error || "-")}}</td>
          </tr>`;
      }}).join("");
      return;
    }}

    if (view === "customers") {{
      tbody.innerHTML = rows.map(r => {{
        const levelHtml = (r.temp_level_stable == null) ? "-" : pill(r.temp_level_stable);
        const confHtml  = (r.confidence == null) ? "-" : (Number(r.confidence).toFixed(2));
        const goalHtml  = (r.next_goal == null || r.next_goal === "") ? "-" : escapeHtml(r.next_goal);
        const opt = r.opt_out ? `<span class="optout">STOP</span>` : "-";
        return `
          <tr>
            <td class="mono">${{escapeHtml(fmtTime(r.ts))}}</td>
            <td>${{levelHtml}}</td>
            <td class="mono">${{confHtml}}</td>
            <td>${{goalHtml}}</td>
            <td class="mono">${{escapeHtml(r.user_id || "")}}</td>
            <td class="rowmsg">${{escapeHtml(r.message || "")}}</td>
            <td>${{opt}}</td>
          </tr>`;
      }}).join("");
      return;
    }}

    // events
    tbody.innerHTML = rows.map(r => {{
      const levelHtml = (r.temp_level_stable == null) ? "-" : pill(r.temp_level_stable);
      const confHtml  = (r.confidence == null) ? "-" : (Number(r.confidence).toFixed(2));
      const goalHtml  = (r.next_goal == null || r.next_goal === "") ? "-" : escapeHtml(r.next_goal);
      return `
        <tr>
          <td class="mono">${{escapeHtml(fmtTime(r.ts))}}</td>
          <td>${{badge(r.role)}}</td>
          <td>${{levelHtml}}</td>
          <td class="mono">${{confHtml}}</td>
          <td>${{goalHtml}}</td>
          <td class="mono">${{escapeHtml(r.user_id || "")}}</td>
          <td class="rowmsg">${{escapeHtml(r.message || "")}}</td>
        </tr>`;
    }}).join("");
  }}

  async function fetchData() {{
    const shopId = document.getElementById("shopId").value.trim();
    const view = document.getElementById("viewSelect").value;
    const minLevel = document.getElementById("minLevel").value;
    const limit = document.getElementById("limit").value;

    const url = `/api/hot?shop_id=${{encodeURIComponent(shopId)}}&min_level=${{encodeURIComponent(minLevel)}}&limit=${{encodeURIComponent(limit)}}&view=${{encodeURIComponent(view)}}&key=${{encodeURIComponent(DASH_KEY)}}`;
    const res = await fetch(url, {{ credentials: "same-origin" }});
    const data = await res.json();
    cache = Array.isArray(data) ? data : [];
    document.getElementById("now").textContent = new Date().toLocaleString();
    render();
  }}

  document.getElementById("btnApply").addEventListener("click", () => {{
    const shopId = document.getElementById("shopId").value.trim();
    const view = document.getElementById("viewSelect").value;
    const minLevel = document.getElementById("minLevel").value;
    const limit = document.getElementById("limit").value;
    const refreshSec = document.getElementById("refreshSec").value;

    const qs = new URLSearchParams({{
      shop_id: shopId,
      view: view,
      min_level: minLevel,
      limit: limit,
      refresh: refreshSec,
      key: DASH_KEY
    }});
    window.location.href = `/dashboard?${{qs.toString()}}`;
  }});

  document.getElementById("btnRefresh").addEventListener("click", fetchData);
  document.getElementById("searchBox").addEventListener("input", () => render());

  let timer = null;
  function setupAutoRefresh() {{
    if (timer) clearInterval(timer);
    const sec = parseInt(document.getElementById("refreshSec").value || "0", 10);
    if (sec > 0) timer = setInterval(fetchData, sec * 1000);
  }}
  document.getElementById("refreshSec").addEventListener("change", setupAutoRefresh);

  fetchData().then(setupAutoRefresh).catch(e => {{
    console.error(e);
    document.getElementById("tbody").innerHTML = `<tr><td class="muted">Error loading</td></tr>`;
  }});
</script>

</body>
</html>
"""
    return HTMLResponse(html)


# ============================================================
# Jobs
# ============================================================

@app.post("/jobs/followup")
async def job_followup(_: None = Depends(require_admin_key)):
    if not FOLLOWUP_ENABLED:
        return {"ok": True, "enabled": False, "reason": "FOLLOWUP_ENABLED!=1"}

    if not is_within_jst_window():
        return {
            "ok": True,
            "enabled": True,
            "skipped": True,
            "reason": f"out_of_time_window (JST {FOLLOWUP_JST_FROM}-{FOLLOWUP_JST_TO})",
            "now_jst": now_jst().isoformat(),
        }

    if not acquire_job_lock("followup", FOLLOWUP_LOCK_TTL_SEC):
        return {"ok": True, "enabled": True, "skipped": True, "reason": "locked"}

    candidates = get_followup_candidates()

    if FOLLOWUP_DRYRUN:
        return {"ok": True, "enabled": True, "dryrun": True, "candidates": candidates}

    sent = 0
    failed = 0
    for c in candidates:
        user_id = c["user_id"]
        conv_key = c["conv_key"]
        level = int(c["temp_level_stable"] or 0)
        goal = c.get("next_goal") or ""
        last_text = c.get("last_user_text") or ""

        # opt-out safety
        if is_opted_out(SHOP_ID, conv_key):
            save_followup_log(SHOP_ID, conv_key, user_id, "(skipped opt_out)", "template", "skipped", "opt_out")
            continue

        mode = "template"
        if FOLLOWUP_USE_OPENAI and OPENAI_API_KEY:
            mode = "openai"
            msg = await build_followup_message_openai(goal, last_text, level)
        else:
            msg = build_followup_template(goal, last_text, level)

        try:
            await push_message(user_id, msg)
            save_followup_log(SHOP_ID, conv_key, user_id, msg, mode, "sent", None)
            sent += 1
        except Exception as e:
            err = str(e)[:200]
            save_followup_log(SHOP_ID, conv_key, user_id, msg, mode, "failed", err)
            failed += 1

    return {"ok": True, "enabled": True, "dryrun": False, "candidates": len(candidates), "sent": sent, "failed": failed}


@app.post("/jobs/push_test")
async def job_push_test(
    _: None = Depends(require_admin_key),
    user_id: str = Query(..., description="LINE userId"),
    text: str = Query(..., description="message text"),
):
    await push_message(user_id, text)
    return {"ok": True}
