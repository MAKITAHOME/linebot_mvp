# app.py (FULL REWRITE)
# FastAPI + Render + Postgres(pg8000)
# LINE webhook: immediate reply + background AI + push
# Dashboard auth: DASHBOARD_KEY (query ?key=... or header X-Dashboard-Key)
# Jobs auth: ADMIN_API_KEY (header x-admin-key)
#
# Required env:
#   LINE_CHANNEL_SECRET
#   LINE_CHANNEL_ACCESS_TOKEN
#   DATABASE_URL
#   OPENAI_API_KEY
#   DASHBOARD_KEY
#   ADMIN_API_KEY  (for /jobs/* only)
# Optional:
#   SHOP_ID
#   DASHBOARD_REFRESH_SEC_DEFAULT
#   FOLLOWUP_ENABLED / FOLLOWUP_* (jobs)
#
# Followup env (recommended):
#   FOLLOWUP_ENABLED=1
#   FOLLOWUP_AFTER_MINUTES=60
#   FOLLOWUP_MIN_LEVEL=8
#   FOLLOWUP_LIMIT=50
#   FOLLOWUP_DRYRUN=0
#   FOLLOWUP_LOCK_TTL_SEC=180
#   FOLLOWUP_MIN_HOURS_BETWEEN=24
#   FOLLOWUP_JST_FROM=10
#   FOLLOWUP_JST_TO=20
#   FOLLOWUP_USE_OPENAI=0   (optional)

import os
import json
import hmac
import hashlib
import base64
import time
import ssl
import secrets
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

# Dashboard/Auth
DASHBOARD_KEY = os.getenv("DASHBOARD_KEY", "").strip()

# Jobs/Auth (machine)
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "").strip()

# Dashboard refresh
DASHBOARD_REFRESH_SEC_DEFAULT = int(os.getenv("DASHBOARD_REFRESH_SEC_DEFAULT", "30"))

# Followup config
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

# In-memory short history (MVP)
CHAT_HISTORY: Dict[str, deque] = defaultdict(lambda: deque(maxlen=40))
TEMP_HISTORY: Dict[str, deque] = defaultdict(lambda: deque(maxlen=3))

JST = timezone(timedelta(hours=9))

app = FastAPI(title="linebot_mvp", version="0.3.1")


# ============================================================
# Auth
# ============================================================

def require_dashboard_key(
    x_dashboard_key: Optional[str] = Header(default=None, alias="X-Dashboard-Key"),
    key: Optional[str] = Query(default=None),
) -> None:
    """
    Human UI protection for /dashboard and /api/hot.
    Accepts:
      - Header: X-Dashboard-Key: <DASHBOARD_KEY>
      - Query : ?key=<DASHBOARD_KEY>
    """
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
    """
    Protect /jobs/* endpoints for machine calls.
    """
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

    # Render: SSL前提、verify失敗時フォールバック
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


def ensure_tables_and_columns() -> None:
    """
    Create tables (if missing) and add columns (if missing).
    This avoids 'column does not exist' in older DB schemas.
    """
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
    cur.execute("""CREATE INDEX IF NOT EXISTS idx_messages_shop_created ON messages(shop_id, created_at DESC);""")
    cur.execute("""CREATE INDEX IF NOT EXISTS idx_messages_conv_role_created ON messages(conv_key, role, created_at DESC);""")
    cur.execute("""CREATE INDEX IF NOT EXISTS idx_followup_shop_conv_created ON followup_logs(shop_id, conv_key, created_at DESC);""")
    cur.execute("""CREATE INDEX IF NOT EXISTS idx_followup_user_created ON followup_logs(user_id, created_at DESC);""")

    cur.close()
    conn.close()


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
    if not LINE_CHANNEL_ACCESS_TOKEN:
        print("[LINE] Missing LINE_CHANNEL_ACCESS_TOKEN")
        return
    if not reply_token:
        return

    url = "https://api.line.me/v2/bot/message/reply"
    headers = {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {"replyToken": reply_token, "messages": [{"type": "text", "text": text[:4900]}]}

    try:
        async with httpx.AsyncClient(timeout=10, verify=certifi.where()) as client:
            r = await client.post(url, headers=headers, json=payload)
            if r.status_code >= 400:
                print("[LINE] reply failed:", r.status_code, r.text[:300])
    except Exception as e:
        print("[LINE] reply exception:", repr(e))


async def push_message(user_id: str, text: str) -> None:
    if not LINE_CHANNEL_ACCESS_TOKEN:
        print("[LINE] Missing LINE_CHANNEL_ACCESS_TOKEN")
        return
    if not user_id or user_id == "unknown":
        print("[LINE] push skipped: invalid user_id")
        return

    url = "https://api.line.me/v2/bot/message/push"
    headers = {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {"to": user_id, "messages": [{"type": "text", "text": text[:4900]}]}

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
ユーザーの最新発言から、成約に近い順に 1〜10 で温度を判定します。

【出力はJSONのみ】
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

【ペナルティ（過大評価防止）】
- 「内見」や「良さそう」など強い単語があっても、
  予算・入居時期・エリアが不明なら Lv8 以上にしない
- 入居時期が半年以上先なら最大でも Lv6
- 条件が全く出ていない場合は最大でも Lv5

【confidence の決め方】
- 根拠が2つ以上揃っている → 0.70〜0.90
- どちらとも取れる/情報不足 → 0.40〜0.65
- ほぼ推測 → 0.30〜0.45

【next_goal】
次に営業が達成すべき「1ステップ」を短く書く。
例：予算確認 / 入居時期確認 / 希望エリア確認 / 内見候補日提示 / 申込意思確認

【reasons】
判定根拠を短い箇条書き3つまで
"""

SYSTEM_PROMPT_ASSISTANT = """
あなたは不動産仲介の優秀な営業アシスタントです。
ユーザーに対して丁寧で簡潔、次の行動につながる返信を日本語で作ってください。
"""

SYSTEM_PROMPT_FOLLOWUP = """
あなたは不動産仲介の追客メッセージ作成AIです。
以下の情報をもとに、押し売り感ゼロで、返信しやすい一通を日本語で作ってください。

ルール：
- 2〜4行、短め
- 質問は最大2つ
- 「内見」「申込」など強い言葉は乱用しない
- 絵文字は最大1つ
"""


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


async def openai_chat(messages: List[Dict[str, str]], temperature: float = 0.2) -> str:
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is missing")

    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    payload = {"model": "gpt-4o-mini", "messages": messages, "temperature": temperature}

    async with httpx.AsyncClient(timeout=25, verify=certifi.where()) as client:
        r = await client.post(OPENAI_API_URL, headers=headers, json=payload)
        r.raise_for_status()
        data = r.json()
        return data["choices"][0]["message"]["content"]


async def analyze_and_generate_reply(user_text: str, user_id: str, conv_key: str) -> Tuple[int, int, float, str, List[str], str]:
    # 1) analyze
    analysis_messages = [
        {"role": "system", "content": SYSTEM_PROMPT_ANALYZE},
        {"role": "user", "content": user_text},
    ]

    raw_level = 5
    conf = 0.5
    next_goal = "情報収集を続ける"
    reasons: List[str] = []

    try:
        raw_json_text = await openai_chat(analysis_messages, temperature=0.0)
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
        print("[OPENAI] analyze failed:", repr(e))

    # stable median (last 3)
    hist = TEMP_HISTORY[conv_key]
    hist.append(raw_level)
    sorted_hist = sorted(hist)
    stable_level = sorted_hist[len(sorted_hist) // 2]

    # 2) assistant reply
    history = CHAT_HISTORY[user_id]
    context_msgs = [{"role": "system", "content": SYSTEM_PROMPT_ASSISTANT}]
    for role, content in list(history)[-10:]:
        context_msgs.append({"role": role, "content": content})
    context_msgs.append({"role": "user", "content": user_text})

    try:
        reply_text = await openai_chat(context_msgs, temperature=0.4)
    except Exception as e:
        print("[OPENAI] reply failed:", repr(e))
        reply_text = "ありがとうございます。条件をもう少し教えてください（エリア/家賃/間取り/入居時期など）。"

    history.append(("user", user_text))
    history.append(("assistant", reply_text))

    return raw_level, stable_level, conf, next_goal, reasons, reply_text


# ============================================================
# DB helpers
# ============================================================

def save_message(shop_id: str, conv_key: str, role: str, content: str) -> None:
    if not DATABASE_URL:
        return
    db_execute(
        """
        INSERT INTO messages (shop_id, conv_key, role, content)
        VALUES (%s, %s, %s, %s)
        """,
        (shop_id, conv_key, role, content),
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


# ============================================================
# Followup Logic
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

    # start < end: 10-20 など
    if start < end:
        return start <= h < end
    # start > end: 22-6 など（深夜跨ぎ）
    if start > end:
        return (h >= start) or (h < end)
    # start == end: 送らない
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
    elif "申込" in goal:
        q = "ご不安点があれば先に解消できます。気になる点はありますか？"
    else:
        q = "条件を少し整理したいので、希望があれば教えてください。"

    if level >= 9:
        lead = "念のため確認です。"
    elif level >= 8:
        lead = "その後いかがでしょうか？"
    else:
        lead = "以前の件、その後の状況だけ伺ってもいいですか？"

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
        out = await openai_chat(msgs, temperature=0.5)
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
        last_followup_at = r[8]
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
# Routes
# ============================================================

@app.get("/")
async def root():
    return {"ok": True}


@app.get("/healthz")
async def healthz():
    return {"ok": True, "ts": int(time.time())}


# ------------------------------------------------------------
# LINE webhook: immediate reply + background AI + push
# ------------------------------------------------------------

async def process_ai_and_push(shop_id: str, user_id: str, conv_key: str, user_text: str) -> None:
    try:
        raw_level, stable_level, conf, next_goal, reasons, ai_reply = await analyze_and_generate_reply(
            user_text=user_text,
            user_id=user_id,
            conv_key=conv_key,
        )

        try:
            upsert_customer(
                shop_id=shop_id,
                conv_key=conv_key,
                user_id=user_id,
                last_user_text=user_text,
                raw_level=raw_level,
                stable_level=stable_level,
                confidence=conf,
                next_goal=next_goal,
            )
        except Exception as db_e:
            print("[DB] upsert_customer failed:", repr(db_e))

        try:
            save_message(shop_id, conv_key, "assistant", ai_reply)
        except Exception as db_e2:
            print("[DB] save assistant message failed:", repr(db_e2))

        if reasons:
            print(f"[TEMP] {conv_key} raw={raw_level} stable={stable_level} conf={conf:.2f} goal={next_goal} reasons={reasons}")
        else:
            print(f"[TEMP] {conv_key} raw={raw_level} stable={stable_level} conf={conf:.2f} goal={next_goal}")

        await push_message(user_id, ai_reply)

    except Exception as e:
        print("[BG] process_ai_and_push exception:", repr(e))


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

        try:
            save_message(SHOP_ID, conv_key, "user", user_text)
        except Exception as e:
            print("[DB] save user message failed:", repr(e))

        await reply_message(reply_token, "ありがとうございます！内容を確認しています。少々お待ちください😊")
        background.add_task(process_ai_and_push, SHOP_ID, user_id, conv_key, user_text)

    return {"ok": True}


# ------------------------------------------------------------
# API for dashboard (protected by DASHBOARD_KEY)
# ------------------------------------------------------------

@app.get("/api/hot")
async def api_hot(
    _: None = Depends(require_dashboard_key),
    shop_id: str = Query(default=SHOP_ID),
    min_level: int = Query(default=8, ge=1, le=10),
    limit: int = Query(default=50, ge=1, le=200),
    view: str = Query(default="customers", pattern="^(customers|events)$"),
):
    if not DATABASE_URL:
        return JSONResponse([], status_code=200)

    if view == "customers":
        rows = db_fetchall(
            """
            SELECT conv_key, user_id, last_user_text, temp_level_stable, confidence, next_goal, updated_at
            FROM customers
            WHERE shop_id = %s AND COALESCE(temp_level_stable, 0) >= %s
            ORDER BY updated_at DESC
            LIMIT %s
            """,
            (shop_id, min_level, limit),
        )
        return JSONResponse([
            {
                "conv_key": r[0],
                "user_id": r[1],
                "last_user_text": r[2],
                "temp_level_stable": r[3],
                "confidence": float(r[4]) if r[4] is not None else None,
                "next_goal": r[5],
                "updated_at": r[6].isoformat() if r[6] else None,
            }
            for r in rows
        ])

    # events view（履歴）
    rows = db_fetchall(
        """
        SELECT
          c.conv_key,
          c.user_id,
          m.content,
          m.created_at,
          NULL AS temp_level_stable,
          NULL AS confidence,
          NULL AS next_goal
        FROM customers c
        LEFT JOIN messages m
          ON c.conv_key = m.conv_key AND m.role = 'user'
        WHERE c.shop_id = %s
        ORDER BY m.created_at DESC NULLS LAST
        LIMIT %s
        """,
        (shop_id, limit),
    )

    return JSONResponse([
        {
            "conv_key": r[0],
            "user_id": r[1],
            "last_user_text": r[2],
            "message_created_at": r[3].isoformat() if r[3] else None,
            "temp_level_stable": r[4],
            "confidence": float(r[5]) if r[5] is not None else None,
            "next_goal": r[6],
        }
        for r in rows
    ])


# ------------------------------------------------------------
# Dashboard (protected by DASHBOARD_KEY)
# ------------------------------------------------------------

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
    view: str = Query(default="events", pattern="^(customers|events)$"),
    min_level: int = Query(default=8, ge=1, le=10),
    limit: int = Query(default=50, ge=1, le=200),
    refresh: int = Query(default=DASHBOARD_REFRESH_SEC_DEFAULT, ge=0, le=300),
    key: Optional[str] = Query(default=None),  # pass-through for JS fetch
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
  .wrap {{ max-width: 1100px; margin: 0 auto; }}
  .card {{
    background: #fff;
    border-radius: 14px;
    box-shadow: 0 1px 6px rgba(0,0,0,.08);
    padding: 14px 16px;
    margin-bottom: 14px;
  }}
  .title {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 12px;
    flex-wrap: wrap;
  }}
  .title h1 {{ font-size: 18px; margin: 0; }}
  .meta {{ font-size: 12px; color: #555; }}
  .filters {{
    display: grid;
    grid-template-columns: 1fr 160px 120px 120px 140px;
    gap: 10px;
    align-items: end;
  }}
  @media (max-width: 860px) {{ .filters {{ grid-template-columns: 1fr 1fr; }} }}
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
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 4px 10px;
    border-radius: 999px;
    font-size: 12px;
    color: #fff;
    font-weight: 700;
    white-space: nowrap;
  }}
  .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas; font-size: 12px; }}
  .muted {{ color: #777; font-size: 12px; }}
  .rowmsg {{ max-width: 520px; word-break: break-word; }}
  .link {{ color: #0b57d0; text-decoration: none; }}
  .link:hover {{ text-decoration: underline; }}
  .search {{ display:flex; gap:10px; align-items: center; margin-top: 10px; }}
  .search input {{ flex: 1; }}
  .hint {{ font-size: 12px; color: #666; margin-top: 6px; }}
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
          <option value="customers" {"selected" if view=="customers" else ""}>customers（最新状態）</option>
          <option value="events" {"selected" if view=="events" else ""}>events（履歴）</option>
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
        <label>検索（自動） user_id / next_goal / 直近メッセージ</label>
        <input id="searchBox" placeholder="例: 渋谷 / 3LDK / 内見 / Uxxxxxxxx" />
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
      <thead>
        <tr>
          <th>更新</th>
          <th>温度</th>
          <th>確度</th>
          <th>次のゴール</th>
          <th class="mono">user_id</th>
          <th>直近メッセージ</th>
        </tr>
      </thead>
      <tbody id="tbody">
        <tr><td colspan="6" class="muted">Loading...</td></tr>
      </tbody>
    </table>
  </div>
</div>

<script>
  const LEVEL_COLORS = {json.dumps(LEVEL_COLORS)};
  const DASH_KEY = {json.dumps(key_q)};

  function fmtTime(iso) {{
    if (!iso) return "-";
    try {{
      const d = new Date(iso);
      return d.toLocaleString();
    }} catch (e) {{
      return iso;
    }}
  }}

  function pill(level) {{
    const c = LEVEL_COLORS[level] || "#999";
    return `<span class="pill" style="background:${{c}}">Lv${{level}}</span>`;
  }}

  function escapeHtml(s) {{
    return (s || "").replace(/[&<>"]/g, c => ({{"&":"&amp;","<":"&lt;",">":"&gt;","\\"":"&quot;"}}[c]));
  }}

  let cache = [];

  function matchesSearch(row, q) {{
    if (!q) return true;
    q = q.toLowerCase();
    const fields = [row.user_id, row.next_goal, row.last_user_text].map(x => (x || "").toLowerCase());
    return fields.some(f => f.includes(q));
  }}

  function render() {{
    const q = document.getElementById("searchBox").value.trim().toLowerCase();
    const rows = cache.filter(r => matchesSearch(r, q));
    document.getElementById("count").textContent = rows.length;

    const tbody = document.getElementById("tbody");
    if (!rows.length) {{
      tbody.innerHTML = `<tr><td colspan="6" class="muted">No data</td></tr>`;
      return;
    }}

    tbody.innerHTML = rows.map(r => {{
      const updated = r.message_created_at || r.updated_at;

      // ★ここが今回の修正ポイント（NULLを 0 にしない）
      const levelHtml = (r.temp_level_stable == null) ? "-" : pill(r.temp_level_stable);
      const confHtml  = (r.confidence == null) ? "-" : (Number(r.confidence).toFixed(2));
      const goalHtml  = (r.next_goal == null || r.next_goal === "") ? "-" : escapeHtml(r.next_goal);

      return `
        <tr>
          <td class="mono">${{escapeHtml(fmtTime(updated))}}</td>
          <td>${{levelHtml}}</td>
          <td class="mono">${{confHtml}}</td>
          <td>${{goalHtml}}</td>
          <td class="mono">${{escapeHtml(r.user_id || "")}}</td>
          <td class="rowmsg">${{escapeHtml(r.last_user_text || "")}}</td>
        </tr>
      `;
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
    document.getElementById("tbody").innerHTML = `<tr><td colspan="6" class="muted">Error loading</td></tr>`;
  }});
</script>

</body>
</html>
"""
    return HTMLResponse(html)


# ------------------------------------------------------------
# Followup job (ADMIN_API_KEY)
# ------------------------------------------------------------

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
        print("[FOLLOWUP] DRYRUN candidates:", len(candidates))
        return {"ok": True, "enabled": True, "dryrun": True, "candidates": candidates}

    sent = 0
    failed = 0
    details: List[Dict[str, Any]] = []

    for c in candidates:
        user_id = c["user_id"]
        conv_key = c["conv_key"]
        level = int(c["temp_level_stable"] or 0)
        goal = c.get("next_goal") or ""
        last_text = c.get("last_user_text") or ""

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
            details.append({"conv_key": conv_key, "user_id": user_id, "status": "sent", "mode": mode})
        except Exception as e:
            err = str(e)[:200]
            print("[FOLLOWUP] send failed:", conv_key, repr(e))
            save_followup_log(SHOP_ID, conv_key, user_id, msg, mode, "failed", err)
            failed += 1
            details.append({"conv_key": conv_key, "user_id": user_id, "status": "failed", "mode": mode, "error": err})

    print(f"[FOLLOWUP] done sent={sent} failed={failed} candidates={len(candidates)}")
    return {
        "ok": True,
        "enabled": True,
        "dryrun": False,
        "candidates": len(candidates),
        "sent": sent,
        "failed": failed,
        "details": details[:50],
    }
