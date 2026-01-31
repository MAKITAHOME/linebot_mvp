# main.py
import os
import json
import hmac
import hashlib
import base64
import ssl
import secrets
from datetime import datetime, timedelta, timezone
from collections import deque, defaultdict
from typing import Any, Dict, List, Optional, Tuple, Literal

import certifi
import httpx
import pg8000

from fastapi import FastAPI, Request, Header, HTTPException, Query, Depends, status
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials


# ============================================================
# Security: Basic Auth (for browser/dashboard)
# ============================================================

security = HTTPBasic(auto_error=False)


def _basic_auth_configured() -> bool:
    return bool(os.getenv("BASIC_AUTH_USER", "").strip() and os.getenv("BASIC_AUTH_PASS", "").strip())


def require_basic_auth(credentials: HTTPBasicCredentials = Depends(security)) -> str:
    """
    /dashboard をブラウザで守る用の Basic 認証。
    BASIC_AUTH_USER / BASIC_AUTH_PASS が設定されている時だけ有効化。
    未設定なら「認証なしで通す」(=開くことを優先)。
    """
    user = os.getenv("BASIC_AUTH_USER", "").strip()
    pw = os.getenv("BASIC_AUTH_PASS", "").strip()

    # 未設定なら auth を無効化（= 通す）
    if not user or not pw:
        return "basic-auth-disabled"

    if credentials is None:
        # ブラウザにログインダイアログを出す
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Basic"},
        )

    ok_user = secrets.compare_digest(credentials.username or "", user)
    ok_pw = secrets.compare_digest(credentials.password or "", pw)
    if not (ok_user and ok_pw):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Basic"},
        )

    return credentials.username


# ============================================================
# Config / Environment
# ============================================================

LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "")
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
SHOP_ID = os.getenv("SHOP_ID", "tokyo_01")

# Optional: protect admin endpoints (dashboard + api + job)
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "").strip()

# Auto followup job config
FOLLOWUP_ENABLED = os.getenv("FOLLOWUP_ENABLED", "0").strip() == "1"
FOLLOWUP_AFTER_MINUTES = int(os.getenv("FOLLOWUP_AFTER_MINUTES", "60"))
FOLLOWUP_MIN_LEVEL = int(os.getenv("FOLLOWUP_MIN_LEVEL", "8"))
FOLLOWUP_LIMIT = int(os.getenv("FOLLOWUP_LIMIT", "50"))
FOLLOWUP_DRYRUN = os.getenv("FOLLOWUP_DRYRUN", "0").strip() == "1"
FOLLOWUP_LOCK_TTL_SEC = int(os.getenv("FOLLOWUP_LOCK_TTL_SEC", "180"))

# Server-side refresh interval for dashboard auto-refresh
DASHBOARD_REFRESH_SEC_DEFAULT = int(os.getenv("DASHBOARD_REFRESH_SEC_DEFAULT", "30"))

# ===== Chat history in-memory (MVP)
# user_id -> last 40 turns (user/assistant)
CHAT_HISTORY: Dict[str, deque] = defaultdict(lambda: deque(maxlen=40))

# For stable temperature median: conv_key -> last 3 raw levels
TEMP_HISTORY: Dict[str, deque] = defaultdict(lambda: deque(maxlen=3))

# App
app = FastAPI(title="linebot_mvp", version="0.1.1")


# ============================================================
# Utilities: Admin access (X-Admin-Key OR BasicAuth)
# ============================================================

def require_admin_or_basic(
    x_admin_key: Optional[str] = Header(default=None),
    credentials: HTTPBasicCredentials = Depends(security),
) -> None:
    """
    - ADMIN_API_KEY が設定されている場合:
        - X-Admin-Key が一致 => OK
        - それ以外は BasicAuth(設定されていれば) でOK
        - どちらも無理なら 401
    - ADMIN_API_KEY が未設定の場合:
        - BasicAuth が設定されていれば BasicAuth で保護
        - どちらも未設定ならオープン（MVP運用向け）
    """
    # 1) Admin key が通ればOK
    if ADMIN_API_KEY and x_admin_key and secrets.compare_digest(x_admin_key.strip(), ADMIN_API_KEY):
        return

    # 2) BasicAuth が設定されていれば BasicAuth で通す（/dashboard からの fetch も通る）
    if _basic_auth_configured():
        require_basic_auth(credentials)
        return

    # 3) どちらも設定されていないなら通す
    if not ADMIN_API_KEY:
        return

    # 4) ADMIN_API_KEY はあるのに、admin key も basic auth も使えない => 拒否
    raise HTTPException(status_code=401, detail="unauthorized")


# ============================================================
# Utilities: DB (pg8000, pure-python)
# ============================================================

def parse_database_url(url: str) -> Dict[str, Any]:
    """
    Supports:
      postgres://user:pass@host:port/db
      postgresql://user:pass@host:port/db
      postgresql://user:pass@host:port/db?sslmode=require
    """
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

    sslmode = cfg["params"].get("sslmode", "").lower()
    use_ssl = sslmode in ("require", "verify-full", "verify-ca") or True  # default True on Render
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


def ensure_tables() -> None:
    conn = None
    try:
        conn = connect_db(verify_ssl=True)
    except ssl.SSLError as e:
        print("[DB] SSL verify failed, fallback to disable:", repr(e))
        conn = connect_db(verify_ssl=False)

    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS customers (
          id BIGSERIAL PRIMARY KEY,
          shop_id TEXT NOT NULL,
          conv_key TEXT NOT NULL,
          user_id TEXT NOT NULL,
          last_user_text TEXT,
          temp_level_raw INT,
          temp_level_stable INT,
          confidence REAL,
          next_goal TEXT,
          updated_at TIMESTAMPTZ DEFAULT now(),
          UNIQUE (shop_id, conv_key)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS messages (
          id BIGSERIAL PRIMARY KEY,
          shop_id TEXT NOT NULL,
          conv_key TEXT NOT NULL,
          role TEXT NOT NULL,          -- user | assistant | system
          content TEXT NOT NULL,
          created_at TIMESTAMPTZ DEFAULT now()
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS job_locks (
          key TEXT PRIMARY KEY,
          locked_until TIMESTAMPTZ NOT NULL
        );
        """
    )

    cur.close()
    conn.close()


@app.on_event("startup")
async def on_startup():
    ensure_tables()
    print("[BOOT] tables ensured")


def db_execute(sql: str, args: Tuple[Any, ...] = ()):
    conn = None
    try:
        conn = connect_db(verify_ssl=True)
    except ssl.SSLError as e:
        print("[DB] SSL verify failed, fallback to disable:", repr(e))
        conn = connect_db(verify_ssl=False)

    cur = conn.cursor()
    cur.execute(sql, args)
    cur.close()
    conn.close()


def db_fetchall(sql: str, args: Tuple[Any, ...] = ()) -> List[Tuple[Any, ...]]:
    conn = None
    try:
        conn = connect_db(verify_ssl=True)
    except ssl.SSLError as e:
        print("[DB] SSL verify failed, fallback to disable:", repr(e))
        conn = connect_db(verify_ssl=False)

    cur = conn.cursor()
    cur.execute(sql, args)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


# ============================================================
# Utilities: LINE signature verify
# ============================================================

def verify_signature(body: bytes, signature: str) -> bool:
    if not LINE_CHANNEL_SECRET:
        return False
    mac = hmac.new(LINE_CHANNEL_SECRET.encode("utf-8"), body, hashlib.sha256).digest()
    expected = base64.b64encode(mac).decode("utf-8")
    return hmac.compare_digest(expected, signature)


# ============================================================
# Utilities: LINE reply
# ============================================================

async def reply_message(reply_token: str, text: str) -> None:
    if not LINE_CHANNEL_ACCESS_TOKEN:
        print("[LINE] Missing LINE_CHANNEL_ACCESS_TOKEN")
        return

    url = "https://api.line.me/v2/bot/message/reply"
    headers = {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "replyToken": reply_token,
        "messages": [{"type": "text", "text": text}],
    }

    try:
        async with httpx.AsyncClient(timeout=10, verify=certifi.where()) as client:
            r = await client.post(url, headers=headers, json=payload)
            if r.status_code >= 400:
                print("[LINE] reply failed:", r.status_code, r.text)
    except Exception as e:
        print("[LINE] reply exception:", repr(e))


# ============================================================
# Utilities: OpenAI (Chat Completions)
# ============================================================

OPENAI_API_URL = "https://api.openai.com/v1/chat/completions"

SYSTEM_PROMPT_ANALYZE = """
あなたは不動産仲介のSaaS向け「顧客温度判定AI」です。

ユーザーの最新発言に対して、
- temp_level_raw: 1〜10（10が最も成約に近い）
- confidence: 0.0〜1.0（数値）
- next_goal: 次に営業が達成すべきゴール（短い日本語）
をJSONで返してください。

【出力はJSONのみ】
{
  "temp_level_raw": 1,
  "confidence": 0.7,
  "next_goal": "例：内見日程を提案する"
}

【ルール】
- confidence は必ず数値。日本語（高い/低い 等）を使わない
- 迷う場合は confidence を低めにし、next_goal を「情報収集」系に
"""

SYSTEM_PROMPT_ASSISTANT = """
あなたは不動産仲介の優秀な営業アシスタントです。
ユーザーに対して丁寧で簡潔、次の行動につながる返信を日本語で作ってください。
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
        if fv > 1.0 and fv <= 100.0:
            fv = fv / 100.0
        return max(0.0, min(1.0, fv))

    s = str(v).strip().lower()

    if s in ("高い", "たかい", "high"):
        return 0.85
    if s in ("中", "普通", "ふつう", "medium"):
        return 0.6
    if s in ("低い", "ひくい", "low"):
        return 0.35

    if s.endswith("%"):
        try:
            fv = float(s[:-1]) / 100.0
            return max(0.0, min(1.0, fv))
        except Exception:
            return 0.6

    try:
        fv = float(s)
        if fv > 1.0 and fv <= 100.0:
            fv = fv / 100.0
        return max(0.0, min(1.0, fv))
    except Exception:
        return 0.6


async def openai_chat(messages: List[Dict[str, str]], temperature: float = 0.2) -> str:
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is missing")

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": "gpt-4o-mini",
        "messages": messages,
        "temperature": temperature,
    }

    async with httpx.AsyncClient(timeout=20, verify=certifi.where()) as client:
        r = await client.post(OPENAI_API_URL, headers=headers, json=payload)
        r.raise_for_status()
        data = r.json()
        return data["choices"][0]["message"]["content"]


async def analyze_and_generate_reply(user_text: str, user_id: str, conv_key: str) -> Tuple[int, int, float, str, str]:
    analysis_messages = [
        {"role": "system", "content": SYSTEM_PROMPT_ANALYZE},
        {"role": "user", "content": user_text},
    ]

    raw_json_text = ""
    try:
        raw_json_text = await openai_chat(analysis_messages, temperature=0.0)
    except Exception as e:
        print("[OPENAI] analyze exception:", repr(e))
        raw_level = 5
        conf = 0.5
        next_goal = "情報収集を続ける"
    else:
        try:
            raw = raw_json_text.strip()
            if raw.startswith("```"):
                raw = raw.split("```", 2)[1]
            j = json.loads(raw)
        except Exception:
            try:
                start = raw_json_text.find("{")
                end = raw_json_text.rfind("}")
                j = json.loads(raw_json_text[start:end + 1])
            except Exception as e2:
                print("[OPENAI] analyze parse failed:", repr(e2), "raw=", raw_json_text[:200])
                j = {"temp_level_raw": 5, "confidence": 0.5, "next_goal": "情報収集を続ける"}

        raw_level = coerce_level(j.get("temp_level_raw", 5))
        conf = coerce_confidence(j.get("confidence", 0.6))
        next_goal = str(j.get("next_goal", "情報収集を続ける")).strip()[:80]

    hist = TEMP_HISTORY[conv_key]
    hist.append(raw_level)
    sorted_hist = sorted(hist)
    stable_level = sorted_hist[len(sorted_hist) // 2]

    history = CHAT_HISTORY[user_id]
    context_msgs = [{"role": "system", "content": SYSTEM_PROMPT_ASSISTANT}]
    for role, content in list(history)[-10:]:
        context_msgs.append({"role": role, "content": content})
    context_msgs.append({"role": "user", "content": user_text})

    try:
        reply_text = await openai_chat(context_msgs, temperature=0.4)
    except Exception as e:
        print("[OPENAI] reply exception:", repr(e))
        reply_text = "ありがとうございます。条件をもう少し教えてください（エリア/家賃/間取り/入居時期など）。"

    history.append(("user", user_text))
    history.append(("assistant", reply_text))

    return raw_level, stable_level, conf, next_goal, reply_text


# ============================================================
# DB write helpers
# ============================================================

def save_message(shop_id: str, conv_key: str, role: str, content: str) -> None:
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
    db_execute(
        """
        INSERT INTO customers (shop_id, conv_key, user_id, last_user_text, temp_level_raw, temp_level_stable, confidence, next_goal, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
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


# ============================================================
# Routes
# ============================================================

@app.get("/")
async def root():
    return {"ok": True}


@app.post("/line/webhook")
async def line_webhook(request: Request, x_line_signature: str = Header(default="")):
    body = await request.body()

    if not verify_signature(body, x_line_signature):
        raise HTTPException(status_code=401, detail="invalid signature")

    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception:
        raise HTTPException(status_code=400, detail="invalid json")

    events = payload.get("events", [])
    for ev in events:
        if ev.get("type") != "message":
            continue

        message = ev.get("message", {})
        if message.get("type") != "text":
            continue

        user_id = ev.get("source", {}).get("userId", "unknown")
        reply_token = ev.get("replyToken", "")
        user_text = message.get("text", "").strip()

        conv_key = f"user:{user_id}"

        save_message(SHOP_ID, conv_key, "user", user_text)

        raw_level, stable_level, conf, next_goal, reply_text = await analyze_and_generate_reply(
            user_text=user_text,
            user_id=user_id,
            conv_key=conv_key,
        )

        upsert_customer(
            shop_id=SHOP_ID,
            conv_key=conv_key,
            user_id=user_id,
            last_user_text=user_text,
            raw_level=raw_level,
            stable_level=stable_level,
            confidence=conf,
            next_goal=next_goal,
        )

        save_message(SHOP_ID, conv_key, "assistant", reply_text)

        print(f"[TEMP] key={conv_key} level_raw={raw_level} level_stable={stable_level} conf={conf:.2f} goal={next_goal}")
        print(f"[DB] wrote conv_key={conv_key} user_id={user_id}")

        if reply_token:
            await reply_message(reply_token, reply_text)

    return {"ok": True}


# ============================================================
# Admin API: HOT customers
# ============================================================

DashboardView = Literal["customers", "events"]


@app.get("/api/hot")
async def api_hot(
    _: str = Depends(require_basic_auth),
    shop_id: str = Query(default=SHOP_ID),
    min_level: int = Query(default=8, ge=1, le=10),
    limit: int = Query(default=50, ge=1, le=200),
    view: str = Query(default="customers", pattern="^(customers|events)$"),
):
    if view == "customers":
        rows = db_fetchall(
            """
            SELECT conv_key,
                   last_user_text,
                   temp_level_stable,
                   confidence,
                   next_goal,
                   updated_at
            FROM customers
            WHERE shop_id = %s AND temp_level_stable >= %s
            ORDER BY updated_at DESC
            LIMIT %s
            """,
            (shop_id, min_level, limit),
        )
        return JSONResponse([
            {
                "user_id": r[0],  # conv_key を user_id 表示に流用
                "last_user_text": r[1],
                "temp_level_stable": r[2],
                "confidence": float(r[3]) if r[3] is not None else None,
                "next_goal": r[4],
                "updated_at": r[5].isoformat() if r[5] else None,
            }
            for r in rows
        ])

    # events view（user_id カラムに依存しない）
    rows = db_fetchall(
        """
        SELECT c.conv_key,
               m.content,
               m.created_at,
               c.temp_level_stable,
               c.confidence,
               c.next_goal
        FROM customers c
        LEFT JOIN messages m
          ON c.conv_key = m.conv_key AND m.role = 'user'
        WHERE c.shop_id = %s AND c.temp_level_stable >= %s
        ORDER BY m.created_at DESC NULLS LAST
        LIMIT %s
        """,
        (shop_id, min_level, limit),
    )

    return JSONResponse([
        {
            "user_id": r[0],  # conv_key
            "last_user_text": r[1],
            "message_created_at": r[2].isoformat() if r[2] else None,
            "temp_level_stable": r[3],
            "confidence": float(r[4]) if r[4] is not None else None,
            "next_goal": r[5],
        }
        for r in rows
    ])
    """
    view=customers: latest snapshot from customers table
    view=events: history from messages table (user messages only) joined with latest customer state
    """

    if view == "customers":
        rows = db_fetchall(
            """
            SELECT shop_id, conv_key, user_id, last_user_text, temp_level_raw, temp_level_stable, confidence, next_goal, updated_at
            FROM customers
            WHERE shop_id = %s AND temp_level_stable >= %s
            ORDER BY updated_at DESC
            LIMIT %s
            """,
            (shop_id, min_level, limit),
        )
        out = []
        for r in rows:
            out.append(
                {
                    "shop_id": r[0],
                    "conv_key": r[1],
                    "user_id": r[2],
                    "last_user_text": r[3],
                    "temp_level_raw": r[4],
                    "temp_level_stable": r[5],
                    "confidence": float(r[6]) if r[6] is not None else None,
                    "next_goal": r[7],
                    "updated_at": r[8].isoformat() if r[8] else None,
                }
            )
        return JSONResponse(out)

    cust_rows = db_fetchall(
        """
        SELECT conv_key, user_id, temp_level_raw, temp_level_stable, confidence, next_goal, updated_at
        FROM customers
        WHERE shop_id = %s AND temp_level_stable >= %s
        ORDER BY updated_at DESC
        LIMIT %s
        """,
        (shop_id, min_level, limit),
    )
    cust_map = {r[0]: r for r in cust_rows}
    if not cust_map:
        return JSONResponse([])

    conv_keys = list(cust_map.keys())
    placeholders = ",".join(["%s"] * len(conv_keys))
    msg_rows = db_fetchall(
        f"""
        SELECT shop_id, conv_key, content, created_at
        FROM messages
        WHERE shop_id = %s AND role = 'user' AND conv_key IN ({placeholders})
        ORDER BY created_at DESC
        LIMIT %s
        """,
        tuple([shop_id] + conv_keys + [limit]),
    )

    out = []
    for r in msg_rows:
        conv_key = r[1]
        c = cust_map.get(conv_key)
        if not c:
            continue
        out.append(
            {
                "shop_id": shop_id,
                "conv_key": conv_key,
                "user_id": c[1],
                "last_user_text": r[2],
                "temp_level_raw": c[2],
                "temp_level_stable": c[3],
                "confidence": float(c[4]) if c[4] is not None else None,
                "next_goal": c[5],
                "updated_at": c[6].isoformat() if c[6] else None,
                "message_created_at": r[3].isoformat() if r[3] else None,
            }
        )

    return JSONResponse(out)


# ============================================================
# Dashboard (HTML)
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
    _user: str = Depends(require_basic_auth),
    shop_id: str = Query(default=SHOP_ID),
    view: DashboardView = Query(default="events"),
    min_level: int = Query(default=8, ge=1, le=10),
    limit: int = Query(default=50, ge=1, le=200),
    refresh: int = Query(default=DASHBOARD_REFRESH_SEC_DEFAULT, ge=0, le=300),  # 0 = auto refresh off
):
    api_url = f"/api/hot?shop_id={shop_id}&min_level={min_level}&limit={limit}&view={view}"

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
  .wrap {{
    max-width: 1100px;
    margin: 0 auto;
  }}
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
  .title h1 {{
    font-size: 18px;
    margin: 0;
  }}
  .meta {{
    font-size: 12px;
    color: #555;
  }}
  .filters {{
    display: grid;
    grid-template-columns: 1fr 160px 120px 120px 140px;
    gap: 10px;
    align-items: end;
  }}
  @media (max-width: 860px) {{
    .filters {{
      grid-template-columns: 1fr 1fr;
    }}
  }}
  label {{
    font-size: 12px;
    color: #444;
    display: block;
    margin-bottom: 4px;
  }}
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
  button:hover {{
    opacity: .9;
  }}
  table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 14px;
  }}
  th, td {{
    padding: 10px 8px;
    border-bottom: 1px solid #eee;
    vertical-align: top;
  }}
  th {{
    text-align: left;
    font-size: 12px;
    color: #666;
    font-weight: 600;
  }}
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
  .mono {{
    font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
    font-size: 12px;
  }}
  .muted {{
    color: #777;
    font-size: 12px;
  }}
  .rowmsg {{
    max-width: 520px;
    word-break: break-word;
  }}
  .link {{
    color: #0b57d0;
    text-decoration: none;
  }}
  .link:hover {{
    text-decoration: underline;
  }}
  .search {{
    display:flex;
    gap:10px;
    align-items: center;
    margin-top: 10px;
  }}
  .search input {{
    flex: 1;
  }}
  .hint {{
    font-size: 12px;
    color: #666;
    margin-top: 6px;
  }}
  .err {{
    margin-top: 8px;
    color: #b00020;
    font-size: 12px;
    white-space: pre-wrap;
  }}
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
      <div id="err" class="err"></div>
    </div>
    <div>
      <button id="btnRefresh">更新</button>
    </div>
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
    return (s || "").replace(/[&<>"]/g, c => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}}[c]));
  }}

  let cache = [];

  function matchesSearch(row, q) {{
    if (!q) return true;
    q = q.toLowerCase();
    const fields = [
      row.user_id,
      row.next_goal,
      row.last_user_text,
    ].map(x => (x || "").toLowerCase());
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
      const conf = (r.confidence ?? 0).toFixed(2);
      return `
        <tr>
          <td class="mono">${{escapeHtml(fmtTime(updated))}}</td>
          <td>${{pill(r.temp_level_stable || 0)}}</td>
          <td class="mono">${{escapeHtml(conf)}}</td>
          <td>${{escapeHtml(r.next_goal || "")}}</td>
          <td class="mono">${{escapeHtml(r.user_id || "")}}</td>
          <td class="rowmsg">${{escapeHtml(r.last_user_text || "")}}</td>
        </tr>
      `;
    }}).join("");
  }}

  async function fetchData() {{
    document.getElementById("err").textContent = "";

    const shopId = document.getElementById("shopId").value.trim();
    const view = document.getElementById("viewSelect").value;
    const minLevel = document.getElementById("minLevel").value;
    const limit = document.getElementById("limit").value;

    const url = `/api/hot?shop_id=${{encodeURIComponent(shopId)}}&min_level=${{encodeURIComponent(minLevel)}}&limit=${{encodeURIComponent(limit)}}&view=${{encodeURIComponent(view)}}`;
    const res = await fetch(url);

    if (!res.ok) {{
      const t = await res.text().catch(() => "");
      throw new Error(`fetch failed: ${{res.status}}\\n${{t}}`);
    }}

    cache = await res.json();
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
      refresh: refreshSec
    }});
    window.location.href = `/dashboard?${{qs.toString()}}`;
  }});

  document.getElementById("btnRefresh").addEventListener("click", () => {{
    fetchData().catch(e => {{
      console.error(e);
      document.getElementById("err").textContent = String(e);
      const tbody = document.getElementById("tbody");
      tbody.innerHTML = `<tr><td colspan="6" class="muted">Error loading</td></tr>`;
    }});
  }});

  document.getElementById("searchBox").addEventListener("input", () => {{
    render();
  }});

  let timer = null;
  function setupAutoRefresh() {{
    if (timer) clearInterval(timer);
    const sec = parseInt(document.getElementById("refreshSec").value || "0", 10);
    if (sec > 0) {{
      timer = setInterval(() => {{
        fetchData().catch(e => {{
          console.error(e);
          document.getElementById("err").textContent = String(e);
        }});
      }}, sec * 1000);
    }}
  }}
  document.getElementById("refreshSec").addEventListener("change", setupAutoRefresh);

  fetchData().then(setupAutoRefresh).catch(e => {{
    console.error(e);
    document.getElementById("err").textContent = String(e);
    const tbody = document.getElementById("tbody");
    tbody.innerHTML = `<tr><td colspan="6" class="muted">Error loading</td></tr>`;
  }});
</script>

</body>
</html>
"""
    return HTMLResponse(html)


# ============================================================
# Followup job (optional)
# ============================================================

def utcnow() -> datetime:
    return datetime.now(timezone.utc)


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


@app.post("/jobs/followup")
async def job_followup(
    _: None = Depends(require_admin_or_basic),
):
    """
    Find HOT customers (stable_level>=FOLLOWUP_MIN_LEVEL) and
    who haven't been updated in FOLLOWUP_AFTER_MINUTES, then (optionally) send followup.
    MVP: Just return candidates and log.
    """

    if not FOLLOWUP_ENABLED:
        return {"ok": True, "enabled": False, "reason": "FOLLOWUP_ENABLED!=1"}

    if not acquire_job_lock("followup", FOLLOWUP_LOCK_TTL_SEC):
        return {"ok": True, "enabled": True, "skipped": True, "reason": "locked"}

    threshold = utcnow() - timedelta(minutes=FOLLOWUP_AFTER_MINUTES)

    rows = db_fetchall(
        """
        SELECT shop_id, conv_key, user_id, temp_level_stable, confidence, next_goal, updated_at, last_user_text
        FROM customers
        WHERE shop_id=%s
          AND temp_level_stable >= %s
          AND updated_at < %s
        ORDER BY updated_at ASC
        LIMIT %s
        """,
        (SHOP_ID, FOLLOWUP_MIN_LEVEL, threshold, FOLLOWUP_LIMIT),
    )

    candidates = []
    for r in rows:
        candidates.append(
            {
                "shop_id": r[0],
                "conv_key": r[1],
                "user_id": r[2],
                "temp_level_stable": r[3],
                "confidence": float(r[4]) if r[4] is not None else None,
                "next_goal": r[5],
                "updated_at": r[6].isoformat() if r[6] else None,
                "last_user_text": r[7],
            }
        )

    if FOLLOWUP_DRYRUN:
        print("[FOLLOWUP] DRYRUN candidates:", len(candidates))
        return {"ok": True, "enabled": True, "dryrun": True, "candidates": candidates}

    print("[FOLLOWUP] candidates:", len(candidates))
    return {"ok": True, "enabled": True, "candidates": candidates}
