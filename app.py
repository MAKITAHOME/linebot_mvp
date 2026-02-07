# app.py (FULL - push: next_goal is DB truth from LLM analyze; phase2 followup LLM + phase3 timing learning)
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
FOLLOWUP_SECOND_TOUCH_AFTER_HOURS = int(os.getenv("FOLLOWUP_SECOND_TOUCH_AFTER_HOURS", "48"))
FOLLOWUP_SECOND_TOUCH_LIMIT = int(os.getenv("FOLLOWUP_SECOND_TOUCH_LIMIT", "50"))

# OpenAI models / timeouts
OPENAI_MODEL_ASSISTANT = os.getenv("OPENAI_MODEL_ASSISTANT", "gpt-4o-mini").strip()  # 返信生成
OPENAI_MODEL_ANALYZE = os.getenv("OPENAI_MODEL_ANALYZE", "gpt-4o-mini").strip()      # 温度判定
OPENAI_MODEL_FOLLOWUP = os.getenv("OPENAI_MODEL_FOLLOWUP", "gpt-4o-mini").strip()    # 追客生成
ANALYZE_TIMEOUT_SEC = float(os.getenv("ANALYZE_TIMEOUT_SEC", "8.5"))
FOLLOWUP_LLM_TIMEOUT_SEC = float(os.getenv("FOLLOWUP_LLM_TIMEOUT_SEC", "8.5"))
RESPONSES_API_URL = "https://api.openai.com/v1/responses"

# followup llm switch
FOLLOWUP_USE_LLM = os.getenv("FOLLOWUP_USE_LLM", "1").strip() != "0"

# timing learning params
FOLLOWUP_TIME_MATCH_HOURS = int(os.getenv("FOLLOWUP_TIME_MATCH_HOURS", "1"))
FOLLOWUP_FORCE_SEND_AFTER_HOURS = int(os.getenv("FOLLOWUP_FORCE_SEND_AFTER_HOURS", "12"))
PREF_HOUR_LOOKBACK_DAYS = int(os.getenv("PREF_HOUR_LOOKBACK_DAYS", "60"))
PREF_HOUR_MIN_SAMPLES = int(os.getenv("PREF_HOUR_MIN_SAMPLES", "3"))

CHAT_HISTORY: Dict[str, deque] = defaultdict(lambda: deque(maxlen=40))
TEMP_HISTORY: Dict[str, deque] = defaultdict(lambda: deque(maxlen=5))

JST = timezone(timedelta(hours=9))

app = FastAPI(title="linebot_mvp", version="1.0.0")


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

    params = {}
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

    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS user_id TEXT;""")
    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS last_user_text TEXT;""")
    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS temp_level_raw INT;""")
    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS temp_level_stable INT;""")
    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS confidence REAL;""")
    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS next_goal TEXT;""")

    # NEW: intent + preferred hour
    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS intent TEXT;""")
    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS pref_hour_jst INT;""")
    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS pref_hour_samples INT;""")
    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS pref_hour_updated_at TIMESTAMPTZ;""")

    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS status TEXT;""")
    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS opt_out BOOLEAN;""")
    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS opt_out_at TIMESTAMPTZ;""")

    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS visit_slot_selected TEXT;""")
    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS visit_slot_selected_at TIMESTAMPTZ;""")

    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS slot_budget TEXT;""")
    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS slot_area TEXT;""")
    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS slot_move_in TEXT;""")
    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS slot_layout TEXT;""")
    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS slots_json TEXT;""")

    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS need_reply BOOLEAN DEFAULT FALSE;""")
    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS need_reply_reason TEXT;""")
    cur.execute("""ALTER TABLE customers ADD COLUMN IF NOT EXISTS need_reply_updated_at TIMESTAMPTZ;""")

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
    cur.execute("""ALTER TABLE messages ADD COLUMN IF NOT EXISTS intent TEXT;""")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS job_locks (
          key TEXT PRIMARY KEY,
          locked_until TIMESTAMPTZ NOT NULL
        );
        """
    )

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
    cur.execute("""ALTER TABLE followup_logs ADD COLUMN IF NOT EXISTS stage INT;""")
    cur.execute("""ALTER TABLE followup_logs ADD COLUMN IF NOT EXISTS send_hour_jst INT;""")

    cur.execute("""CREATE INDEX IF NOT EXISTS idx_customers_shop_updated ON customers(shop_id, updated_at DESC);""")
    cur.execute("""CREATE INDEX IF NOT EXISTS idx_messages_conv_created ON messages(conv_key, created_at DESC);""")
    cur.execute("""CREATE INDEX IF NOT EXISTS idx_followup_shop_conv_created ON followup_logs(shop_id, conv_key, created_at DESC);""")
    cur.execute("""CREATE INDEX IF NOT EXISTS idx_customers_need_reply ON customers(shop_id, need_reply, updated_at DESC);""")
    cur.execute("""CREATE INDEX IF NOT EXISTS idx_messages_shop_conv_role_created ON messages(shop_id, conv_key, role, created_at DESC);""")

    cur.execute("""UPDATE customers SET need_reply=FALSE WHERE need_reply IS NULL;""")

    cur.close()
    conn.close()


@app.on_event("startup")
async def on_startup():
    ensure_tables_and_columns()
    print("[BOOT] tables/columns ensured")


# ============================================================
# need_reply (DB truth)
# ============================================================

def set_need_reply(shop_id: str, conv_key: str, need: bool, reason: str = "") -> None:
    if not DATABASE_URL:
        return
    db_execute(
        """
        UPDATE customers
        SET need_reply=%s, need_reply_reason=%s, need_reply_updated_at=now(), updated_at=now()
        WHERE shop_id=%s AND conv_key=%s
        """,
        (bool(need), (reason or "")[:120], shop_id, conv_key),
    )


def get_customer_flags(shop_id: str, conv_key: str) -> Dict[str, Any]:
    if not DATABASE_URL:
        return {}
    rows = db_fetchall(
        """
        SELECT visit_slot_selected, slot_budget, slot_area, slot_move_in, slot_layout,
               COALESCE(status,'ACTIVE'), COALESCE(opt_out,FALSE)
        FROM customers
        WHERE shop_id=%s AND conv_key=%s
        """,
        (shop_id, conv_key),
    )
    if not rows:
        return {}
    vsel, b, a, m, l, st, opt = rows[0]
    return {
        "visit_slot_selected": vsel,
        "slot_budget": b,
        "slot_area": a,
        "slot_move_in": m,
        "slot_layout": l,
        "status": (st or "ACTIVE"),
        "opt_out": bool(opt),
    }


def compute_need_reply(next_goal: str, flags: Dict[str, Any], assistant_text: str = "") -> Tuple[bool, str]:
    goal = (next_goal or "").strip()
    st = (flags.get("status") or "ACTIVE").upper()
    if flags.get("opt_out") or st in ("OPTOUT", "LOST"):
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

    if "要件確認" in goal:
        missing = [k for k, v in [("budget", budget), ("area", area), ("move_in", move_in), ("layout", layout)] if not v]
        if missing:
            return True, "need_slots"
        return False, "slots_ok"

    if "？" in (assistant_text or "") or "?" in (assistant_text or ""):
        return True, "assistant_question"

    return False, "no_need"


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
                print("[LINE] reply failed:", r.status_code, r.text[:300])
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
                print("[LINE] push failed:", r.status_code, r.text[:300])
    except Exception as e:
        print("[LINE] push exception:", repr(e))


# ============================================================
# OpenAI - chat completions (reply) + responses api (analyze/followup)
# ============================================================

OPENAI_CHAT_URL = "https://api.openai.com/v1/chat/completions"

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

【重要：レベル基準（厳守）】
Lv10: 申込/審査/契約の話が明確、または内見日程が具体的に確定
Lv9 : 内見したい＋日程調整に入っている（候補日が出ている等）
Lv8 : 条件がほぼ確定（エリア/予算/入居時期が揃う）＋内見意思が強い
Lv7 : 条件がかなり具体（エリアor沿線、予算、入居時期のうち2つ以上）＋前向きな質問
Lv6 : 条件が一部具体（上のうち1つ）＋検討継続が明確
Lv5 : 一般質問中心、条件が曖昧、温度不明
Lv4 : 情報収集段階が明確（比較中/とりあえず）で条件未確定
Lv3 : 反応が薄い/曖昧/先すぎる（半年以上先など）/冷めている
Lv2 : 反応が薄い/要件なし/冷めている
Lv1 : 明確に不要、拒否、ブロック示唆

【next_goalの決め方（重要）】
- 目標は「次の1歩を進めるために、今聞くべき情報」。
- 例: 内見系なら「候補日/日程確定」「別日希望の確認」。
- 条件不足なら「予算」「エリア」「入居時期」「間取り」など、最も重要な不足を優先。
- 質問は最大2つに収まる形のゴールにする。

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

ANALYZE_JSON_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "temp_level_raw": {"type": "integer", "minimum": 1, "maximum": 10},
        "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
        "intent": {"type": "string", "enum": ["rent", "buy", "invest", "research", "other"]},
        "next_goal": {"type": "string", "maxLength": 80},
        "reasons": {
            "type": "array",
            "items": {"type": "string", "maxLength": 60},
            "minItems": 0,
            "maxItems": 3
        }
    },
    "required": ["temp_level_raw", "confidence", "intent", "next_goal", "reasons"]
}

FOLLOWUP_JSON_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "message": {"type": "string", "maxLength": 900}
    },
    "required": ["message"]
}


async def openai_chat(messages: List[Dict[str, str]], temperature: float = 0.2, timeout_sec: float = 25.0) -> str:
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is missing")
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    payload = {"model": OPENAI_MODEL_ASSISTANT, "messages": messages, "temperature": temperature}
    async with httpx.AsyncClient(timeout=timeout_sec, verify=certifi.where()) as client:
        r = await client.post(OPENAI_CHAT_URL, headers=headers, json=payload)
        r.raise_for_status()
        data = r.json()
        return data["choices"][0]["message"]["content"]


def _responses_extract_output_text(data: Dict[str, Any]) -> Optional[str]:
    output_items = data.get("output", []) or []
    chunks: List[str] = []
    for item in output_items:
        if item.get("type") == "message":
            content = item.get("content", []) or []
            for c in content:
                if c.get("type") == "output_text" and isinstance(c.get("text"), str):
                    chunks.append(c["text"])
    if not chunks:
        return None
    return "\n".join(chunks).strip()


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
        "text": {
            "format": {
                "type": "json_schema",
                "strict": True,
                "schema": schema
            }
        }
    }
    try:
        async with httpx.AsyncClient(timeout=timeout_sec, verify=certifi.where()) as client:
            r = await client.post(RESPONSES_API_URL, headers=headers, json=payload)
            if r.status_code >= 400:
                print("[OPENAI] responses failed:", r.status_code, (r.text or "")[:240])
                return None
            data = r.json()
        out = _responses_extract_output_text(data)
        if not out:
            return None
        return json.loads(out)
    except Exception as e:
        print("[OPENAI] responses exception:", repr(e))
        return None


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


def coerce_intent(v: Any) -> str:
    s = str(v or "").strip().lower()
    if s in ("rent", "buy", "invest", "research", "other"):
        return s
    return "other"


def coerce_goal(v: Any) -> str:
    s = str(v or "").strip()
    if not s:
        return "要件確認"
    return s[:80]


# ============================================================
# Utility: slots
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
    if not DATABASE_URL:
        return {}
    rows = db_fetchall("SELECT slots_json FROM customers WHERE shop_id=%s AND conv_key=%s", (shop_id, conv_key))
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
        (slots.get("budget"), slots.get("area"), slots.get("move_in"), slots.get("layout"), sj, shop_id, conv_key),
    )


# ============================================================
# Preferred send hour learning (Phase 3)
# ============================================================

def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def now_jst() -> datetime:
    return datetime.now(JST)


def _to_jst(dt: datetime) -> datetime:
    try:
        return dt.astimezone(JST)
    except Exception:
        return dt


def learn_pref_hour_from_messages(shop_id: str, conv_key: str) -> Tuple[Optional[int], int]:
    if not DATABASE_URL:
        return None, 0
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
        j = _to_jst(dt)
        hours.append(int(j.hour))
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
    if not DATABASE_URL:
        return
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
        if 0 <= h <= 23:
            return h
    except Exception:
        pass
    return 19


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
# Utility: customer status
# ============================================================

def ensure_customer_row(shop_id: str, conv_key: str, user_id: str) -> None:
    if not DATABASE_URL:
        return
    db_execute(
        """
        INSERT INTO customers (shop_id, conv_key, user_id, updated_at, status, need_reply, need_reply_reason, need_reply_updated_at)
        VALUES (%s, %s, %s, now(), 'ACTIVE', FALSE, '', now())
        ON CONFLICT (shop_id, conv_key)
        DO UPDATE SET user_id=EXCLUDED.user_id, updated_at=now()
        """,
        (shop_id, conv_key, user_id),
    )


def mark_opt_out(shop_id: str, conv_key: str, user_id: str) -> None:
    if not DATABASE_URL:
        return
    db_execute(
        """
        UPDATE customers
        SET opt_out=TRUE, opt_out_at=now(), status='OPTOUT', user_id=%s,
            need_reply=FALSE, need_reply_reason='optout', need_reply_updated_at=now(),
            updated_at=now()
        WHERE shop_id=%s AND conv_key=%s
        """,
        (user_id, shop_id, conv_key),
    )


def mark_lost(shop_id: str, conv_key: str) -> None:
    if not DATABASE_URL:
        return
    db_execute(
        """
        UPDATE customers
        SET status='LOST',
            need_reply=FALSE, need_reply_reason='lost', need_reply_updated_at=now(),
            updated_at=now()
        WHERE shop_id=%s AND conv_key=%s
        """,
        (shop_id, conv_key),
    )


def mark_cold(shop_id: str, conv_key: str) -> None:
    if not DATABASE_URL:
        return
    db_execute(
        """
        UPDATE customers
        SET status='COLD',
            need_reply=FALSE, need_reply_reason='cold', need_reply_updated_at=now(),
            updated_at=now()
        WHERE shop_id=%s AND conv_key=%s
        """,
        (shop_id, conv_key),
    )


def revive_if_cold(shop_id: str, conv_key: str) -> None:
    if not DATABASE_URL:
        return
    db_execute(
        """
        UPDATE customers
        SET status='ACTIVE', updated_at=now()
        WHERE shop_id=%s AND conv_key=%s
          AND COALESCE(status,'ACTIVE')='COLD'
          AND COALESCE(opt_out,FALSE)=FALSE
        """,
        (shop_id, conv_key),
    )


def revive_if_lost_by_keywords(shop_id: str, conv_key: str, text: str) -> bool:
    if not DATABASE_URL:
        return False
    rows = db_fetchall(
        "SELECT COALESCE(opt_out,FALSE), COALESCE(status,'ACTIVE') FROM customers WHERE shop_id=%s AND conv_key=%s",
        (shop_id, conv_key),
    )
    if not rows:
        return False
    if bool(rows[0][0]):
        return False
    if (rows[0][1] or "ACTIVE").upper() != "LOST":
        return False

    for pat in LOST_REVIVE_PATTERNS:
        if re.search(pat, text or ""):
            db_execute(
                "UPDATE customers SET status='ACTIVE', updated_at=now() WHERE shop_id=%s AND conv_key=%s",
                (shop_id, conv_key),
            )
            return True
    return False


def is_inactive(shop_id: str, conv_key: str) -> bool:
    if not DATABASE_URL:
        return False
    rows = db_fetchall(
        "SELECT COALESCE(opt_out,FALSE), COALESCE(status,'ACTIVE') FROM customers WHERE shop_id=%s AND conv_key=%s",
        (shop_id, conv_key),
    )
    if not rows:
        return False
    opt_out = bool(rows[0][0])
    st = (rows[0][1] or "ACTIVE").upper()
    return opt_out or st in ("COLD", "LOST", "OPTOUT")


def set_visit_slot(shop_id: str, conv_key: str, slot_text: str) -> None:
    if not DATABASE_URL:
        return
    db_execute(
        "UPDATE customers SET visit_slot_selected=%s, visit_slot_selected_at=now(), updated_at=now() WHERE shop_id=%s AND conv_key=%s",
        (slot_text, shop_id, conv_key),
    )


# ============================================================
# Utility: messages + scoring persistence
# ============================================================

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
    if not DATABASE_URL:
        return
    db_execute(
        """
        INSERT INTO messages (shop_id, conv_key, role, content, temp_level_raw, temp_level_stable, confidence, intent, next_goal)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            shop_id,
            conv_key,
            role,
            content,
            temp_level_raw,
            temp_level_stable,
            confidence,
            (intent[:24] if intent else None),
            (next_goal[:120] if next_goal else None),
        ),
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
    intent: str,
    next_goal: str,
) -> None:
    if not DATABASE_URL:
        return
    db_execute(
        """
        INSERT INTO customers
          (shop_id, conv_key, user_id, last_user_text, temp_level_raw, temp_level_stable, confidence, intent, next_goal, updated_at, status)
        VALUES
          (%s,%s,%s,%s,%s,%s,%s,%s,%s,now(), COALESCE((SELECT status FROM customers WHERE shop_id=%s AND conv_key=%s),'ACTIVE'))
        ON CONFLICT (shop_id, conv_key)
        DO UPDATE SET
          user_id=EXCLUDED.user_id,
          last_user_text=EXCLUDED.last_user_text,
          temp_level_raw=EXCLUDED.temp_level_raw,
          temp_level_stable=EXCLUDED.temp_level_stable,
          confidence=EXCLUDED.confidence,
          intent=EXCLUDED.intent,
          next_goal=EXCLUDED.next_goal,
          updated_at=now()
        """,
        (shop_id, conv_key, user_id, last_user_text, raw_level, stable_level, confidence, intent, next_goal, shop_id, conv_key),
    )


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


def get_recent_conversation_for_followup(shop_id: str, conv_key: str, limit: int = 10) -> List[Dict[str, str]]:
    return get_recent_conversation(shop_id, conv_key, limit=max(6, min(20, int(limit))))


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
    send_hour_jst: Optional[int] = None,
) -> None:
    if not DATABASE_URL:
        return
    db_execute(
        """
        INSERT INTO followup_logs (shop_id, conv_key, user_id, message, mode, status, error, variant, stage, send_hour_jst)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            shop_id,
            conv_key,
            user_id,
            message,
            mode,
            status,
            (error or None),
            (variant or None),
            (stage or None),
            (send_hour_jst if send_hour_jst is not None else None),
        ),
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
# Visit slots & parsing
# ============================================================

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
# AB selection & fallback followup templates
# ============================================================

def pick_ab_variant(conv_key: str) -> str:
    if not FOLLOWUP_AB_ENABLED:
        return "A"
    h = hashlib.sha256(conv_key.encode("utf-8")).hexdigest()
    return "A" if (int(h[:2], 16) % 2 == 0) else "B"


def build_followup_template_ab_fallback(variant: str, next_goal: str, last_user_text: str, level: int) -> str:
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
        return body.strip()[:900]

    lead = "少しだけ確認です。"
    yn = "①番号でOK ②別日希望 ③一旦ストップ" if is_visit else "①このまま探す ②一旦ストップ ③条件変更"
    body = f"{lead}\n{slot_lines}{q}\n返信は「{yn}」のどれでもOKです。"
    return body.strip()[:900]


def build_second_touch_message_fallback(next_goal: str) -> str:
    if any(k in (next_goal or "") for k in ["内見", "候補日", "日程"]):
        return (
            "その後いかがでしょうか？\n"
            "ご都合が合う時で大丈夫なので、内見希望なら「1〜6」か「別日」とだけ返信ください。"
        )[:900]
    return (
        "その後いかがでしょうか？\n"
        "急ぎでなければ大丈夫です。必要になったら一言だけ返信ください🙂"
    )[:900]


# ============================================================
# Phase 2: LLM followup generation
# ============================================================

def _intent_label(intent: str) -> str:
    return {"rent": "賃貸", "buy": "購入", "invest": "投資", "research": "情報収集", "other": "不明"}.get(intent, "不明")


async def generate_followup_message_llm(
    shop_id: str,
    conv_key: str,
    stage: int,
    variant: str,
    customer: Dict[str, Any],
) -> Optional[str]:
    if not FOLLOWUP_USE_LLM or not OPENAI_API_KEY:
        return None

    level = int(customer.get("level") or 0)
    next_goal = (customer.get("next_goal") or "").strip()
    last_user_text = (customer.get("last_user_text") or "").strip()
    intent = (customer.get("intent") or "other").strip().lower()

    flags = get_customer_flags(shop_id, conv_key)
    slots = get_customer_slots(shop_id, conv_key)
    visit_selected = flags.get("visit_slot_selected")

    visit_slots = upcoming_visit_slots_jst(VISIT_DAYS_AHEAD)
    visit_block = ""
    if any(k in next_goal for k in ["内見", "候補日", "日程"]):
        if visit_selected == "REQUEST_CHANGE":
            visit_block = "ユーザーは『別日/別時間』希望。候補番号ではなく希望の曜日/時間帯を聞く。"
        else:
            visit_block = "内見候補(1〜6): " + " / ".join([f"{i+1}:{s}" for i, s in enumerate(visit_slots)])

    style = "丁寧でやさしく、少しフレンドリー" if variant == "A" else "短く、選択肢形式で迷わせない"
    stage_rule = "初回追客（距離感は近すぎない）" if stage == 1 else "2回目追客（しつこくならない・一言で終える）"

    instructions = (
        "あなたは不動産/太陽光/投資の問い合わせ対応のトップ営業です。"
        "次の条件を厳守し、追客LINE文を1通だけ作ってください。"
        "出力はJSONのみ。\n\n"
        "【厳守】\n"
        "- 押し売り禁止。丁寧。短く。\n"
        "- 質問は最大2つ。\n"
        "- 返信しやすい形（番号/選択肢/一言でOK）を優先。\n"
        "- 次のゴール(next_goal)に沿う。\n"
        "- intent（賃貸/購入/投資/情報収集）を踏まえて言い回しを変える。\n"
        "- 文字数は短め（~350字目安）。\n"
    )

    history = get_recent_conversation_for_followup(shop_id, conv_key, limit=12)
    context_user = {
        "stage": stage,
        "ab_variant": variant,
        "style": style,
        "stage_rule": stage_rule,
        "intent": intent,
        "intent_label": _intent_label(intent),
        "level": level,
        "next_goal": next_goal,
        "last_user_text": last_user_text[:200],
        "known_slots": slots,
        "visit_selected": visit_selected,
        "visit_block": visit_block,
        "rules_hint": "内見/日程なら番号1-6か別日希望を促す。要件確認なら不足スロットを埋める。",
    }

    input_msgs: List[Dict[str, str]] = []
    input_msgs.append({"role": "user", "content": f"コンテキスト: {json.dumps(context_user, ensure_ascii=False)}"})
    input_msgs.append({"role": "user", "content": "直近の会話（古い→新しい）:"})
    for m in history[-12:]:
        role = m.get("role")
        content = (m.get("content") or "")[:800]
        if role in ("user", "assistant"):
            input_msgs.append({"role": role, "content": content})

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
    if not msg:
        return None
    return msg[:900]


# ============================================================
# Phase 1: analyze (Responses structured) + reply generation
# ============================================================

async def analyze_only(shop_id: str, conv_key: str, user_text: str) -> Tuple[int, float, str, str, List[str], Dict[str, str], str]:
    """
    return: raw_level, conf, intent, next_goal, reasons, merged_slots, status_override
    """
    t = (user_text or "").strip()

    for pat in OPTOUT_PATTERNS:
        if re.search(pat, t, flags=re.IGNORECASE):
            return 1, 0.95, "other", "関係終了確認", ["配信停止/連絡不要の意思"], {}, "OPTOUT"

    for pat in CANCEL_PATTERNS:
        if re.search(pat, t):
            return 2, 0.90, "other", "関係終了確認", ["キャンセル/拒否の明確表現"], {}, "LOST"

    new_slots = extract_slots(user_text)
    prev_slots = get_customer_slots(shop_id, conv_key)
    merged = merge_slots(prev_slots, new_slots)

    history = get_recent_conversation(shop_id, conv_key, ANALYZE_HISTORY_LIMIT)
    if not history or history[-1].get("content") != user_text:
        history.append({"role": "user", "content": user_text})

    raw_level = 5
    conf = 0.6
    intent = "other"
    next_goal = "要件確認"
    reasons: List[str] = []

    input_msgs: List[Dict[str, str]] = []
    if merged:
        input_msgs.append({"role": "user", "content": f"抽出スロット(参考): {json.dumps(merged, ensure_ascii=False)}"})
    input_msgs.extend(history[-max(2, ANALYZE_HISTORY_LIMIT):])

    j = await openai_responses_json(
        model=OPENAI_MODEL_ANALYZE,
        instructions=SYSTEM_PROMPT_ANALYZE.strip(),
        input_msgs=input_msgs,
        schema=ANALYZE_JSON_SCHEMA,
        timeout_sec=ANALYZE_TIMEOUT_SEC,
    )

    if j:
        raw_level = coerce_level(j.get("temp_level_raw", 5))
        conf = coerce_confidence(j.get("confidence", 0.6))
        intent = coerce_intent(j.get("intent", "other"))
        next_goal = coerce_goal(j.get("next_goal", "要件確認"))
        rs = j.get("reasons", [])
        if isinstance(rs, list):
            reasons = [str(x).strip()[:60] for x in rs if str(x).strip()][:3]
        return raw_level, conf, intent, next_goal, reasons, merged, ""

    if len(t) <= SHORT_TEXT_MAX_LEN:
        return 3, 0.75, "other", "要件確認", ["短文で情報不足（fallback）"], merged, ""

    return raw_level, conf, intent, next_goal, reasons, merged, ""


async def generate_reply_only(user_id: str, user_text: str) -> str:
    history = CHAT_HISTORY[user_id]
    ctx = [{"role": "system", "content": SYSTEM_PROMPT_ASSISTANT}]
    for role, content in list(history)[-10:]:
        ctx.append({"role": role, "content": content})
    ctx.append({"role": "user", "content": user_text})

    reply_text = await openai_chat(ctx, temperature=0.35, timeout_sec=FAST_REPLY_TIMEOUT_SEC)
    reply_text = (reply_text or "").strip()
    if not reply_text:
        reply_text = "ありがとうございます。条件をもう少し教えてください（エリア/予算/間取り/入居時期など）。"

    history.append(("user", user_text))
    history.append(("assistant", reply_text))
    return reply_text


# ============================================================
# Status maintenance
# ============================================================

def maintenance_update_statuses(shop_id: str) -> None:
    if not DATABASE_URL:
        return
    db_execute(
        """
        UPDATE customers SET status='OPTOUT', need_reply=FALSE, need_reply_reason='optout'
        WHERE shop_id=%s AND COALESCE(opt_out,FALSE)=TRUE AND COALESCE(status,'')<>'OPTOUT'
        """,
        (shop_id,),
    )
    db_execute(
        """
        UPDATE customers SET status='COLD', need_reply=FALSE, need_reply_reason='cold'
        WHERE shop_id=%s
          AND COALESCE(status,'ACTIVE')='ACTIVE'
          AND COALESCE(temp_level_stable,0) <= 2
          AND updated_at < (now() - interval '7 days')
        """,
        (shop_id,),
    )


# ============================================================
# Followup job lock
# ============================================================

def acquire_job_lock(key: str, ttl_sec: int) -> bool:
    now = utcnow()
    until = now + timedelta(seconds=ttl_sec)
    rows = db_fetchall("SELECT locked_until FROM job_locks WHERE key=%s", (key,))
    if rows and rows[0][0] and rows[0][0] > now:
        return False
    db_execute(
        """
        INSERT INTO job_locks (key, locked_until)
        VALUES (%s, %s)
        ON CONFLICT (key) DO UPDATE SET locked_until=EXCLUDED.locked_until
        """,
        (key, until),
    )
    return True


# ============================================================
# Followup candidate queries
# ============================================================

def get_followup_candidates_stage1() -> List[Dict[str, Any]]:
    if not DATABASE_URL:
        return []
    threshold = utcnow() - timedelta(minutes=FOLLOWUP_AFTER_MINUTES)

    rows = db_fetchall(
        """
        SELECT conv_key, user_id,
               COALESCE(temp_level_stable,0), COALESCE(next_goal,''), COALESCE(last_user_text,''),
               COALESCE(intent,'other'),
               COALESCE(status,'ACTIVE'), COALESCE(opt_out,FALSE),
               pref_hour_jst,
               updated_at
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

    out: List[Dict[str, Any]] = []
    for conv_key, user_id, lvl, goal, last_text, intent, st, opt, pref_hour_jst, updated_at in rows:
        st = (st or "ACTIVE").upper()
        if opt or st in ("COLD", "LOST", "OPTOUT"):
            continue
        out.append(
            {
                "conv_key": conv_key,
                "user_id": user_id,
                "level": int(lvl or 0),
                "next_goal": goal or "",
                "last_user_text": last_text or "",
                "intent": (intent or "other"),
                "pref_hour_jst": pref_hour_jst,
                "updated_at": updated_at,
            }
        )
    return out


def get_followup_candidates_stage2() -> List[Dict[str, Any]]:
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
        if is_inactive(SHOP_ID, conv_key):
            continue
        cg = db_fetchall(
            """
            SELECT COALESCE(next_goal,''), COALESCE(intent,'other'), COALESCE(last_user_text,''), COALESCE(temp_level_stable,0),
                   pref_hour_jst, updated_at
            FROM customers
            WHERE shop_id=%s AND conv_key=%s
            LIMIT 1
            """,
            (SHOP_ID, conv_key),
        )
        goal = cg[0][0] if cg else ""
        intent = cg[0][1] if cg else "other"
        last_text = cg[0][2] if cg else ""
        lvl = int(cg[0][3] or 0) if cg else 0
        pref_hour_jst = cg[0][4] if cg else None
        updated_at = cg[0][5] if cg else None
        out.append(
            {
                "conv_key": conv_key,
                "user_id": user_id,
                "next_goal": goal or "",
                "intent": intent or "other",
                "last_user_text": last_text or "",
                "level": lvl,
                "pref_hour_jst": pref_hour_jst,
                "updated_at": updated_at,
            }
        )
    return out


# ============================================================
# Background tasks
# ============================================================

async def process_analysis_only_store(shop_id: str, user_id: str, conv_key: str, user_text: str, reply_text: str) -> None:
    try:
        raw_level, conf, intent, next_goal, reasons, merged_slots, status_override = await analyze_only(shop_id, conv_key, user_text)
        stable_level = stable_from_history(conv_key, raw_level)

        if status_override == "OPTOUT":
            mark_opt_out(shop_id, conv_key, user_id)
            return
        elif status_override == "LOST":
            mark_lost(shop_id, conv_key)
            return

        if merged_slots:
            set_customer_slots(shop_id, conv_key, merged_slots)

        # ✅ next_goal is DB truth from LLM
        upsert_customer_state(shop_id, conv_key, user_id, user_text, raw_level, stable_level, conf, intent, next_goal)
        save_message(shop_id, conv_key, "assistant", reply_text, raw_level, stable_level, conf, intent, next_goal)

        flags = get_customer_flags(shop_id, conv_key)
        need, reason = compute_need_reply(next_goal, flags, assistant_text=reply_text)
        set_need_reply(shop_id, conv_key, need, reason)
    except Exception as e:
        print("[BG] process_analysis_only_store:", repr(e))


async def process_ai_and_push_full(shop_id: str, user_id: str, conv_key: str, user_text: str) -> None:
    try:
        raw_level, conf, intent, next_goal, reasons, merged_slots, status_override = await analyze_only(shop_id, conv_key, user_text)
        stable_level = stable_from_history(conv_key, raw_level)

        if status_override == "OPTOUT":
            mark_opt_out(shop_id, conv_key, user_id)
            return
        elif status_override == "LOST":
            mark_lost(shop_id, conv_key)
            return

        if merged_slots:
            set_customer_slots(shop_id, conv_key, merged_slots)

        try:
            reply_text = await openai_chat(
                [{"role": "system", "content": SYSTEM_PROMPT_ASSISTANT}, {"role": "user", "content": user_text}],
                temperature=0.35,
                timeout_sec=20.0,
            )
            reply_text = (reply_text or "").strip() or "ありがとうございます。条件をもう少し教えてください（エリア/予算/間取り/入居時期など）。"
        except Exception:
            reply_text = "ありがとうございます。条件をもう少し教えてください（エリア/予算/間取り/入居時期など）。"

        # ✅ next_goal is DB truth from LLM
        upsert_customer_state(shop_id, conv_key, user_id, user_text, raw_level, stable_level, conf, intent, next_goal)
        save_message(shop_id, conv_key, "assistant", reply_text, raw_level, stable_level, conf, intent, next_goal)

        flags = get_customer_flags(shop_id, conv_key)
        need, reason = compute_need_reply(next_goal, flags, assistant_text=reply_text)
        set_need_reply(shop_id, conv_key, need, reason)

        if is_inactive(shop_id, conv_key):
            return

        await push_line(user_id, reply_text)
    except Exception as e:
        print("[BG] process_ai_and_push_full:", repr(e))


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

        try:
            ensure_customer_row(SHOP_ID, conv_key, user_id)
        except Exception as e:
            print("[DB] ensure_customer_row:", repr(e))

        try:
            set_need_reply(SHOP_ID, conv_key, False, "user_replied")
        except Exception:
            pass

        try:
            revive_if_cold(SHOP_ID, conv_key)
        except Exception:
            pass

        try:
            revive_if_lost_by_keywords(SHOP_ID, conv_key, user_text)
        except Exception:
            pass

        try:
            save_message(SHOP_ID, conv_key, "user", user_text)
        except Exception as e:
            print("[DB] save user:", repr(e))

        try:
            update_customer_pref_hour(SHOP_ID, conv_key)
        except Exception as e:
            print("[PREF] update_customer_pref_hour:", repr(e))

        try:
            attribute_followup_response(SHOP_ID, conv_key)
        except Exception:
            pass

        try:
            prev = get_customer_slots(SHOP_ID, conv_key)
            merged = merge_slots(prev, extract_slots(user_text))
            if merged and merged != prev:
                set_customer_slots(SHOP_ID, conv_key, merged)
        except Exception:
            pass

        if is_visit_change_request(user_text):
            set_visit_slot(SHOP_ID, conv_key, "REQUEST_CHANGE")
            try:
                set_need_reply(SHOP_ID, conv_key, True, "visit_change_request")
            except Exception:
                pass
            await reply_line(reply_token, "承知しました。ご希望の曜日や時間帯（例：平日夜/土日午後など）を教えてください。")
            continue

        sel = parse_slot_selection(user_text)
        if sel is not None:
            slots = upcoming_visit_slots_jst(VISIT_DAYS_AHEAD)
            if 1 <= sel <= len(slots):
                picked = slots[sel - 1]
                set_visit_slot(SHOP_ID, conv_key, picked)
                try:
                    set_need_reply(SHOP_ID, conv_key, False, "visit_selected")
                except Exception:
                    pass
                await reply_line(reply_token, f"ありがとうございます！内見希望枠は「{picked}」で承りました。")
            else:
                await reply_line(reply_token, "番号は 1〜6 の範囲でお願いします。")
            continue

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

        fast_reply_text: Optional[str] = None
        try:
            fast_reply_text = await asyncio.wait_for(
                generate_reply_only(user_id=user_id, user_text=user_text),
                timeout=FAST_REPLY_TIMEOUT_SEC,
            )
        except Exception:
            fast_reply_text = None

        if fast_reply_text:
            await reply_line(reply_token, fast_reply_text)
            background.add_task(process_analysis_only_store, SHOP_ID, user_id, conv_key, user_text, fast_reply_text)
        else:
            await reply_line(reply_token, "ありがとうございます！内容を確認しています。少々お待ちください😊")
            background.add_task(process_ai_and_push_full, SHOP_ID, user_id, conv_key, user_text)

    return {"ok": True}


# ============================================================
# Dashboard APIs
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
            SELECT conv_key, user_id, last_user_text, temp_level_stable, confidence, COALESCE(intent,'other'), next_goal, updated_at,
                   COALESCE(status,'ACTIVE') as status,
                   visit_slot_selected,
                   slot_budget, slot_area, slot_move_in, slot_layout,
                   COALESCE(need_reply,FALSE) as need_reply,
                   COALESCE(need_reply_reason,'') as need_reply_reason,
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
                "view": "customers",
                "conv_key": r[0],
                "user_id": r[1],
                "message": r[2],
                "temp_level_stable": r[3],
                "confidence": float(r[4]) if r[4] is not None else None,
                "intent": r[5],
                "next_goal": r[6],
                "ts": r[7].isoformat() if r[7] else None,
                "status": r[8],
                "visit_slot_selected": r[9],
                "slot_budget": r[10],
                "slot_area": r[11],
                "slot_move_in": r[12],
                "slot_layout": r[13],
                "need_reply": bool(r[14]),
                "need_reply_reason": r[15],
                "pref_hour_jst": r[16],
                "pref_hour_samples": r[17],
            }
            for r in rows
        ])

    if view == "events":
        rows = db_fetchall(
            """
            SELECT m.role, c.user_id, c.conv_key, m.content, m.created_at, m.temp_level_stable, m.confidence, COALESCE(m.intent,'other'), m.next_goal
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
                "conv_key": r[2],
                "message": r[3],
                "ts": r[4].isoformat() if r[4] else None,
                "temp_level_stable": r[5],
                "confidence": float(r[6]) if r[6] is not None else None,
                "intent": r[7],
                "next_goal": r[8],
            }
            for r in rows
        ])

    if view == "followups":
        rows = db_fetchall(
            """
            SELECT user_id, conv_key, variant, mode, status, stage, message, error, responded_at, send_hour_jst, created_at
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
                "send_hour_jst": r[9],
                "ts": r[10].isoformat() if r[10] else None,
            }
            for r in rows
        ])

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


@app.get("/api/stats/level_dist")
async def api_stats_level_dist(
    _: None = Depends(require_dashboard_key),
    shop_id: str = Query(default=SHOP_ID),
):
    if not DATABASE_URL:
        return JSONResponse({str(i): 0 for i in range(1, 11)}, status_code=200)

    rows = db_fetchall(
        """
        SELECT temp_level_stable, COUNT(*)
        FROM customers
        WHERE shop_id=%s
          AND temp_level_stable BETWEEN 1 AND 10
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
    return JSONResponse(dist, status_code=200)


# ============================================================
# Dashboard HTML (simple & stable)
# ============================================================

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(
    _: None = Depends(require_dashboard_key),
    shop_id: str = Query(default=SHOP_ID),
    min_level: int = Query(default=1, ge=1, le=10),
    limit: int = Query(default=80, ge=1, le=200),
    refresh: int = Query(default=DASHBOARD_REFRESH_SEC_DEFAULT, ge=0, le=300),
    key: Optional[str] = Query(default=None),
):
    key_q = (key or "").strip()
    html = f"""
<!doctype html>
<html lang="ja">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Dashboard | {shop_id}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
  body{{font-family:system-ui,-apple-system,"Hiragino Sans","Noto Sans JP",sans-serif;margin:16px;background:#0b1020;color:#fff}}
  .row{{display:flex;gap:12px;flex-wrap:wrap;align-items:center}}
  .card{{background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.12);border-radius:14px;padding:12px}}
  table{{width:100%;border-collapse:collapse}}
  th,td{{border-bottom:1px solid rgba(255,255,255,.12);padding:8px;font-size:12px;vertical-align:top}}
  th{{color:rgba(255,255,255,.7);text-align:left}}
  .mono{{font-family:ui-monospace,Menlo,Monaco,Consolas,monospace}}
  a{{color:#8ab4f8}}
</style>
</head>
<body>
  <div class="row">
    <div class="card"><b>SHOP</b> <span class="mono">{shop_id}</span></div>
    <div class="card"><b>min_level</b> {min_level} / <b>limit</b> {limit} / <b>refresh</b> {refresh}s</div>
  </div>

  <div class="row" style="margin-top:12px;">
    <div class="card" style="flex:1;min-width:320px;">
      <div style="display:flex;justify-content:space-between;align-items:center;">
        <b>温度分布（全体）</b><span class="mono">/api/stats/level_dist</span>
      </div>
      <canvas id="chart" height="110"></canvas>
    </div>
    <div class="card" style="flex:1;min-width:320px;">
      <div style="display:flex;justify-content:space-between;align-items:center;">
        <b>今やる（need_reply=TRUE）</b><span class="mono">customers</span>
      </div>
      <div style="margin-top:8px;overflow:auto;max-height:420px;">
        <table>
          <thead><tr>
            <th>更新</th><th>Lv</th><th>intent</th><th>need</th><th>goal</th><th>pref</th><th>user</th><th>msg</th>
          </tr></thead>
          <tbody id="needRows"><tr><td colspan="8">loading...</td></tr></tbody>
        </table>
      </div>
    </div>
  </div>

  <div class="card" style="margin-top:12px;">
    <div style="display:flex;justify-content:space-between;align-items:center;">
      <b>顧客（最新）</b><span class="mono">/api/hot?view=customers</span>
    </div>
    <div style="margin-top:8px;overflow:auto;max-height:520px;">
      <table>
        <thead><tr>
          <th>更新</th><th>Lv</th><th>conf</th><th>intent</th><th>goal</th><th>pref</th><th>status</th><th>user</th><th>msg</th>
        </tr></thead>
        <tbody id="custRows"><tr><td colspan="9">loading...</td></tr></tbody>
      </table>
    </div>
  </div>

<script>
const KEY = {json.dumps(key_q)};
const SHOP = {json.dumps(shop_id)};
const MIN = {min_level};
const LIMIT = {limit};
const REFRESH = {refresh};

function esc(s){{return (s??"").toString().replace(/[&<>"]/g,c=>({{"&":"&amp;","<":"&lt;",">":"&gt;","\\"":"&quot;"}}[c]))}}
function fmt(iso){{ if(!iso) return "-"; try{{return new Date(iso).toLocaleString()}}catch(e){{return iso}} }}
async function fetchJson(url){{ const r=await fetch(url); return await r.json(); }}

let chart=null;
async function renderDist(){{
  const dist = await fetchJson(`/api/stats/level_dist?shop_id=${{encodeURIComponent(SHOP)}}&key=${{encodeURIComponent(KEY)}}`);
  const labels = ["1","2","3","4","5","6","7","8","9","10"];
  const data = labels.map(k=>Number(dist[k]||0));
  const ctx=document.getElementById("chart");
  if(chart){{ chart.data.labels=labels; chart.data.datasets[0].data=data; chart.update(); return; }}
  chart=new Chart(ctx,{{type:'bar',data:{{labels,datasets:[{{label:'count',data,borderWidth:1}}]}},options:{{responsive:true}}}});
}}

async function renderCustomers(){{
  const rows = await fetchJson(`/api/hot?view=customers&shop_id=${{encodeURIComponent(SHOP)}}&min_level=${{encodeURIComponent(MIN)}}&limit=${{encodeURIComponent(LIMIT)}}&key=${{encodeURIComponent(KEY)}}`);
  const need = rows.filter(r=>r.need_reply);
  document.getElementById("needRows").innerHTML = need.length? need.map(r=>{{
    const link = `/dashboard/customer?shop_id=${{encodeURIComponent(SHOP)}}&conv_key=${{encodeURIComponent(r.conv_key)}}&key=${{encodeURIComponent(KEY)}}`;
    return `<tr>
      <td class="mono">${{esc(fmt(r.ts))}}</td>
      <td class="mono">${{esc(r.temp_level_stable)}}</td>
      <td class="mono">${{esc(r.intent||"-")}}</td>
      <td>${{esc(r.need_reply_reason||"need")}}</td>
      <td>${{esc(r.next_goal||"-")}}</td>
      <td class="mono">${{esc(r.pref_hour_jst??"-")}}(${{esc(r.pref_hour_samples??"-")}})</td>
      <td class="mono"><a href="${{link}}">${{esc(r.user_id||"")}}</a></td>
      <td>${{esc((r.message||"").slice(0,140))}}</td>
    </tr>`;
  }}).join("") : `<tr><td colspan="8">need_reply なし</td></tr>`;

  document.getElementById("custRows").innerHTML = rows.length? rows.map(r=>{{
    const link = `/dashboard/customer?shop_id=${{encodeURIComponent(SHOP)}}&conv_key=${{encodeURIComponent(r.conv_key)}}&key=${{encodeURIComponent(KEY)}}`;
    return `<tr>
      <td class="mono">${{esc(fmt(r.ts))}}</td>
      <td class="mono">${{esc(r.temp_level_stable)}}</td>
      <td class="mono">${{r.confidence==null?"-":Number(r.confidence).toFixed(2)}}</td>
      <td class="mono">${{esc(r.intent||"-")}}</td>
      <td>${{esc(r.next_goal||"-")}}</td>
      <td class="mono">${{esc(r.pref_hour_jst??"-")}}(${{esc(r.pref_hour_samples??"-")}})</td>
      <td class="mono">${{esc(r.status||"-")}}</td>
      <td class="mono"><a href="${{link}}">${{esc(r.user_id||"")}}</a></td>
      <td>${{esc((r.message||"").slice(0,160))}}</td>
    </tr>`;
  }}).join("") : `<tr><td colspan="9">no data</td></tr>`;
}}

async function tick(){{
  await Promise.all([renderDist(), renderCustomers()]);
}}

tick().catch(console.error);
if(REFRESH>0) setInterval(()=>tick().catch(console.error), REFRESH*1000);
</script>
</body>
</html>
"""
    return HTMLResponse(html)


# ============================================================
# Customer detail
# ============================================================

@app.get("/api/customer/detail")
async def api_customer_detail(
    _: None = Depends(require_dashboard_key),
    shop_id: str = Query(default=SHOP_ID),
    conv_key: str = Query(...),
    msg_limit: int = Query(default=120, ge=10, le=400),
    followup_limit: int = Query(default=60, ge=10, le=200),
):
    if not DATABASE_URL:
        return JSONResponse({"ok": True, "customer": None, "messages": [], "followups": []}, status_code=200)

    crow = db_fetchall(
        """
        SELECT conv_key, user_id, last_user_text, temp_level_stable, confidence, COALESCE(intent,'other'), next_goal, updated_at,
               COALESCE(status,'ACTIVE'), COALESCE(opt_out,FALSE),
               visit_slot_selected, visit_slot_selected_at,
               slot_budget, slot_area, slot_move_in, slot_layout,
               COALESCE(need_reply,FALSE), COALESCE(need_reply_reason,''), need_reply_updated_at,
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
            "visit_slot_selected": r[10],
            "visit_slot_selected_at": r[11].isoformat() if r[11] else None,
            "slot_budget": r[12],
            "slot_area": r[13],
            "slot_move_in": r[14],
            "slot_layout": r[15],
            "need_reply": bool(r[16]),
            "need_reply_reason": r[17],
            "need_reply_updated_at": r[18].isoformat() if r[18] else None,
            "pref_hour_jst": r[19],
            "pref_hour_samples": r[20],
        }

    msgs = db_fetchall(
        """
        SELECT role, content, created_at, temp_level_stable, confidence, COALESCE(intent,'other'), next_goal
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

    return JSONResponse({"ok": True, "customer": customer, "messages": messages, "followups": followups}, status_code=200)


@app.get("/dashboard/customer", response_class=HTMLResponse)
async def dashboard_customer(
    _: None = Depends(require_dashboard_key),
    shop_id: str = Query(default=SHOP_ID),
    conv_key: str = Query(...),
    key: Optional[str] = Query(default=None),
):
    key_q = (key or "").strip()
    html = f"""
<!doctype html>
<html lang="ja">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Customer | {shop_id}</title>
<style>
  body{{font-family:system-ui,-apple-system,"Hiragino Sans","Noto Sans JP",sans-serif;margin:16px;background:#0b1020;color:#fff}}
  .card{{background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.12);border-radius:14px;padding:12px;margin-bottom:12px}}
  .mono{{font-family:ui-monospace,Menlo,Monaco,Consolas,monospace}}
  a{{color:#8ab4f8}}
  .msg{{padding:8px;border-bottom:1px solid rgba(255,255,255,.12)}}
  .u{{color:#ffb020}} .a{{color:#37d67a}}
  pre{{white-space:pre-wrap}}
</style>
</head>
<body>
  <div class="card">
    <a href="/dashboard?shop_id={shop_id}&key={key_q}">← back</a>
    <div class="mono" style="margin-top:8px;">{conv_key}</div>
  </div>
  <div class="card" id="cust">loading...</div>
  <div class="card">
    <b>Messages</b>
    <div id="msgs">loading...</div>
  </div>
  <div class="card">
    <b>Followups</b>
    <div id="fls">loading...</div>
  </div>

<script>
const KEY={json.dumps(key_q)};
const SHOP={json.dumps(shop_id)};
const CONV={json.dumps(conv_key)};

function esc(s){{return (s??"").toString().replace(/[&<>"]/g,c=>({{"&":"&amp;","<":"&lt;",">":"&gt;","\\"":"&quot;"}}[c]))}}
async function fetchJson(url){{const r=await fetch(url); return await r.json();}}

(async()=>{
    const d = await fetchJson(
  "/api/customer/detail?shop_id=" + encodeURIComponent(SHOP)
  + "&conv_key=" + encodeURIComponent(CONV)
  + "&key=" + encodeURIComponent(KEY)
);

  const c = d.customer || {{}};
  document.getElementById("cust").innerHTML = `
    <b>User</b>: <span class="mono">${{esc(c.user_id||"-")}}</span><br/>
    <b>Status</b>: <span class="mono">${{esc(c.status||"-")}}</span> / <b>Lv</b>: <span class="mono">${{esc(c.temp_level_stable||"-")}}</span> / <b>conf</b>: <span class="mono">${{c.confidence==null?"-":Number(c.confidence).toFixed(2)}}</span><br/>
    <b>Intent</b>: <span class="mono">${{esc(c.intent||"-")}}</span> / <b>Goal</b>: ${{esc(c.next_goal||"-")}}<br/>
    <b>need_reply</b>: <span class="mono">${{esc(c.need_reply? "TRUE":"FALSE")}}</span> (${{esc(c.need_reply_reason||"")}})<br/>
    <b>pref_hour</b>: <span class="mono">${{esc(c.pref_hour_jst??"-")}}</span> samples:<span class="mono">${{esc(c.pref_hour_samples??"-")}}</span><br/>
    <b>slots</b>: budget=${{esc(c.slot_budget||"-")}} / area=${{esc(c.slot_area||"-")}} / move_in=${{esc(c.slot_move_in||"-")}} / layout=${{esc(c.slot_layout||"-")}}<br/>
    <b>visit</b>: ${{esc(c.visit_slot_selected||"-")}}<br/>
    <b>updated</b>: <span class="mono">${{esc(c.updated_at||"-")}}</span>
  `;

  const msgs = d.messages||[];
  document.getElementById("msgs").innerHTML = msgs.map(m=>{
    const cls = (m.role==="user")?"u":"a";
    return `<div class="msg">
      <div class="mono ${{cls}}">${{esc(m.role)}} / ${{esc(m.ts||"")}} / Lv:${{esc(m.temp_level_stable||"-")}} / intent:${{esc(m.intent||"-")}} / goal:${{esc(m.next_goal||"-")}}</div>
      <pre>${{esc(m.content||"")}}</pre>
    </div>`;
  }).join("") || "no messages";

  const fls = d.followups||[];
  document.getElementById("fls").innerHTML = fls.map(f=>{
    return `<div class="msg">
      <div class="mono">ts:${{esc(f.ts||"")}} / stage:${{esc(f.stage||"-")}} / var:${{esc(f.variant||"-")}} / mode:${{esc(f.mode||"-")}} / status:${{esc(f.status||"-")}} / send_hour:${{esc(f.send_hour_jst??"-")}}</div>
      <pre>${{esc(f.message||"")}}</pre>
      <div class="mono">responded:${{esc(f.responded_at||"-")}} / err:${{esc(f.error||"-")}}</div>
    </div>`;
  }).join("") || "no followups";
})();
</script>
</body>
</html>
"""
    return HTMLResponse(html)


# ============================================================
# Admin actions (minimal)
# ============================================================

def fetch_customer_for_action(shop_id: str, conv_key: str) -> Optional[Dict[str, Any]]:
    rows = db_fetchall(
        """
        SELECT conv_key, user_id,
               COALESCE(temp_level_stable,0),
               COALESCE(next_goal,''),
               COALESCE(last_user_text,''),
               COALESCE(status,'ACTIVE'),
               COALESCE(opt_out,FALSE),
               COALESCE(intent,'other'),
               pref_hour_jst,
               updated_at
        FROM customers
        WHERE shop_id=%s AND conv_key=%s
        LIMIT 1
        """,
        (shop_id, conv_key),
    )
    if not rows:
        return None
    conv_key, user_id, lvl, goal, last_text, st, opt, intent, pref_hour_jst, updated_at = rows[0]
    return {
        "conv_key": conv_key,
        "user_id": user_id,
        "level": int(lvl or 0),
        "next_goal": goal or "",
        "last_user_text": last_text or "",
        "status": (st or "ACTIVE"),
        "opt_out": bool(opt),
        "intent": intent or "other",
        "pref_hour_jst": pref_hour_jst,
        "updated_at": updated_at,
    }


@app.post("/api/customer/mark_cold")
async def api_customer_mark_cold(
    _: None = Depends(require_admin_key),
    shop_id: str = Query(default=SHOP_ID),
    conv_key: str = Query(...),
):
    if not DATABASE_URL:
        return {"ok": True}
    mark_cold(shop_id, conv_key)
    return {"ok": True}


@app.post("/api/customer/mark_optout")
async def api_customer_mark_optout(
    _: None = Depends(require_admin_key),
    shop_id: str = Query(default=SHOP_ID),
    conv_key: str = Query(...),
):
    if not DATABASE_URL:
        return {"ok": True}
    c = fetch_customer_for_action(shop_id, conv_key)
    uid = (c or {}).get("user_id") or "unknown"
    mark_opt_out(shop_id, conv_key, uid)
    return {"ok": True}


@app.post("/api/customer/reset_need_reply")
async def api_customer_reset_need_reply(
    _: None = Depends(require_admin_key),
    shop_id: str = Query(default=SHOP_ID),
    conv_key: str = Query(...),
):
    if not DATABASE_URL:
        return {"ok": True}
    set_need_reply(shop_id, conv_key, False, "manual_reset")
    return {"ok": True}


@app.post("/api/customer/send_followup_now")
async def api_customer_send_followup_now(
    _: None = Depends(require_admin_key),
    shop_id: str = Query(default=SHOP_ID),
    conv_key: str = Query(...),
    stage: int = Query(default=1, ge=1, le=2),
):
    if not DATABASE_URL:
        return {"ok": True, "sent": False, "reason": "no_db"}

    c = fetch_customer_for_action(shop_id, conv_key)
    if not c:
        return {"ok": True, "sent": False, "reason": "not_found"}

    if c["opt_out"] or (c["status"] or "").upper() in ("COLD", "LOST", "OPTOUT"):
        return {"ok": True, "sent": False, "reason": "inactive"}

    user_id = c["user_id"]
    if not user_id or user_id == "unknown":
        return {"ok": True, "sent": False, "reason": "no_user_id"}

    variant = pick_ab_variant(conv_key)

    msg = None
    if stage == 1:
        msg = await generate_followup_message_llm(shop_id, conv_key, stage=1, variant=variant, customer=c)
        if not msg:
            msg = build_followup_template_ab_fallback(variant, c["next_goal"], c["last_user_text"], c["level"])
        mode = "llm" if msg and FOLLOWUP_USE_LLM else "template"
    else:
        msg = await generate_followup_message_llm(shop_id, conv_key, stage=2, variant=variant, customer=c)
        if not msg:
            msg = build_second_touch_message_fallback(c["next_goal"])
        mode = "llm" if msg and FOLLOWUP_USE_LLM else "template"

    try:
        await push_line(user_id, msg)
        save_followup_log(shop_id, conv_key, user_id, msg, mode, "sent", None, variant, stage, send_hour_jst=now_jst().hour)
        return {"ok": True, "sent": True, "stage": stage, "variant": variant}
    except Exception as e:
        save_followup_log(shop_id, conv_key, user_id, msg, mode, "failed", str(e)[:200], variant, stage, send_hour_jst=now_jst().hour)
        return {"ok": True, "sent": False, "error": str(e)[:200]}


# ============================================================
# Jobs
# ============================================================

@app.post("/jobs/maintenance")
async def job_maintenance(_: None = Depends(require_admin_key)):
    maintenance_update_statuses(SHOP_ID)
    return {"ok": True}


@app.post("/jobs/followup")
async def job_followup(_: None = Depends(require_admin_key)):
    if not FOLLOWUP_ENABLED:
        return {"ok": True, "enabled": False, "reason": "FOLLOWUP_ENABLED!=1"}

    maintenance_update_statuses(SHOP_ID)

    if not is_within_jst_window():
        return {"ok": True, "enabled": True, "skipped": True, "reason": "out_of_time_window"}

    if not acquire_job_lock("followup", FOLLOWUP_LOCK_TTL_SEC):
        return {"ok": True, "enabled": True, "skipped": True, "reason": "locked"}

    stage1 = get_followup_candidates_stage1()
    stage2 = get_followup_candidates_stage2()

    if FOLLOWUP_DRYRUN:
        return {"ok": True, "enabled": True, "dryrun": True, "stage1": stage1[:50], "stage2": stage2[:50]}

    sent1 = 0
    sent2 = 0
    skipped_time = 0
    failed = 0

    nowj = now_jst()
    now_hour = nowj.hour

    def should_send_now(updated_at: Optional[datetime], pref_hour: Optional[int]) -> bool:
        if updated_at:
            age_h = (utcnow() - updated_at).total_seconds() / 3600.0
            if age_h >= max(1, FOLLOWUP_FORCE_SEND_AFTER_HOURS):
                return True
        target = choose_send_hour_jst(pref_hour)
        return within_hour_band(now_hour, target, FOLLOWUP_TIME_MATCH_HOURS)

    for c in stage1:
        conv_key = c["conv_key"]
        user_id = c["user_id"]

        if is_inactive(SHOP_ID, conv_key):
            save_followup_log(SHOP_ID, conv_key, user_id, "(skipped inactive)", "llm/template", "skipped", "inactive", None, 1, send_hour_jst=now_hour)
            continue

        if not should_send_now(c.get("updated_at"), c.get("pref_hour_jst")):
            skipped_time += 1
            continue

        variant = pick_ab_variant(conv_key)
        msg = await generate_followup_message_llm(SHOP_ID, conv_key, stage=1, variant=variant, customer=c)
        mode = "llm"
        if not msg:
            msg = build_followup_template_ab_fallback(variant, c["next_goal"], c["last_user_text"], c["level"])
            mode = "template"

        try:
            await push_line(user_id, msg)
            save_followup_log(SHOP_ID, conv_key, user_id, msg, mode, "sent", None, variant, 1, send_hour_jst=now_hour)
            sent1 += 1
        except Exception as e:
            save_followup_log(SHOP_ID, conv_key, user_id, msg, mode, "failed", str(e)[:200], variant, 1, send_hour_jst=now_hour)
            failed += 1

    for c in stage2:
        conv_key = c["conv_key"]
        user_id = c["user_id"]

        if is_inactive(SHOP_ID, conv_key):
            save_followup_log(SHOP_ID, conv_key, user_id, "(skipped inactive)", "llm/template", "skipped", "inactive", None, 2, send_hour_jst=now_hour)
            continue

        if not should_send_now(c.get("updated_at"), c.get("pref_hour_jst")):
            skipped_time += 1
            continue

        variant = pick_ab_variant(conv_key)
        msg = await generate_followup_message_llm(SHOP_ID, conv_key, stage=2, variant=variant, customer=c)
        mode = "llm"
        if not msg:
            msg = build_second_touch_message_fallback(c["next_goal"])
            mode = "template"

        try:
            await push_line(user_id, msg)
            save_followup_log(SHOP_ID, conv_key, user_id, msg, mode, "sent", None, variant, 2, send_hour_jst=now_hour)
            sent2 += 1
        except Exception as e:
            save_followup_log(SHOP_ID, conv_key, user_id, msg, mode, "failed", str(e)[:200], variant, 2, send_hour_jst=now_hour)
            failed += 1

    return {
        "ok": True,
        "enabled": True,
        "sent_stage1": sent1,
        "sent_stage2": sent2,
        "skipped_time": skipped_time,
        "failed": failed,
        "now_hour_jst": now_hour,
        "time_band": FOLLOWUP_TIME_MATCH_HOURS,
        "force_send_after_h": FOLLOWUP_FORCE_SEND_AFTER_HOURS,
        "llm_enabled": FOLLOWUP_USE_LLM,
    }


@app.post("/jobs/push_test")
async def job_push_test(
    _: None = Depends(require_admin_key),
    user_id: str = Query(...),
    text: str = Query(...),
):
    await push_line(user_id, text)
    return {"ok": True}
