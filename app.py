import os
import json
import hmac
import hashlib
import base64
import asyncio
import ssl
import html as html_lib
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from collections import deque, defaultdict
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, unquote

import httpx
import certifi
import pg8000
from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import HTMLResponse

app = FastAPI()

# =========================
# ENV
# =========================
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "")
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

DATABASE_URL = os.getenv("DATABASE_URL", "")
SHOP_ID = os.getenv("SHOP_ID", "tokyo_01")

# 設定すると /api/* と /dashboard と /jobs/* を保護（未設定なら開発用に公開のまま）
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "")

# DB SSL: auto(推奨) / verify / disable
DB_SSL_MODE = os.getenv("DB_SSL_MODE", "auto").lower().strip()

# デバッグログ増やしたい時だけ 1
DEBUG = os.getenv("DEBUG", "0") == "1"

# 自動追客
FOLLOWUP_ENABLED = os.getenv("FOLLOWUP_ENABLED", "0") == "1"
FOLLOWUP_JST_FROM = int(os.getenv("FOLLOWUP_JST_FROM", "10"))          # 10時
FOLLOWUP_JST_TO = int(os.getenv("FOLLOWUP_JST_TO", "20"))              # 20時
FOLLOWUP_LIMIT_PER_RUN = int(os.getenv("FOLLOWUP_LIMIT_PER_RUN", "30"))
FOLLOWUP_MIN_HOURS_BETWEEN = int(os.getenv("FOLLOWUP_MIN_HOURS_BETWEEN", "24"))

# =========================
# In-memory (MVP)
# =========================
CHAT_HISTORY = defaultdict(lambda: deque(maxlen=40))  # 最大20往復
TEMP_HISTORY = defaultdict(lambda: deque(maxlen=3))   # 揺れ止め（中央値）


# =========================
# Helpers
# =========================
def log(msg: str) -> None:
    if DEBUG:
        print(msg)


def verify_signature(body: bytes, signature: str) -> bool:
    if not LINE_CHANNEL_SECRET:
        return False
    mac = hmac.new(LINE_CHANNEL_SECRET.encode("utf-8"), body, hashlib.sha256).digest()
    expected = base64.b64encode(mac).decode("utf-8")
    return hmac.compare_digest(expected, signature)


def median(values: List[int], default: int = 5) -> int:
    if not values:
        return default
    s = sorted(values)
    return s[len(s) // 2]


def extract_json(text: str) -> Dict[str, Any]:
    text = (text or "").strip()
    try:
        return json.loads(text)
    except Exception:
        pass
    start, end = text.find("{"), text.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(text[start : end + 1])
        except Exception:
            pass
    return {}


def coerce_level(raw: Any, default: int = 5) -> int:
    try:
        if isinstance(raw, str):
            s = raw.strip()
            digits = "".join(ch for ch in s if ch.isdigit())
            if digits:
                raw = int(digits)
        lvl = int(raw)
        return max(1, min(10, lvl))
    except Exception:
        return default


def coerce_confidence(raw: Any, default: float = 0.7) -> float:
    """'低い/高い/80%' などでも落ちない"""
    if isinstance(raw, (int, float)):
        return max(0.0, min(1.0, float(raw)))

    if isinstance(raw, str):
        s = raw.strip().lower()
        if s in {"高い", "高め", "high"}:
            return 0.85
        if s in {"中", "普通", "ふつう", "medium"}:
            return 0.60
        if s in {"低い", "低め", "low"}:
            return 0.40

        if s.endswith("%"):
            try:
                v = float(s[:-1]) / 100.0
                return max(0.0, min(1.0, v))
            except Exception:
                return default

        try:
            v = float(s)
            if 1.0 < v <= 100.0:
                v = v / 100.0
            return max(0.0, min(1.0, v))
        except Exception:
            return default

    return default


def check_admin_key(x_admin_key: Optional[str], query_key: Optional[str] = None) -> None:
    """ADMIN_API_KEYがある時だけ認証（ブラウザ用に ?key= も許可）"""
    if not ADMIN_API_KEY:
        return
    if x_admin_key == ADMIN_API_KEY:
        return
    if query_key == ADMIN_API_KEY:
        return
    raise HTTPException(status_code=401, detail="Unauthorized")


def can_db() -> bool:
    return bool(DATABASE_URL) and bool(SHOP_ID)


def parse_database_url(url: str) -> Dict[str, Any]:
    u = urlparse(url)
    return {
        "user": unquote(u.username or ""),
        "password": unquote(u.password or ""),
        "host": u.hostname or "",
        "port": u.port or 5432,
        "database": (u.path or "").lstrip("/"),
    }


def make_ssl_context(mode: str) -> ssl.SSLContext:
    """verify = 検証ON / disable = 検証OFF"""
    if mode == "disable":
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx

    ctx = ssl.create_default_context(cafile=certifi.where())
    ctx.check_hostname = True
    ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx


def db_connect():
    """auto/verify/disable 対応。autoはverify失敗時にdisableへフォールバック"""
    cfg = parse_database_url(DATABASE_URL)
    if not cfg["host"]:
        raise RuntimeError("DATABASE_URL host is empty")

    mode = DB_SSL_MODE if DB_SSL_MODE in {"auto", "verify", "disable"} else "auto"

    if mode in {"auto", "verify"}:
        try:
            return pg8000.connect(
                user=cfg["user"],
                password=cfg["password"],
                host=cfg["host"],
                port=cfg["port"],
                database=cfg["database"],
                ssl_context=make_ssl_context("verify"),
            )
        except ssl.SSLCertVerificationError as e:
            if mode == "verify":
                raise
            print(f"[DB] SSL verify failed, fallback to disable: {e}")

    return pg8000.connect(
        user=cfg["user"],
        password=cfg["password"],
        host=cfg["host"],
        port=cfg["port"],
        database=cfg["database"],
        ssl_context=make_ssl_context("disable"),
    )


def to_utc(dt):
    """DBから返るdatetimeをUTCのawareに統一"""
    if dt is None:
        return None
    if getattr(dt, "tzinfo", None) is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def in_jst_sending_window() -> bool:
    """追客送信可能時間帯（JST）か"""
    jst = datetime.now(ZoneInfo("Asia/Tokyo"))
    hour = jst.hour
    return FOLLOWUP_JST_FROM <= hour < FOLLOWUP_JST_TO


def conv_key_to_to_id(conv_key: str) -> str:
    """
    LINE pushMessage の to に入れるIDを返す
    conv_key は webhook で作ってる "user:xxx" "group:xxx" "room:xxx"
    """
    if ":" not in conv_key:
        return conv_key
    return conv_key.split(":", 1)[1]


# =========================
# DB (sync) - use via asyncio.to_thread
# =========================
def ensure_tables_sync():
    if not can_db():
        print("[DB] skipped ensure_tables (missing DATABASE_URL or SHOP_ID)")
        return

    conn = db_connect()
    try:
        cur = conn.cursor()

        # 最新状態
        cur.execute("""
        CREATE TABLE IF NOT EXISTS customers (
          id BIGSERIAL PRIMARY KEY,
          shop_id TEXT NOT NULL,
          conv_key TEXT NOT NULL,
          last_user_text TEXT,
          temp_level_raw INT,
          temp_level_stable INT,
          confidence REAL,
          next_goal TEXT,
          updated_at TIMESTAMPTZ DEFAULT now(),
          UNIQUE (shop_id, conv_key)
        );
        """)

        # 会話ログ
        cur.execute("""
        CREATE TABLE IF NOT EXISTS messages (
          id BIGSERIAL PRIMARY KEY,
          shop_id TEXT NOT NULL,
          conv_key TEXT NOT NULL,
          role TEXT NOT NULL,
          content TEXT NOT NULL,
          created_at TIMESTAMPTZ DEFAULT now()
        );
        """)

        # 履歴（更新しても残る）
        cur.execute("""
        CREATE TABLE IF NOT EXISTS customer_events (
          id BIGSERIAL PRIMARY KEY,
          shop_id TEXT NOT NULL,
          conv_key TEXT NOT NULL,
          user_text TEXT,
          temp_level_raw INT,
          temp_level_stable INT,
          confidence REAL,
          next_goal TEXT,
          created_at TIMESTAMPTZ DEFAULT now()
        );
        """)

        # 追客の状態（スパム防止）
        cur.execute("""
        CREATE TABLE IF NOT EXISTS followup_state (
          id BIGSERIAL PRIMARY KEY,
          shop_id TEXT NOT NULL,
          conv_key TEXT NOT NULL,
          followup_count INT DEFAULT 0,
          last_followup_at TIMESTAMPTZ,
          last_followup_reason TEXT,
          UNIQUE (shop_id, conv_key)
        );
        """)

        # インデックス
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_customers_shop_level_updated
        ON customers (shop_id, temp_level_stable, updated_at DESC);
        """)
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_customer_events_shop_created
        ON customer_events (shop_id, created_at DESC);
        """)
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_followup_state_shop_last
        ON followup_state (shop_id, last_followup_at DESC);
        """)

        conn.commit()
        print("[DB] tables ensured")
    finally:
        conn.close()


def db_write_interaction_sync(
    shop_id: str,
    conv_key: str,
    user_text: str,
    assistant_text: str,
    raw_level: int,
    stable_level: int,
    conf: float,
    next_goal: str,
):
    """1接続でまとめて書く（速い・安定）"""
    conn = db_connect()
    try:
        cur = conn.cursor()

        # messages（毎回残る）
        cur.execute(
            "INSERT INTO messages (shop_id, conv_key, role, content) VALUES (%s,%s,%s,%s)",
            (shop_id, conv_key, "user", user_text),
        )
        cur.execute(
            "INSERT INTO messages (shop_id, conv_key, role, content) VALUES (%s,%s,%s,%s)",
            (shop_id, conv_key, "assistant", assistant_text),
        )

        # customer_events（履歴として毎回INSERT）
        cur.execute(
            """
            INSERT INTO customer_events
              (shop_id, conv_key, user_text, temp_level_raw, temp_level_stable, confidence, next_goal)
            VALUES
              (%s,%s,%s,%s,%s,%s,%s)
            """,
            (shop_id, conv_key, user_text, raw_level, stable_level, conf, next_goal),
        )

        # customers（最新状態としてUPSERT）
        cur.execute(
            """
            INSERT INTO customers
              (shop_id, conv_key, last_user_text, temp_level_raw, temp_level_stable, confidence, next_goal, updated_at)
            VALUES
              (%s,%s,%s,%s,%s,%s,%s, now())
            ON CONFLICT (shop_id, conv_key)
            DO UPDATE SET
              last_user_text=EXCLUDED.last_user_text,
              temp_level_raw=EXCLUDED.temp_level_raw,
              temp_level_stable=EXCLUDED.temp_level_stable,
              confidence=EXCLUDED.confidence,
              next_goal=EXCLUDED.next_goal,
              updated_at=now()
            """,
            (shop_id, conv_key, user_text, raw_level, stable_level, conf, next_goal),
        )

        conn.commit()
    finally:
        conn.close()


def db_insert_assistant_message_sync(shop_id: str, conv_key: str, content: str):
    """追客送信など（assistantだけ）をmessagesに残す"""
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO messages (shop_id, conv_key, role, content) VALUES (%s,%s,%s,%s)",
            (shop_id, conv_key, "assistant", content),
        )
        conn.commit()
    finally:
        conn.close()


def db_get_last_message_per_conv_sync(shop_id: str):
    """
    各conv_keyの最新メッセージ（role/created_at/content）＋ customers の温度
    """
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT DISTINCT ON (m.conv_key)
              m.conv_key,
              m.role,
              m.content,
              m.created_at,
              c.temp_level_stable,
              c.confidence,
              c.next_goal
            FROM messages m
            JOIN customers c
              ON c.shop_id = m.shop_id AND c.conv_key = m.conv_key
            WHERE m.shop_id = %s AND c.shop_id = %s
            ORDER BY m.conv_key, m.created_at DESC
            """,
            (shop_id, shop_id),
        )
        return cur.fetchall()
    finally:
        conn.close()


def db_get_last_user_at_map_sync(shop_id: str) -> Dict[str, datetime]:
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT DISTINCT ON (conv_key)
              conv_key, created_at
            FROM messages
            WHERE shop_id=%s AND role='user'
            ORDER BY conv_key, created_at DESC
            """,
            (shop_id,),
        )
        rows = cur.fetchall()
        m = {}
        for conv_key, created_at in rows:
            m[conv_key] = to_utc(created_at)
        return m
    finally:
        conn.close()


def db_get_followup_state_map_sync(shop_id: str) -> Dict[str, Dict[str, Any]]:
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT conv_key, followup_count, last_followup_at, last_followup_reason FROM followup_state WHERE shop_id=%s",
            (shop_id,),
        )
        rows = cur.fetchall()
        m = {}
        for conv_key, cnt, last_at, reason in rows:
            m[conv_key] = {
                "followup_count": int(cnt or 0),
                "last_followup_at": to_utc(last_at),
                "last_followup_reason": reason,
            }
        return m
    finally:
        conn.close()


def db_upsert_followup_state_sync(shop_id: str, conv_key: str, new_count: int, reason: str):
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO followup_state (shop_id, conv_key, followup_count, last_followup_at, last_followup_reason)
            VALUES (%s,%s,%s, now(), %s)
            ON CONFLICT (shop_id, conv_key)
            DO UPDATE SET
              followup_count=EXCLUDED.followup_count,
              last_followup_at=now(),
              last_followup_reason=EXCLUDED.last_followup_reason
            """,
            (shop_id, conv_key, new_count, reason),
        )
        conn.commit()
    finally:
        conn.close()


def db_fetch_recent_messages_sync(shop_id: str, conv_key: str, limit: int = 10) -> List[Dict[str, str]]:
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT role, content
            FROM messages
            WHERE shop_id=%s AND conv_key=%s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (shop_id, conv_key, limit),
        )
        rows = cur.fetchall()
        # 古い→新しい順にしたいので逆順
        rows = list(reversed(rows))
        return [{"role": r, "content": c} for r, c in rows]
    finally:
        conn.close()


# =========================
# LINE Reply / Push
# =========================
async def reply_message(reply_token: str, text: str) -> None:
    url = "https://api.line.me/v2/bot/message/reply"
    headers = {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {"replyToken": reply_token, "messages": [{"type": "text", "text": text}]}
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(url, headers=headers, json=payload)
        r.raise_for_status()


async def push_message(to_id: str, text: str) -> None:
    url = "https://api.line.me/v2/bot/message/push"
    headers = {
        "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {"to": to_id, "messages": [{"type": "text", "text": text}]}
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(url, headers=headers, json=payload)
        r.raise_for_status()


# =========================
# OpenAI (temp10 + reply)
# =========================
async def analyze_and_generate_reply(
    last_messages: List[Dict[str, str]], user_text: str
) -> Tuple[Dict[str, Any], str]:
    fallback = "お問い合わせありがとうございます。内容を確認して改めてご連絡いたします。"

    if not OPENAI_API_KEY:
        return (
            {"temperature_level": 5, "confidence": 0.4, "signals": ["no_api_key"], "next_goal": "条件整理"},
            fallback,
        )

    system_prompt = (
        "あなたは不動産の追客AIです。直近会話と最新発言から温度感を10段階(1〜10)で判定し、"
        "温度感に合う返信文を作ってください。\n\n"
        "【温度感10段階】\n"
        "1:ほぼ脈なし 2:低反応 3:情報収集 4:条件相談 5:検討中 6:前向き 7:候補比較 8:内見検討 9:内見日程調整 10:申込/今すぐ\n\n"
        "【返信】丁寧な日本語で2〜5文。断定しない（必要なら『確認します』）。"
        "温度が高いほど次アクションを具体化（内見日程2択など）。\n\n"
        "【出力】必ずJSONのみ。\n"
        "temperature_level は 1〜10 の整数、confidence は 0〜1 の数値（文字禁止）。\n"
        "keys: temperature_level, confidence, signals, next_goal, reply_text"
    )

    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"【直近会話】\n{json.dumps(last_messages[-20:], ensure_ascii=False)}\n\n【最新】\n{user_text}\n"},
        ],
        "temperature": 0.4,
    }
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload)
            r.raise_for_status()
            data = r.json()

        result = extract_json(data["choices"][0]["message"]["content"])
        if not result:
            return (
                {"temperature_level": 5, "confidence": 0.4, "signals": ["json_parse_failed"], "next_goal": "条件整理"},
                fallback,
            )

        lvl = coerce_level(result.get("temperature_level", 5), default=5)
        conf = coerce_confidence(result.get("confidence", 0.7), default=0.7)

        if conf < 0.6:
            lvl = 5

        reply_text = (result.get("reply_text") or "").strip() or fallback
        temp_info = {
            "temperature_level": lvl,
            "confidence": conf,
            "signals": result.get("signals", []),
            "next_goal": str(result.get("next_goal", "条件整理")),
        }
        return temp_info, reply_text

    except Exception as e:
        print(f"[OPENAI] error: {e}")
        return (
            {"temperature_level": 5, "confidence": 0.4, "signals": ["openai_error"], "next_goal": "条件整理"},
            fallback,
        )


async def generate_followup_text(
    last_messages: List[Dict[str, str]],
    temp_level: int,
    hours_waited: int,
) -> str:
    """
    追客用の1通を生成（失敗時はテンプレ）
    """
    # テンプレ（安全）
    if temp_level >= 8:
        fallback = "ご検討状況いかがでしょうか？もしよければ内見候補日を2つほど教えてください。こちらで調整いたします。"
        tone = "前向き。日程確定へ。2択で提案。"
    elif temp_level >= 4:
        fallback = "ご検討状況いかがでしょうか？希望条件（エリア・家賃・入居時期）で変わった点があれば教えてください。"
        tone = "丁寧。条件整理。質問は1つまで。"
    else:
        fallback = "ご状況いかがでしょうか。もし条件が固まりましたら、いつでもご連絡ください。"
        tone = "軽め。追いすぎない。"

    if not OPENAI_API_KEY:
        return fallback

    system_prompt = (
        "あなたは不動産の追客担当です。未返信の方へのフォローメッセージを1通作成してください。\n"
        f"温度感レベルは {temp_level}/10、最後の返信から約{hours_waited}時間経過。\n"
        f"方針: {tone}\n"
        "・日本語で2〜4文\n"
        "・断定しない（空室などは確認します）\n"
        "・最後に返信しやすい質問を1つ\n"
        "・絵文字は不要\n"
    )

    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"直近の会話:\n{json.dumps(last_messages[-10:], ensure_ascii=False)}"},
        ],
        "temperature": 0.5,
    }
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload)
            r.raise_for_status()
            data = r.json()

        text = (data["choices"][0]["message"]["content"] or "").strip()
        return text if text else fallback
    except Exception as e:
        print(f"[OPENAI followup] error: {e}")
        return fallback


# =========================
# Startup
# =========================
@app.on_event("startup")
async def on_startup():
    try:
        await asyncio.to_thread(ensure_tables_sync)
    except Exception as e:
        print(f"[DB] ensure tables failed: {e}")


# =========================
# API / Job
# =========================
@app.get("/")
async def root():
    return {"ok": True}


@app.post("/jobs/followup")
async def job_followup(
    shop_id: str = SHOP_ID,
    dry_run: int = 0,
    limit: int = FOLLOWUP_LIMIT_PER_RUN,
    key: Optional[str] = None,
    x_admin_key: Optional[str] = Header(None, alias="X-Admin-Key"),
):
    """
    自動追客を実行するジョブ。
    Render Cron Job から叩く想定。
    """
    check_admin_key(x_admin_key, key)

    if not FOLLOWUP_ENABLED:
        return {"ok": False, "reason": "FOLLOWUP_ENABLED is off"}

    if not in_jst_sending_window():
        return {"ok": True, "skipped": True, "reason": "outside JST sending window"}

    if not can_db():
        return {"ok": False, "reason": "DB not configured"}

    dry_run = 1 if int(dry_run) == 1 else 0
    limit = max(1, min(200, int(limit)))

    # 追客候補：各conv_keyの最新メッセージが assistant で、一定時間経過
    rows = await asyncio.to_thread(db_get_last_message_per_conv_sync, shop_id)
    user_last_map = await asyncio.to_thread(db_get_last_user_at_map_sync, shop_id)
    fu_map = await asyncio.to_thread(db_get_followup_state_map_sync, shop_id)

    now = now_utc()

    # 追客ルール（安全め）
    def threshold_hours(level: int) -> int:
        if level >= 8:
            return 24
        if level >= 4:
            return 48
        return 72

    def max_count(level: int) -> int:
        if level >= 8:
            return 3
        if level >= 4:
            return 2
        return 1

    sent = []
    skipped = []

    for conv_key, last_role, last_content, last_at, lvl, conf, goal in rows:
        if len(sent) >= limit:
            break

        last_at_utc = to_utc(last_at)
        if last_at_utc is None:
            skipped.append({"conv_key": conv_key, "reason": "no_last_at"})
            continue

        # 「最後がassistant」＝ユーザー未返信状態
        if last_role != "assistant":
            continue

        lvl_int = int(lvl or 5)
        wait_h = int(threshold_hours(lvl_int))

        age_hours = int((now - last_at_utc).total_seconds() / 3600)
        if age_hours < wait_h:
            continue

        # 追客スパム防止
        state = fu_map.get(conv_key, {"followup_count": 0, "last_followup_at": None})
        followup_count = int(state.get("followup_count", 0))
        last_followup_at = state.get("last_followup_at")

        # ユーザーが追客後に返信していたら、回数をリセット扱い
        user_last_at = user_last_map.get(conv_key)
        if user_last_at and last_followup_at and user_last_at > last_followup_at:
            followup_count = 0
            last_followup_at = None

        if followup_count >= max_count(lvl_int):
            skipped.append({"conv_key": conv_key, "reason": "max_followup_reached"})
            continue

        if last_followup_at:
            since_fu = int((now - last_followup_at).total_seconds() / 3600)
            if since_fu < FOLLOWUP_MIN_HOURS_BETWEEN:
                skipped.append({"conv_key": conv_key, "reason": "too_soon_since_last_followup"})
                continue

        # 直近会話を取って、追客文を作る
        recent_msgs = await asyncio.to_thread(db_fetch_recent_messages_sync, shop_id, conv_key, 10)
        follow_text = await generate_followup_text(recent_msgs, lvl_int, age_hours)

        to_id = conv_key_to_to_id(conv_key)
        reason = f"no_reply_{age_hours}h_lvl{lvl_int}"

        if dry_run:
            sent.append({"conv_key": conv_key, "to": to_id, "text": follow_text, "dry_run": True, "reason": reason})
            continue

        try:
            await push_message(to_id, follow_text)
            # DBに追客送信も記録（assistantメッセージとして）
            await asyncio.to_thread(db_insert_assistant_message_sync, shop_id, conv_key, f"[FOLLOWUP] {follow_text}")
            await asyncio.to_thread(db_upsert_followup_state_sync, shop_id, conv_key, followup_count + 1, reason)

            sent.append({"conv_key": conv_key, "to": to_id, "reason": reason})
        except Exception as e:
            skipped.append({"conv_key": conv_key, "reason": f"push_failed: {e}"})

    return {"ok": True, "dry_run": bool(dry_run), "sent": sent, "sent_count": len(sent), "skipped": skipped, "skipped_count": len(skipped)}


# =========================
# HOT API / Dashboard（既存）
# =========================
@app.get("/api/hot")
async def api_hot(
    shop_id: str = SHOP_ID,
    min_level: int = 8,
    limit: int = 50,
    view: str = "latest",        # latest / events
    q: Optional[str] = None,     # 検索（JSONでも使える）
    key: Optional[str] = None,   # ブラウザ用
    x_admin_key: Optional[str] = Header(None, alias="X-Admin-Key"),
):
    check_admin_key(x_admin_key, key)

    min_level = max(1, min(10, int(min_level)))
    limit = max(1, min(200, int(limit)))
    view = (view or "latest").lower()
    q_norm = (q or "").strip().lower()

    if not can_db():
        return {"shop_id": shop_id, "min_level": min_level, "count": 0, "items": [], "error": "DB not configured"}

    if view == "events":
        items = await asyncio.to_thread(db_fetch_hot_events_sync, shop_id, min_level, limit)
    else:
        items = await asyncio.to_thread(db_fetch_hot_customers_sync, shop_id, min_level, limit)

    if q_norm:
        def hit(it: Dict[str, Any]) -> bool:
            blob = " ".join([
                str(it.get("updated_at", "")),
                str(it.get("conv_key", "")),
                str(it.get("user_id", "")),
                str(it.get("temp_level_stable", "")),
                str(it.get("confidence", "")),
                str(it.get("next_goal", "")),
                str(it.get("last_user_text", "")),
            ]).lower()
            return q_norm in blob
        items = [it for it in items if hit(it)]

    return {"shop_id": shop_id, "min_level": min_level, "view": view, "q": q or "", "count": len(items), "items": items}


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(
    shop_id: str = SHOP_ID,
    min_level: int = 8,
    limit: int = 50,
    view: str = "events",        # 履歴がデフォルト
    refresh: int = 30,           # 自動更新秒。0でOFF
    q: Optional[str] = None,     # 検索ワード
    key: Optional[str] = None,
    x_admin_key: Optional[str] = Header(None, alias="X-Admin-Key"),
):
    check_admin_key(x_admin_key, key)

    min_level = max(1, min(10, int(min_level)))
    limit = max(1, min(200, int(limit)))
    view = (view or "events").lower()
    refresh = int(refresh)
    if refresh < 0:
        refresh = 0
    if refresh > 600:
        refresh = 600
    q_value = (q or "").strip()

    if not can_db():
        return HTMLResponse("<h2>DB未設定</h2><p>DATABASE_URL / SHOP_ID を設定してください。</p>", status_code=500)

    if view == "latest":
        items = await asyncio.to_thread(db_fetch_hot_customers_sync, shop_id, min_level, limit)
    else:
        items = await asyncio.to_thread(db_fetch_hot_events_sync, shop_id, min_level, limit)

    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    legend_html = " ".join([f'<span class="badge badge-lvl-{i}">{i}</span>' for i in range(1, 11)])

    rows_html = ""
    for it in items:
        lvl = it.get("temp_level_stable", "")
        try:
            lvl_int = int(lvl)
        except Exception:
            lvl_int = 0

        row_class = f"lvl-{lvl_int}" if 1 <= lvl_int <= 10 else ""
        badge_class = f"badge badge-lvl-{lvl_int}" if 1 <= lvl_int <= 10 else "badge"

        search_blob = " ".join([
            str(it.get("updated_at", "")),
            str(it.get("conv_key", "")),
            str(it.get("user_id", "")),
            str(it.get("temp_level_stable", "")),
            str(it.get("confidence", "")),
            str(it.get("next_goal", "")),
            str(it.get("last_user_text", "")),
        ]).lower()
        search_blob_esc = html_lib.escape(search_blob, quote=True)

        rows_html += f"""
        <tr class="{row_class}" data-search="{search_blob_esc}">
          <td>{html_lib.escape(str(it.get("updated_at","")))}</td>
          <td style="text-align:center"><span class="{badge_class}">{html_lib.escape(str(it.get("temp_level_stable","")))}</span></td>
          <td style="text-align:center">{html_lib.escape(str(it.get("confidence","")))}</td>
          <td>{html_lib.escape(str(it.get("next_goal","")))}</td>
          <td><code>{html_lib.escape(str(it.get("user_id") or ""))}</code></td>
          <td>{html_lib.escape(str(it.get("last_user_text","")))}</td>
        </tr>
        """

    meta_refresh = ""
    if refresh > 0:
        meta_refresh = f'<meta http-equiv="refresh" content="{refresh}">'

    api_link = f"/api/hot?shop_id={html_lib.escape(shop_id)}&min_level={min_level}&limit={limit}&view={html_lib.escape(view)}"
    if q_value:
        api_link += f"&q={html_lib.escape(q_value)}"
    if ADMIN_API_KEY and key:
        api_link += f"&key={html_lib.escape(key)}"

    html_page = f"""
    <!doctype html>
    <html lang="ja">
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      {meta_refresh}
      <title>HOT顧客ダッシュボード</title>
      <style>
        body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 24px; }}
        .top {{ display:flex; gap:16px; align-items:flex-end; flex-wrap:wrap; }}
        .card {{ padding: 12px 16px; border: 1px solid #ddd; border-radius: 12px; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 12px; }}
        th, td {{ border-bottom: 1px solid #eee; padding: 10px; vertical-align: top; }}
        th {{ text-align: left; background: #fafafa; position: sticky; top: 0; }}
        .muted {{ color: #666; font-size: 12px; }}
        .pill {{ display:inline-block; padding: 2px 8px; border-radius: 999px; background:#111; color:#fff; font-size:12px; }}
        input, select {{ padding: 8px; border: 1px solid #ddd; border-radius: 10px; }}
        button {{ padding: 8px 12px; border: 1px solid #111; background:#111; color:#fff; border-radius: 10px; cursor:pointer; }}
        a {{ color: #0b5; text-decoration: none; }}

        /* 10段階の行背景色（寒色→暖色） */
        .lvl-1  {{ background: #e3f2fd; }}
        .lvl-2  {{ background: #e1f5fe; }}
        .lvl-3  {{ background: #e0f7fa; }}
        .lvl-4  {{ background: #e8f5e9; }}
        .lvl-5  {{ background: #f1f8e9; }}
        .lvl-6  {{ background: #fffde7; }}
        .lvl-7  {{ background: #fff8e1; }}
        .lvl-8  {{ background: #fff3e0; }}
        .lvl-9  {{ background: #fbe9e7; }}
        .lvl-10 {{ background: #ffebee; }}

        .badge {{
          display:inline-block;
          min-width: 28px;
          text-align:center;
          font-weight:700;
          padding: 2px 8px;
          border-radius: 999px;
          border: 1px solid #ddd;
          background: #fff;
        }}

        /* 10段階のバッジ色（少し濃いめ） */
        .badge-lvl-1  {{ background:#bbdefb; border-color:#64b5f6; }}
        .badge-lvl-2  {{ background:#b3e5fc; border-color:#4fc3f7; }}
        .badge-lvl-3  {{ background:#b2ebf2; border-color:#4dd0e1; }}
        .badge-lvl-4  {{ background:#c8e6c9; border-color:#81c784; }}
        .badge-lvl-5  {{ background:#dcedc8; border-color:#aed581; }}
        .badge-lvl-6  {{ background:#fff9c4; border-color:#fff176; }}
        .badge-lvl-7  {{ background:#ffecb3; border-color:#ffd54f; }}
        .badge-lvl-8  {{ background:#ffe0b2; border-color:#ffb74d; }}
        .badge-lvl-9  {{ background:#ffccbc; border-color:#ff8a65; }}
        .badge-lvl-10 {{ background:#ffcdd2; border-color:#e57373; }}

        .rowtools {{ display:flex; gap:8px; flex-wrap:wrap; align-items:center; margin-top:10px; }}
        .kpi {{ display:flex; gap:10px; flex-wrap:wrap; align-items:center; }}
        .kpi span {{ font-size:12px; color:#666; }}
        .kpi b {{ font-size:14px; }}
        .btn-ghost {{
          padding: 8px 12px;
          border: 1px solid #ddd;
          background: #fff;
          color: #111;
          border-radius: 10px;
          cursor: pointer;
        }}
      </style>
    </head>
    <body>
      <div class="top">
        <div class="card">
          <div><span class="pill">HOT顧客</span> <b>{html_lib.escape(shop_id)}</b></div>
          <div class="muted">view={html_lib.escape(view)} / min_level={min_level} / limit={limit} / refresh={refresh}s / total={len(items)} / {now_str}</div>
          <div class="muted">温度カラー：{legend_html}（1=低温 → 10=最熱）</div>
          <div class="muted">JSON: <a href="{api_link}">{api_link}</a></div>

          <div class="rowtools">
            <div>
              <div class="muted">検索（自動）</div>
              <input id="searchBox" value="{html_lib.escape(q_value, quote=True)}" placeholder="user_id / ゴール / メッセージなど" style="min-width:320px;">
            </div>
            <div class="kpi">
              <span>表示</span><b id="visibleCount">{len(items)}</b>
              <span>/ 全件</span><b id="totalCount">{len(items)}</b>
            </div>
            <div>
              <button class="btn-ghost" type="button" id="clearBtn">クリア</button>
            </div>
          </div>
        </div>

        <form class="card" method="get" action="/dashboard">
          <div class="muted">フィルタ</div>
          <div style="display:flex; gap:8px; flex-wrap:wrap; margin-top:8px;">
            <input name="shop_id" value="{html_lib.escape(shop_id)}" placeholder="shop_id">
            <select name="view">
              <option value="events" {"selected" if view=="events" else ""}>events（履歴）</option>
              <option value="latest" {"selected" if view=="latest" else ""}>latest（最新）</option>
            </select>
            <input name="min_level" value="{min_level}" placeholder="min_level (1-10)">
            <input name="limit" value="{limit}" placeholder="limit (<=200)">
            <input name="refresh" value="{refresh}" placeholder="refresh seconds (0=off)">
            <input name="q" value="{html_lib.escape(q_value, quote=True)}" placeholder="検索（q理解用）">
            {"<input name='key' value='"+html_lib.escape(key)+"' placeholder='admin key'>" if ADMIN_API_KEY else ""}
            <button type="submit">更新</button>
          </div>
        </form>
      </div>

      <table>
        <thead>
          <tr>
            <th>更新</th>
            <th>温度</th>
            <th>確度</th>
            <th>次のゴール</th>
            <th>user_id</th>
            <th>直近メッセージ</th>
          </tr>
        </thead>
        <tbody id="tbody">
          {rows_html if rows_html else "<tr><td colspan='6' class='muted'>該当なし</td></tr>"}
        </tbody>
      </table>

      <script>
        (function() {{
          const searchBox = document.getElementById("searchBox");
          const clearBtn = document.getElementById("clearBtn");
          const tbody = document.getElementById("tbody");
          const visibleCount = document.getElementById("visibleCount");
          const totalCount = document.getElementById("totalCount");

          function updateUrlParam(q) {{
            try {{
              const url = new URL(window.location.href);
              if (q && q.trim().length > 0) {{
                url.searchParams.set("q", q);
              }} else {{
                url.searchParams.delete("q");
              }}
              window.history.replaceState({{}}, "", url.toString());
            }} catch (e) {{}}
          }}

          function applyFilter() {{
            const q = (searchBox.value || "").trim().toLowerCase();
            const rows = tbody.querySelectorAll("tr");
            let shown = 0;
            rows.forEach((tr) => {{
              const blob = (tr.getAttribute("data-search") || "");
              const hit = q === "" ? true : blob.indexOf(q) !== -1;
              tr.style.display = hit ? "" : "none";
              if (hit) shown += 1;
            }});
            visibleCount.textContent = String(shown);
            totalCount.textContent = String(rows.length);
            updateUrlParam(q);
          }}

          let timer = null;
          searchBox.addEventListener("input", () => {{
            if (timer) clearTimeout(timer);
            timer = setTimeout(applyFilter, 50);
          }});

          clearBtn.addEventListener("click", () => {{
            searchBox.value = "";
            applyFilter();
            searchBox.focus();
          }});

          applyFilter();
        }})();
      </script>
    </body>
    </html>
    """
    return HTMLResponse(html_page)


# =========================
# Webhook
# =========================
@app.post("/line/webhook")
async def line_webhook(
    request: Request,
    x_line_signature: str = Header(None, alias="X-Line-Signature"),
):
    body = await request.body()
    if not x_line_signature or not verify_signature(body, x_line_signature):
        raise HTTPException(status_code=400, detail="Invalid signature")

    data = json.loads(body.decode("utf-8"))

    for ev in data.get("events", []):
        if ev.get("type") != "message":
            continue

        msg = ev.get("message", {})
        if msg.get("type") != "text":
            continue

        reply_token = ev.get("replyToken", "")
        user_text = msg.get("text", "")

        source = ev.get("source", {})
        src_type = source.get("type", "unknown")  # user/group/room
        src_id = source.get("userId") or source.get("groupId") or source.get("roomId") or "unknown"
        conv_key = f"{src_type}:{src_id}"

        CHAT_HISTORY[conv_key].append({"role": "user", "content": user_text})

        last_msgs = list(CHAT_HISTORY[conv_key])[-20:]
        temp_info, reply_text = await analyze_and_generate_reply(last_msgs, user_text)

        TEMP_HISTORY[conv_key].append(int(temp_info["temperature_level"]))
        stable = median(list(TEMP_HISTORY[conv_key]), default=5)
        temp_info["temperature_level_stable"] = stable

        CHAT_HISTORY[conv_key].append({"role": "assistant", "content": reply_text})

        if can_db():
            try:
                await asyncio.to_thread(
                    db_write_interaction_sync,
                    SHOP_ID,
                    conv_key,
                    user_text,
                    reply_text,
                    int(temp_info["temperature_level"]),
                    int(temp_info["temperature_level_stable"]),
                    float(temp_info["confidence"]),
                    str(temp_info["next_goal"]),
                )
                print(f"[DB] wrote conv_key={conv_key} shop_id={SHOP_ID}")
            except Exception as e:
                print(f"[DB] write failed: {e}")
        else:
            print("[DB] skipped (missing DATABASE_URL or SHOP_ID)")

        print(
            f"[TEMP] key={conv_key} level_raw={temp_info['temperature_level']} "
            f"level_stable={temp_info['temperature_level_stable']} conf={temp_info['confidence']} "
            f"goal={temp_info['next_goal']}"
        )

        try:
            await reply_message(reply_token, reply_text)
        except Exception as e:
            print(f"[LINE] reply failed: {e}")

    return {"ok": True}
