import os
import json
import hmac
import hashlib
import base64
import asyncio
import ssl
from collections import deque, defaultdict
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse, unquote

import httpx
import pg8000
from fastapi import FastAPI, Request, Header, HTTPException

app = FastAPI()

# ===== ENV =====
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "")
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

DATABASE_URL = os.getenv("DATABASE_URL", "")  # Render Environmentに入れる
SHOP_ID = os.getenv("SHOP_ID", "tokyo_01")    # Render Environmentに入れる

# ===== In-memory history (MVP) =====
CHAT_HISTORY = defaultdict(lambda: deque(maxlen=40))  # max 20 turns
TEMP_HISTORY = defaultdict(lambda: deque(maxlen=3))   # median smoothing


# ---------------------------
# Helpers
# ---------------------------
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
            return json.loads(text[start:end + 1])
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
    """
    OpenAIが '高い' '低い' '80%' などを返しても落ちないように数値化する。
    返り値は 0.0〜1.0 にクランプ。
    """
    if isinstance(raw, (int, float)):
        return max(0.0, min(1.0, float(raw)))

    if isinstance(raw, str):
        s = raw.strip().lower()

        # 日本語ラベル
        if s in {"高い", "高め", "high"}:
            return 0.85
        if s in {"中", "普通", "ふつう", "medium"}:
            return 0.60
        if s in {"低い", "低め", "low"}:
            return 0.40

        # "80%"
        if s.endswith("%"):
            try:
                v = float(s[:-1]) / 100.0
                return max(0.0, min(1.0, v))
            except Exception:
                return default

        # "0.8" / "80"
        try:
            v = float(s)
            if v > 1.0 and v <= 100.0:
                v = v / 100.0
            return max(0.0, min(1.0, v))
        except Exception:
            return default

    return default


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


def db_connect():
    cfg = parse_database_url(DATABASE_URL)
    ctx = ssl.create_default_context()
    # Render PostgresはTLSが普通に通る
    return pg8000.connect(
        user=cfg["user"],
        password=cfg["password"],
        host=cfg["host"],
        port=cfg["port"],
        database=cfg["database"],
        ssl_context=ctx,
    )


def ensure_tables_sync():
    if not can_db():
        print("[DB] skipped ensure_tables (missing DATABASE_URL or SHOP_ID)")
        return
    conn = db_connect()
    try:
        cur = conn.cursor()
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
        conn.commit()
        print("[DB] tables ensured")
    finally:
        conn.close()


def db_insert_message_sync(shop_id: str, conv_key: str, role: str, content: str):
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO messages (shop_id, conv_key, role, content) VALUES (%s,%s,%s,%s)",
            (shop_id, conv_key, role, content),
        )
        conn.commit()
    finally:
        conn.close()


def db_upsert_customer_sync(
    shop_id: str,
    conv_key: str,
    last_user_text: str,
    raw_level: int,
    stable_level: int,
    conf: float,
    next_goal: str,
):
    conn = db_connect()
    try:
        cur = conn.cursor()
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
            (shop_id, conv_key, last_user_text, raw_level, stable_level, conf, next_goal),
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------
# LINE reply
# ---------------------------
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


# ---------------------------
# OpenAI: temp(10) + reply
# ---------------------------
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
        "temperature_level は 1〜10 の整数、confidence は 0〜1 の数値（文字は禁止）。\n"
        "keys: temperature_level, confidence, signals, next_goal, reply_text"
    )

    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": f"【直近会話】\n{json.dumps(last_messages[-20:], ensure_ascii=False)}\n\n【最新】\n{user_text}\n",
            },
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

        # 安全側（自信低いときは真ん中寄せ）
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


@app.on_event("startup")
async def on_startup():
    # テーブルを保証（失敗しても起動は続ける）
    try:
        await asyncio.to_thread(ensure_tables_sync)
    except Exception as e:
        print(f"[DB] ensure tables failed: {e}")


@app.get("/")
async def root():
    return {"ok": True}


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
        src_type = source.get("type", "unknown")
        src_id = source.get("userId") or source.get("groupId") or source.get("roomId") or "unknown"
        conv_key = f"{src_type}:{src_id}"

        # memory
        CHAT_HISTORY[conv_key].append({"role": "user", "content": user_text})

        # AI analyze + reply
        last_msgs = list(CHAT_HISTORY[conv_key])[-20:]
        temp_info, reply_text = await analyze_and_generate_reply(last_msgs, user_text)

        # stabilize
        TEMP_HISTORY[conv_key].append(int(temp_info["temperature_level"]))
        stable = median(list(TEMP_HISTORY[conv_key]), default=5)
        temp_info["temperature_level_stable"] = stable

        # memory
        CHAT_HISTORY[conv_key].append({"role": "assistant", "content": reply_text})

        # DB write (best-effort / never block LINE reply)
        if can_db():
            try:
                await asyncio.to_thread(db_insert_message_sync, SHOP_ID, conv_key, "user", user_text)
                await asyncio.to_thread(db_insert_message_sync, SHOP_ID, conv_key, "assistant", reply_text)
                await asyncio.to_thread(
                    db_upsert_customer_sync,
                    SHOP_ID, conv_key, user_text,
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

        # logs
        print(
            f"[TEMP] key={conv_key} level_raw={temp_info['temperature_level']} "
            f"level_stable={temp_info['temperature_level_stable']} conf={temp_info['confidence']} "
            f"goal={temp_info['next_goal']}"
        )

        # reply
        await reply_message(reply_token, reply_text)

    return {"ok": True}
