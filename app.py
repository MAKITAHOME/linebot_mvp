import os
import json
import hmac
import hashlib
import base64
import subprocess
from collections import deque, defaultdict
from typing import Any, Dict, List, Tuple

import httpx
from fastapi import FastAPI, Request, Header, HTTPException

app = FastAPI()

# ====== ENV ======
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "")
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

DATABASE_URL = os.getenv("DATABASE_URL", "")  # postgres://... (Render Environment)
SHOP_ID = os.getenv("SHOP_ID", "tokyo_01")

# ====== In-memory history (MVP) ======
CHAT_HISTORY = defaultdict(lambda: deque(maxlen=40))  # max 20 turns
TEMP_HISTORY = defaultdict(lambda: deque(maxlen=3))   # median smoothing


# ---------------------------
# Utils
# ---------------------------
def verify_signature(body: bytes, signature: str) -> bool:
    """Verify LINE webhook signature."""
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
    """Extract JSON object from model output (handles fenced or extra text)."""
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


# ---------------------------
# LINE Reply
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
# OpenAI: temperature(10) + reply (single call)
# ---------------------------
async def analyze_and_generate_reply(last_messages: List[Dict[str, str]], user_text: str) -> Tuple[Dict[str, Any], str]:
    fallback = "お問い合わせありがとうございます。内容を確認して改めてご連絡いたします。"

    if not OPENAI_API_KEY:
        return (
            {"temperature_level": 5, "confidence": 0.4, "next_goal": "条件整理", "signals": ["no_api_key"]},
            fallback,
        )

    system_prompt = (
        "あなたは不動産の追客AIです。直近会話と最新発言から温度感を10段階(1〜10)で判定し、"
        "温度感に合う返信文を作ってください。\n\n"
        "【温度感10段階】\n"
        "1:ほぼ脈なし 2:低反応 3:情報収集 4:条件相談 5:検討中 6:前向き 7:候補比較 8:内見検討 9:内見日程調整 10:申込/今すぐ\n\n"
        "【返信】丁寧な日本語で2〜5文。断定せず、必要なら『確認します』。"
        "温度が高いほど次アクションを具体化（例：内見日程2択）。\n\n"
        "【出力】必ずJSONのみ。keys: temperature_level, confidence, signals, next_goal, reply_text"
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
                {"temperature_level": 5, "confidence": 0.4, "next_goal": "条件整理", "signals": ["json_parse_failed"]},
                fallback,
            )

        lvl = int(result.get("temperature_level", 5))
        lvl = max(1, min(10, lvl))

        conf = float(result.get("confidence", 0.7))
        conf = max(0.0, min(1.0, conf))
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
            {"temperature_level": 5, "confidence": 0.4, "next_goal": "条件整理", "signals": ["openai_error"]},
            fallback,
        )


# ---------------------------
# DB write via psql (no Python DB libs)
# ---------------------------
def can_db_write() -> bool:
    return bool(DATABASE_URL) and bool(SHOP_ID)


def psql_exec(sql: str) -> None:
    """
    Execute SQL using `psql` command.
    Requires psql installed in runtime (Render usually has it) and DATABASE_URL.
    """
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL missing")

    # Use psql with connection string. ON_ERROR_STOP makes failures non-silent.
    cmd = ["psql", DATABASE_URL, "-v", "ON_ERROR_STOP=1", "-c", sql]
    # Capture output for logs
    out = subprocess.run(cmd, capture_output=True, text=True)
    if out.returncode != 0:
        raise RuntimeError(f"psql failed: {out.stderr.strip()}")


def db_write_message(shop_id: str, conv_key: str, role: str, content: str) -> None:
    safe = content.replace("'", "''")  # simplest escaping for single quotes
    sql = (
        "INSERT INTO messages (shop_id, conv_key, role, content) "
        f"VALUES ('{shop_id}', '{conv_key}', '{role}', '{safe}');"
    )
    psql_exec(sql)


def db_upsert_customer(shop_id: str, conv_key: str, last_user_text: str,
                       raw_level: int, stable_level: int, conf: float, next_goal: str) -> None:
    safe_text = last_user_text.replace("'", "''")
    safe_goal = next_goal.replace("'", "''")
    sql = f"""
    INSERT INTO customers
      (shop_id, conv_key, last_user_text, temp_level_raw, temp_level_stable, confidence, next_goal, updated_at)
    VALUES
      ('{shop_id}', '{conv_key}', '{safe_text}', {raw_level}, {stable_level}, {conf}, '{safe_goal}', now())
    ON CONFLICT (shop_id, conv_key)
    DO UPDATE SET
      last_user_text=EXCLUDED.last_user_text,
      temp_level_raw=EXCLUDED.temp_level_raw,
      temp_level_stable=EXCLUDED.temp_level_stable,
      confidence=EXCLUDED.confidence,
      next_goal=EXCLUDED.next_goal,
      updated_at=now();
    """
    psql_exec(sql)


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

        # Memory log
        CHAT_HISTORY[conv_key].append({"role": "user", "content": user_text})

        # AI analyze + reply
        last_msgs = list(CHAT_HISTORY[conv_key])[-20:]
        temp_info, reply_text = await analyze_and_generate_reply(last_msgs, user_text)

        # Stabilize temp
        TEMP_HISTORY[conv_key].append(int(temp_info["temperature_level"]))
        stable = median(list(TEMP_HISTORY[conv_key]), default=5)
        temp_info["temperature_level_stable"] = stable

        # Memory log
        CHAT_HISTORY[conv_key].append({"role": "assistant", "content": reply_text})

        # DB write (best-effort; never block reply)
        if can_db_write():
            try:
                db_write_message(SHOP_ID, conv_key, "user", user_text)
                db_write_message(SHOP_ID, conv_key, "assistant", reply_text)
                db_upsert_customer(
                    SHOP_ID,
                    conv_key,
                    user_text,
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

        # Logs
        print(
            f"[TEMP] key={conv_key} level_raw={temp_info['temperature_level']} "
            f"level_stable={temp_info['temperature_level_stable']} conf={temp_info['confidence']} "
            f"goal={temp_info['next_goal']}"
        )

        # Reply to LINE
        await reply_message(reply_token, reply_text)

    return {"ok": True}
