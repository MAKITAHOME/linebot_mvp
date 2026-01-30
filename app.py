import os
import json
import hmac
import hashlib
import base64
from collections import deque, defaultdict

import httpx
from fastapi import FastAPI, Request, Header, HTTPException

app = FastAPI()

# ====== 環境変数 ======
LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "")
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

# ====== 会話履歴（超かんたん版：メモリ保存） ======
# userIdごとに直近20件（user/assistant合計）保存
CHAT_HISTORY = defaultdict(lambda: deque(maxlen=20))


# ====== LINE署名チェック ======
def verify_signature(body: bytes, signature: str) -> bool:
    if not LINE_CHANNEL_SECRET:
        return False
    mac = hmac.new(LINE_CHANNEL_SECRET.encode("utf-8"), body, hashlib.sha256).digest()
    expected = base64.b64encode(mac).decode("utf-8")
    return hmac.compare_digest(expected, signature)


# ====== LINEへ返信 ======
async def reply_message(reply_token: str, text: str):
    if not LINE_CHANNEL_ACCESS_TOKEN:
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

    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(url, headers=headers, json=payload)
        r.raise_for_status()


# ====== OpenAI：温度感(5段階)判定 + 返信文生成（1回でまとめて） ======
def _extract_json(text: str) -> dict:
    """
    たまに ```json ... ``` みたいに返ることがあるので救出する
    """
    text = text.strip()

    # そのままJSONならそれでOK
    try:
        return json.loads(text)
    except Exception:
        pass

    # JSON部分だけ抜く（最初の{〜最後の}）
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(text[start : end + 1])
        except Exception:
            pass

    return {}


async def analyze_and_generate_reply(last_messages: list[dict], user_text: str) -> tuple[dict, str]:
    """
    last_messages: [{"role":"user"/"assistant","content":"..."}]
    return: (temp_info_dict, reply_text)
    """
    # OpenAIが使えない時も必ず返信する（未読防止）
    fallback = "お問い合わせありがとうございます。内容を確認して改めてご連絡いたします。"

    if not OPENAI_API_KEY:
        return (
            {"temperature_level": 3, "confidence": 0.4, "signals": ["no_api_key"], "next_goal": "条件整理"},
            fallback,
        )

    system_prompt = (
        "あなたは不動産の追客AIです。"
        "以下の直近会話（user/assistant）とユーザーの最新発言を読み、"
        "1) 温度感を5段階(1〜5)で判定し、2) その温度感に合う返信文を作ってください。\n\n"
        "【温度感の基準】\n"
        "5: すぐ動く（内見日程/申込/今日明日/電話希望など）\n"
        "4: 前向き（条件が具体的、返信が速い、深掘り質問が多い）\n"
        "3: 検討中（比較/相談、返信はある、条件は揺れている）\n"
        "2: 低反応（短文・受け身・質問に答えないことが増える）\n"
        "1: ほぼ反応なし（既読スルー傾向、会話が進まない）\n\n"
        "【返信の方針】\n"
        "・日本語で丁寧に、2〜5文。\n"
        "・断定しない（空室や条件は『確認します』など慎重に）。\n"
        "・温度が高いほど『次の行動』を具体化（内見日程2択など）。\n"
        "・温度が低いほど追いすぎず、質問は1つまで。\n\n"
        "【出力形式】\n"
        "必ず JSON のみで返してください（文章や説明は禁止）。\n"
        "keys: temperature_level(1-5), confidence(0-1), signals(array), next_goal(string), reply_text(string)\n"
    )

    # 会話は直近20件まで（＝最大10往復）
    convo = last_messages[-20:]

    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": (
                    "【直近会話】\n"
                    f"{json.dumps(convo, ensure_ascii=False)}\n\n"
                    "【最新のユーザー発言】\n"
                    f"{user_text}\n"
                ),
            },
        ],
        "temperature": 0.4,
    }

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload)
            r.raise_for_status()
            data = r.json()

        content = data["choices"][0]["message"]["content"].strip()
        result = _extract_json(content)

        # パース失敗したらfallback
        if not result:
            return (
                {"temperature_level": 3, "confidence": 0.4, "signals": ["json_parse_failed"], "next_goal": "条件整理"},
                fallback,
            )

        # 安全に補正
        lvl = int(result.get("temperature_level", 3))
        if lvl < 1:
            lvl = 1
        if lvl > 5:
            lvl = 5

        conf = float(result.get("confidence", 0.7))
        if conf < 0:
            conf = 0.0
        if conf > 1:
            conf = 1.0

        reply_text = str(result.get("reply_text", "")).strip()
        if not reply_text:
            reply_text = fallback

        temp_info = {
            "temperature_level": lvl,
            "confidence": conf,
            "signals": result.get("signals", []),
            "next_goal": result.get("next_goal", "条件整理"),
        }

        # 低自信なら安全側（真ん中）に倒す
        if conf < 0.6:
            temp_info["temperature_level"] = 3

        return temp_info, reply_text

    except Exception:
        return (
            {"temperature_level": 3, "confidence": 0.4, "signals": ["openai_error"], "next_goal": "条件整理"},
            fallback,
        )


@app.get("/")
async def root():
    return {"ok": True}


# ====== LINE Webhook ======
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
        try:
            if ev.get("type") != "message":
                continue

            message = ev.get("message", {})
            if message.get("type") != "text":
                continue

            reply_token = ev.get("replyToken", "")
            if not reply_token:
                continue

            txt = message.get("text", "")

            # 会話キー（userIdがないケースもあるので保険）
            source = ev.get("source", {})
            src_type = source.get("type", "unknown")
            src_id = source.get("userId") or source.get("groupId") or source.get("roomId") or "unknown"
            conv_key = f"{src_type}:{src_id}"

            # user発言を保存
            CHAT_HISTORY[conv_key].append({"role": "user", "content": txt})

            # 直近会話（最大20件）で温度判定 + 返信生成
            last_msgs = list(CHAT_HISTORY[conv_key])[-20:]
            temp_info, ai_text = await analyze_and_generate_reply(last_msgs, txt)

            # assistant発言も保存
            CHAT_HISTORY[conv_key].append({"role": "assistant", "content": ai_text})

            # デバッグ（Render Logsで確認できる）
            print(f"[TEMP] key={conv_key} level={temp_info['temperature_level']} conf={temp_info['confidence']} goal={temp_info.get('next_goal')}")

            await reply_message(reply_token, ai_text)

        except Exception as e:
            # ここで落ちても webhook 自体は返す（未読防止）
            print(f"[ERROR] {e}")

    return {"ok": True}
