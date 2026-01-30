import os
import json
import hmac
import hashlib
import base64
import httpx
from fastapi import FastAPI, Request, Header, HTTPException

app = FastAPI()

LINE_CHANNEL_SECRET = os.getenv("LINE_CHANNEL_SECRET", "")
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

# ---------- 署名チェック ----------
def verify_signature(body: bytes, signature: str) -> bool:
    if not LINE_CHANNEL_SECRET:
        return False
    mac = hmac.new(
        LINE_CHANNEL_SECRET.encode("utf-8"),
        body,
        hashlib.sha256
    ).digest()
    expected = base64.b64encode(mac).decode("utf-8")
    return hmac.compare_digest(expected, signature)

# ---------- AI返信生成 ----------
async def generate_ai_reply(user_text: str) -> str:
    if not OPENAI_API_KEY:
        return "AIの設定がまだ完了していません。"

    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }

    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {
                "role": "system",
                "content": (
                    "あなたは不動産の追客担当です。"
                    "丁寧で短い日本語で返信してください。"
                    "必ず質問か次の提案を入れてください。"
                ),
            },
            {"role": "user", "content": user_text},
        ],
        "temperature": 0.6,
    }

    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(
            "https://api.openai.com/v1/chat/completions",
            headers=headers,
            json=payload,
        )
        r.raise_for_status()
        data = r.json()

    return data["choices"][0]["message"]["content"].strip()

# ---------- LINE返信 ----------
async def reply_message(reply_token: str, text: str):
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

# ---------- 動作確認 ----------
@app.get("/")
async def root():
    return {"ok": True}

# ---------- LINE Webhook ----------
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
        if ev.get("type") == "message" and ev.get("message", {}).get("type") == "text":
            txt = ev["message"]["text"]
            ai_text = await generate_ai_reply(txt)
            await reply_message(ev["replyToken"], ai_text)

    return {"ok": True}
