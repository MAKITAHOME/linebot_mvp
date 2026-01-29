import os, json, hmac, hashlib, base64
import httpx
from fastapi import FastAPI, Request, Header, HTTPException

app = FastAPI()

SECRET = os.getenv("LINE_CHANNEL_SECRET", "")
TOKEN  = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")

def verify_signature(body: bytes, signature: str) -> bool:
    if not SECRET:
        return False
    mac = hmac.new(SECRET.encode("utf-8"), body, hashlib.sha256).digest()
    expected = base64.b64encode(mac).decode("utf-8")
    return hmac.compare_digest(expected, signature)

async def reply_message(reply_token: str, text: str):
    url = "https://api.line.me/v2/bot/message/reply"
    headers = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
    payload = {"replyToken": reply_token, "messages": [{"type": "text", "text": text}]}
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(url, headers=headers, json=payload)
        r.raise_for_status()

@app.post("/line/webhook")
async def line_webhook(request: Request, x_line_signature: str = Header(None)):
    body = await request.body()

    if not x_line_signature or not verify_signature(body, x_line_signature):
        raise HTTPException(status_code=400, detail="Invalid signature")

    data = json.loads(body.decode("utf-8"))
    for ev in data.get("events", []):
        if ev.get("type") == "message" and ev.get("message", {}).get("type") == "text":
            txt = ev["message"]["text"]
            await reply_message(ev["replyToken"], f"受け取った: {txt}")

    return {"ok": True}
