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

# ====== 会話履歴（メモリ保存 / MVP用） ======
# userIdごとに直近40件（user/assistant合計）保存（=最大20往復）
CHAT_HISTORY = defaultdict(lambda: deque(maxlen=40))

# 温度レベル履歴（揺れ止め用：直近3回の中央値）
TEMP_HISTORY = defaultdict(lambda: deque(maxlen=3))


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


# ====== OpenAIレスポンスからJSONを救出 ======
def _extract_json(text: str) -> dict:
    text = (text or "").strip()

    # そのままJSONならOK
    try:
        return json.loads(text)
    except Exception:
        pass

    # ```json ... ``` など対策：最初の{〜最後の}だけ抜き出す
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(text[start : end + 1])
        except Exception:
            pass

    return {}


def _median_of_three(values: list[int]) -> int:
    # valuesは最大3件想定
    if not values:
        return 5
    s = sorted(values)
    return s[len(s) // 2]


# ====== OpenAI：温度感(10段階)判定 + 返信文生成（1回でまとめて） ======
async def analyze_and_generate_reply(last_messages: list[dict], user_text: str) -> tuple[dict, str]:
    """
    last_messages: [{"role":"user"/"assistant","content":"..."}]
    return: (temp_info_dict, reply_text)
    """

    fallback = "お問い合わせありがとうございます。内容を確認して改めてご連絡いたします。"

    if not OPENAI_API_KEY:
        return (
            {"temperature_level": 5, "confidence": 0.4, "signals": ["no_api_key"], "next_goal": "条件整理"},
            fallback,
        )

    # 会話は直近20件（=最大10往復）だけ使う
    convo = last_messages[-20:]

    system_prompt = (
        "あなたは不動産の追客AIです。以下の直近会話（user/assistant）と最新のユーザー発言を読み、"
        "1) 見込み客の温度感を10段階(1〜10)で判定し、2) その温度感に合う返信文を作ってください。\n\n"
        "【温度感10段階の基準（必ずこの定義に従う）】\n"
        "1: ほぼ脈なし（既読スルー傾向、会話が進まない）\n"
        "2: 低反応（短文・受け身、質問に答えないことが増える）\n"
        "3: 情報収集（ふわっと、検討開始）\n"
        "4: 条件相談（希望が少し具体化、相談が増える）\n"
        "5: 検討中（比較中、返信はある）\n"
        "6: 前向き（条件がかなり具体、やり取りが活発）\n"
        "7: 候補比較（内見に近い質問や比較が出る）\n"
        "8: 内見検討（見に行けるか/空き状況/内見可否の話）\n"
        "9: 内見日程調整（日時の具体、候補日が出る）\n"
        "10: 申込/今すぐ（申込したい、至急、電話希望、緊急度が高い）\n\n"
        "【返信の方針】\n"
        "・日本語で丁寧に、2〜5文。\n"
        "・断定しない（空室や条件は『確認します』など慎重に）。\n"
        "・温度が高いほど『次の行動』を具体化（内見日程2択など）。\n"
        "・温度が低いほど追いすぎず、質問は1つまで。\n\n"
        "【出力形式】\n"
        "必ず JSON のみで返してください（文章や説明は禁止）。\n"
        "keys: temperature_level(1-10), confidence(0-1), signals(array), next_goal(string), reply_text(string)\n"
    )

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

        # JSONが取れなければfallback
        if not result:
            return (
                {"temperature_level": 5, "confidence": 0.4, "signals": ["json_parse_failed"], "next_goal": "条件整理"},
                fallback,
            )

        # 安全に補正
        lvl = int(result.get("temperature_level", 5))
        lvl = max(1, min(10, lvl))

        conf = float(result.get("confidence", 0.7))
        conf = max(0.0, min(1.0, conf))

        reply_text = str(result.get("reply_text", "")).strip() or fallback
        signals = result.get("signals", [])
        next_goal = str(result.get("next_goal", "条件整理"))

        # 低自信なら安全側（真ん中）に倒す
        if conf < 0.6:
            lvl = 5

        temp_info = {
            "temperature_level": lvl,
            "confidence": conf,
            "signals": signals,
            "next_goal": next_goal,
        }

        return temp_info, reply_text

    except Exception:
        return (
            {"temperature_level": 5, "confidence": 0.4, "signals": ["openai_error"], "next_goal": "条件整理"},
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

            # 会話キー（userIdが無い場合もあるので保険）
            source = ev.get("source", {})
            src_type = source.get("type", "unknown")
            src_id = source.get("userId") or source.get("groupId") or source.get("roomId") or "unknown"
            conv_key = f"{src_type}:{src_id}"

            # user発言を保存
            CHAT_HISTORY[conv_key].append({"role": "user", "content": txt})

            # 温度判定 + 返信生成（AI1回）
            last_msgs = list(CHAT_HISTORY[conv_key])[-20:]
            temp_info, ai_text = await analyze_and_generate_reply(last_msgs, txt)

            # 揺れ止め：直近3回の中央値を採用（SaaS運用で効く）
            TEMP_HISTORY[conv_key].append(int(temp_info.get("temperature_level", 5)))
            stabilized = _median_of_three(list(TEMP_HISTORY[conv_key]))
            temp_info["temperature_level_stable"] = stabilized

            # assistant発言も保存
            CHAT_HISTORY[conv_key].append({"role": "assistant", "content": ai_text})

            # デバッグ（Render Logsで確認）
            print(
                f"[TEMP] key={conv_key} "
                f"level_raw={temp_info.get('temperature_level')} "
                f"level_stable={temp_info.get('temperature_level_stable')} "
                f"conf={temp_info.get('confidence')} "
                f"goal={temp_info.get('next_goal')}"
            )

            await reply_message(reply_token, ai_text)

        except Exception as e:
            # 落ちても webhook 自体は返す（未読防止）
            print(f"[ERROR] {e}")

    return {"ok": True}
