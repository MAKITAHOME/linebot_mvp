import os
import json
import hmac
import hashlib
import base64
import asyncio
import ssl
import html as html_lib
from datetime import datetime
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

# 設定すると /api/* と /dashboard を保護（未設定なら開発用に公開）
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "")

# DB SSL: auto(推奨) / verify / disable
DB_SSL_MODE = os.getenv("DB_SSL_MODE", "auto").lower().strip()

# デバッグログ増やしたい時だけ 1
DEBUG = os.getenv("DEBUG", "0") == "1"


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
    """ADMIN_API_KEYがある時だけ認証"""
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

        # インデックス
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_customers_shop_level_updated
        ON customers (shop_id, temp_level_stable, updated_at DESC);
        """)
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_customer_events_shop_created
        ON customer_events (shop_id, created_at DESC);
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


def _rows_to_items(rows):
    items = []
    for conv_key, user_text, raw, stable, conf, goal, ts in rows:
        try:
            ts_str = ts.isoformat()
        except Exception:
            ts_str = str(ts)

        user_id = None
        if isinstance(conv_key, str) and conv_key.startswith("user:"):
            user_id = conv_key.split("user:", 1)[1]

        items.append(
            {
                "conv_key": conv_key,
                "user_id": user_id,
                "last_user_text": user_text,
                "temp_level_raw": raw,
                "temp_level_stable": stable,
                "confidence": conf,
                "next_goal": goal,
                "updated_at": ts_str,
            }
        )
    return items


def db_fetch_hot_customers_sync(shop_id: str, min_level: int, limit: int):
    """最新状態（customers）"""
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
              conv_key,
              last_user_text,
              temp_level_raw,
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
        return _rows_to_items(cur.fetchall())
    finally:
        conn.close()


def db_fetch_hot_events_sync(shop_id: str, min_level: int, limit: int):
    """履歴（customer_events）"""
    conn = db_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
              conv_key,
              user_text,
              temp_level_raw,
              temp_level_stable,
              confidence,
              next_goal,
              created_at
            FROM customer_events
            WHERE shop_id = %s AND temp_level_stable >= %s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (shop_id, min_level, limit),
        )
        return _rows_to_items(cur.fetchall())
    finally:
        conn.close()


# =========================
# LINE Reply
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

        # 自信が低いときは安全側
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
# Routes
# =========================
@app.get("/")
async def root():
    return {"ok": True}


@app.get("/api/hot")
async def api_hot(
    shop_id: str = SHOP_ID,
    min_level: int = 8,
    limit: int = 50,
    view: str = "latest",        # latest / events
    key: Optional[str] = None,   # ブラウザ用
    x_admin_key: Optional[str] = Header(None, alias="X-Admin-Key"),
):
    check_admin_key(x_admin_key, key)

    min_level = max(1, min(10, int(min_level)))
    limit = max(1, min(200, int(limit)))
    view = (view or "latest").lower()

    if not can_db():
        return {"shop_id": shop_id, "min_level": min_level, "count": 0, "items": [], "error": "DB not configured"}

    if view == "events":
        items = await asyncio.to_thread(db_fetch_hot_events_sync, shop_id, min_level, limit)
    else:
        items = await asyncio.to_thread(db_fetch_hot_customers_sync, shop_id, min_level, limit)

    return {"shop_id": shop_id, "min_level": min_level, "view": view, "count": len(items), "items": items}


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(
    shop_id: str = SHOP_ID,
    min_level: int = 8,
    limit: int = 50,
    view: str = "events",        # dashboardは履歴表示がデフォルト
    key: Optional[str] = None,   # ブラウザ用
    x_admin_key: Optional[str] = Header(None, alias="X-Admin-Key"),
):
    check_admin_key(x_admin_key, key)

    min_level = max(1, min(10, int(min_level)))
    limit = max(1, min(200, int(limit)))
    view = (view or "events").lower()

    if not can_db():
        return HTMLResponse("<h2>DB未設定</h2><p>DATABASE_URL / SHOP_ID を設定してください。</p>", status_code=500)

    if view == "latest":
        items = await asyncio.to_thread(db_fetch_hot_customers_sync, shop_id, min_level, limit)
    else:
        items = await asyncio.to_thread(db_fetch_hot_events_sync, shop_id, min_level, limit)

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    rows_html = ""
    for it in items:
        rows_html += f"""
        <tr>
          <td>{html_lib.escape(str(it.get("updated_at","")))}</td>
          <td style="text-align:center">{html_lib.escape(str(it.get("temp_level_stable","")))}</td>
          <td style="text-align:center">{html_lib.escape(str(it.get("confidence","")))}</td>
          <td>{html_lib.escape(str(it.get("next_goal","")))}</td>
          <td><code>{html_lib.escape(str(it.get("user_id") or ""))}</code></td>
          <td>{html_lib.escape(str(it.get("last_user_text","")))}</td>
        </tr>
        """

    api_link = f"/api/hot?shop_id={html_lib.escape(shop_id)}&min_level={min_level}&limit={limit}&view={html_lib.escape(view)}"
    if ADMIN_API_KEY and key:
        api_link += f"&key={html_lib.escape(key)}"

    html_page = f"""
    <!doctype html>
    <html lang="ja">
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
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
      </style>
    </head>
    <body>
      <div class="top">
        <div class="card">
          <div><span class="pill">HOT顧客</span> <b>{html_lib.escape(shop_id)}</b></div>
          <div class="muted">view={html_lib.escape(view)} / min_level={min_level} / limit={limit} / count={len(items)} / {now}</div>
          <div class="muted">JSON: <a href="{api_link}">{api_link}</a></div>
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
        <tbody>
          {rows_html if rows_html else "<tr><td colspan='6' class='muted'>該当なし</td></tr>"}
        </tbody>
      </table>
    </body>
    </html>
    """
    return HTMLResponse(html_page)


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

        CHAT_HISTORY[conv_key].append({"role": "assistant", "content": reply_text})

        # DB write (best-effort)
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

        # reply
        try:
            await reply_message(reply_token, reply_text)
        except Exception as e:
            print(f"[LINE] reply failed: {e}")

    return {"ok": True}
