import os
import feedparser
import json
from flask import Flask, request, jsonify, render_template, Response
from apscheduler.schedulers.background import BackgroundScheduler
import urllib.parse
import queue
import threading
import psycopg
from psycopg.rows import dict_row
from pywebpush import webpush, WebPushException

app = Flask(__name__)
DATABASE_URL = os.environ.get("DATABASE_URL")
VAPID_PUBLIC_KEY = os.environ.get("VAPID_PUBLIC_KEY", "")
VAPID_PRIVATE_KEY = os.environ.get("VAPID_PRIVATE_KEY", "")
VAPID_CLAIMS = {"sub": "mailto:admin@example.com"}

# gunicorn起動時もテーブルを初期化する
def _init_on_startup():
    try:
        with get_db() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS companies (
                    id SERIAL PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS news (
                    id SERIAL PRIMARY KEY,
                    company_id INTEGER NOT NULL REFERENCES companies(id),
                    title TEXT NOT NULL,
                    url TEXT NOT NULL UNIQUE,
                    published TEXT,
                    summary TEXT,
                    fetched_at TIMESTAMP DEFAULT NOW(),
                    is_new BOOLEAN DEFAULT TRUE
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS push_subscriptions (
                    id SERIAL PRIMARY KEY,
                    endpoint TEXT UNIQUE NOT NULL,
                    subscription JSONB NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
    except Exception as e:
        print(f"DB init error: {e}")

clients: list[queue.Queue] = []
clients_lock = threading.Lock()


def get_db():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


_init_on_startup()


def init_db():
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS companies (
                id SERIAL PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS news (
                id SERIAL PRIMARY KEY,
                company_id INTEGER NOT NULL REFERENCES companies(id),
                title TEXT NOT NULL,
                url TEXT NOT NULL UNIQUE,
                published TEXT,
                summary TEXT,
                fetched_at TIMESTAMP DEFAULT NOW(),
                is_new BOOLEAN DEFAULT TRUE
            )
        """)


def fetch_news_for_company(company_id: int, company_name: str) -> int:
    query = urllib.parse.quote(company_name)
    url = f"https://news.google.com/rss/search?q={query}&hl=ja&gl=JP&ceid=JP:ja"
    try:
        feed = feedparser.parse(url)
    except Exception:
        return 0

    from datetime import date, timezone
    import email.utils

    today = date.today()

    def is_today(published_str):
        if not published_str:
            return False
        try:
            dt = email.utils.parsedate_to_datetime(published_str)
            return dt.astimezone(timezone.utc).date() == today
        except Exception:
            return False

    new_count = 0
    with get_db() as conn:
        for entry in feed.entries[:50]:
            published = entry.get("published", "")
            if not is_today(published):
                continue
            title = entry.get("title", "")
            link = entry.get("link", "")
            summary = entry.get("summary", "")[:300]
            try:
                conn.execute(
                    "INSERT INTO news (company_id, title, url, published, summary) VALUES (%s, %s, %s, %s, %s)",
                    (company_id, title, link, published, summary),
                )
                new_count += 1
            except Exception:
                conn.rollback()

    return new_count


def fetch_all_news():
    with get_db() as conn:
        companies = conn.execute("SELECT id, name FROM companies").fetchall()

    new_items = []
    for company in companies:
        count = fetch_news_for_company(company["id"], company["name"])
        if count > 0:
            new_items.append({"company": company["name"], "count": count})

    if new_items:
        broadcast_sse({"type": "new_news", "items": new_items, "total": sum(i["count"] for i in new_items)})
        body = "、".join([f'{i["company"]}({i["count"]}件)' for i in new_items])
        send_push_notifications("📰 新着ニュース", body)


def send_push_notifications(title: str, body: str):
    if not VAPID_PRIVATE_KEY:
        return
    with get_db() as conn:
        subs = conn.execute("SELECT subscription FROM push_subscriptions").fetchall()
    for row in subs:
        sub = row["subscription"]
        try:
            webpush(
                subscription_info=sub,
                data=json.dumps({"title": title, "body": body}, ensure_ascii=False),
                vapid_private_key=VAPID_PRIVATE_KEY,
                vapid_claims=VAPID_CLAIMS,
            )
        except WebPushException as e:
            if "410" in str(e) or "404" in str(e):
                with get_db() as conn:
                    conn.execute("DELETE FROM push_subscriptions WHERE endpoint = %s", (sub.get("endpoint"),))


def broadcast_sse(data: dict):
    msg = f"data: {json.dumps(data, ensure_ascii=False)}\n\n"
    with clients_lock:
        dead = []
        for q in clients:
            try:
                q.put_nowait(msg)
            except queue.Full:
                dead.append(q)
        for q in dead:
            clients.remove(q)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/companies", methods=["GET"])
def list_companies():
    with get_db() as conn:
        rows = conn.execute("SELECT id, name, created_at FROM companies ORDER BY name").fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/companies", methods=["POST"])
def add_company():
    data = request.json
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"error": "企業名を入力してください"}), 400

    try:
        with get_db() as conn:
            row = conn.execute("INSERT INTO companies (name) VALUES (%s) RETURNING id", (name,)).fetchone()
            company_id = row["id"]
    except Exception:
        return jsonify({"error": "すでに登録されています"}), 409

    fetch_news_for_company(company_id, name)
    broadcast_sse({"type": "company_added", "name": name})
    return jsonify({"id": company_id, "name": name}), 201


@app.route("/api/companies/<int:company_id>", methods=["DELETE"])
def delete_company(company_id):
    with get_db() as conn:
        conn.execute("DELETE FROM news WHERE company_id = %s", (company_id,))
        conn.execute("DELETE FROM companies WHERE id = %s", (company_id,))
    return jsonify({"ok": True})


@app.route("/api/news")
def list_news():
    company_id = request.args.get("company_id", type=int)
    limit = request.args.get("limit", 50, type=int)

    with get_db() as conn:
        if company_id:
            rows = conn.execute(
                """SELECT n.id, c.name as company_name, n.title, n.url, n.published,
                          n.summary, n.fetched_at, n.is_new
                   FROM news n JOIN companies c ON c.id = n.company_id
                   WHERE n.company_id = %s ORDER BY n.id DESC LIMIT %s""",
                (company_id, limit),
            ).fetchall()
            conn.execute("UPDATE news SET is_new = FALSE WHERE company_id = %s", (company_id,))
        else:
            rows = conn.execute(
                """SELECT n.id, c.name as company_name, n.title, n.url, n.published,
                          n.summary, n.fetched_at, n.is_new
                   FROM news n JOIN companies c ON c.id = n.company_id
                   ORDER BY n.id DESC LIMIT %s""",
                (limit,),
            ).fetchall()
            conn.execute("UPDATE news SET is_new = FALSE")

    return jsonify([dict(r) for r in rows])


@app.route("/api/news/unread-counts")
def unread_counts():
    with get_db() as conn:
        rows = conn.execute(
            "SELECT company_id, COUNT(*) as count FROM news WHERE is_new = TRUE GROUP BY company_id"
        ).fetchall()
    return jsonify({str(r["company_id"]): r["count"] for r in rows})


@app.route("/api/vapid-public-key")
def vapid_public_key():
    return jsonify({"key": VAPID_PUBLIC_KEY})


@app.route("/api/push/subscribe", methods=["POST"])
def push_subscribe():
    sub = request.json
    endpoint = sub.get("endpoint", "")
    with get_db() as conn:
        conn.execute(
            "INSERT INTO push_subscriptions (endpoint, subscription) VALUES (%s, %s) ON CONFLICT (endpoint) DO NOTHING",
            (endpoint, json.dumps(sub))
        )
    return jsonify({"ok": True})


@app.route("/api/push/unsubscribe", methods=["POST"])
def push_unsubscribe():
    endpoint = (request.json or {}).get("endpoint", "")
    with get_db() as conn:
        conn.execute("DELETE FROM push_subscriptions WHERE endpoint = %s", (endpoint,))
    return jsonify({"ok": True})


@app.route("/api/news/refresh", methods=["POST"])
def refresh_news():
    fetch_all_news()
    return jsonify({"ok": True})


@app.route("/api/events")
def sse():
    q: queue.Queue = queue.Queue(maxsize=20)
    with clients_lock:
        clients.append(q)

    def generate():
        yield "data: {\"type\": \"connected\"}\n\n"
        while True:
            try:
                msg = q.get(timeout=30)
                yield msg
            except queue.Empty:
                yield ": ping\n\n"

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


if __name__ == "__main__":
    init_db()
    scheduler = BackgroundScheduler()
    scheduler.add_job(fetch_all_news, "interval", minutes=30, next_run_time=None)
    scheduler.start()
    print("営業ニュースアプリ起動中... http://localhost:5100")
    app.run(debug=False, port=5100, threaded=True)
