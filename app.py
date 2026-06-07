import os
import sqlite3
import feedparser
import json
from flask import Flask, request, jsonify, render_template, Response
from apscheduler.schedulers.background import BackgroundScheduler
import urllib.parse
import queue
import threading

app = Flask(__name__)
DB_PATH = os.environ.get("DB_PATH", "news.db")

# SSE用のクライアントキュー管理
clients: list[queue.Queue] = []
clients_lock = threading.Lock()


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS companies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                created_at TEXT DEFAULT (datetime('now', 'localtime'))
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS news (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                url TEXT NOT NULL UNIQUE,
                published TEXT,
                summary TEXT,
                fetched_at TEXT DEFAULT (datetime('now', 'localtime')),
                is_new INTEGER DEFAULT 1,
                FOREIGN KEY (company_id) REFERENCES companies(id)
            )
        """)


def fetch_news_for_company(company_id: int, company_name: str) -> int:
    """Google News RSSから企業名でニュースを取得し、新着件数を返す"""
    query = urllib.parse.quote(company_name)
    url = f"https://news.google.com/rss/search?q={query}&hl=ja&gl=JP&ceid=JP:ja"

    try:
        feed = feedparser.parse(url)
    except Exception:
        return 0

    new_count = 0
    with get_db() as conn:
        for entry in feed.entries[:20]:
            title = entry.get("title", "")
            link = entry.get("link", "")
            summary = entry.get("summary", "")[:300]
            published = entry.get("published", "")

            try:
                conn.execute(
                    "INSERT INTO news (company_id, title, url, published, summary) VALUES (?, ?, ?, ?, ?)",
                    (company_id, title, link, published, summary),
                )
                new_count += 1
            except sqlite3.IntegrityError:
                pass  # 重複はスキップ

    return new_count


def fetch_all_news():
    """スケジューラから全企業のニュースを取得"""
    with get_db() as conn:
        companies = conn.execute("SELECT id, name FROM companies").fetchall()

    total_new = 0
    new_items = []
    for company in companies:
        count = fetch_news_for_company(company["id"], company["name"])
        if count > 0:
            total_new += count
            new_items.append({"company": company["name"], "count": count})

    if new_items:
        broadcast_sse({"type": "new_news", "items": new_items, "total": total_new})


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


# --- Routes ---

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

    with get_db() as conn:
        try:
            cur = conn.execute("INSERT INTO companies (name) VALUES (?)", (name,))
            company_id = cur.lastrowid
        except sqlite3.IntegrityError:
            return jsonify({"error": "すでに登録されています"}), 409

    # 登録直後にニュース取得
    fetch_news_for_company(company_id, name)
    broadcast_sse({"type": "company_added", "name": name})
    return jsonify({"id": company_id, "name": name}), 201


@app.route("/api/companies/<int:company_id>", methods=["DELETE"])
def delete_company(company_id):
    with get_db() as conn:
        conn.execute("DELETE FROM news WHERE company_id = ?", (company_id,))
        conn.execute("DELETE FROM companies WHERE id = ?", (company_id,))
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
                   WHERE n.company_id = ?
                   ORDER BY n.id DESC LIMIT ?""",
                (company_id, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT n.id, c.name as company_name, n.title, n.url, n.published,
                          n.summary, n.fetched_at, n.is_new
                   FROM news n JOIN companies c ON c.id = n.company_id
                   ORDER BY n.id DESC LIMIT ?""",
                (limit,),
            ).fetchall()

        # 既読にする
        if company_id:
            conn.execute("UPDATE news SET is_new = 0 WHERE company_id = ?", (company_id,))
        else:
            conn.execute("UPDATE news SET is_new = 0")

    return jsonify([dict(r) for r in rows])


@app.route("/api/news/unread-counts")
def unread_counts():
    with get_db() as conn:
        rows = conn.execute(
            """SELECT company_id, COUNT(*) as count
               FROM news WHERE is_new = 1
               GROUP BY company_id"""
        ).fetchall()
    return jsonify({str(r["company_id"]): r["count"] for r in rows})


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
