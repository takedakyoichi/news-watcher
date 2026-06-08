import os
import feedparser
import json
from flask import Flask, request, jsonify, render_template, Response, redirect
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from werkzeug.middleware.proxy_fix import ProxyFix
from apscheduler.schedulers.background import BackgroundScheduler
import urllib.parse
import queue
import threading
from concurrent.futures import ThreadPoolExecutor
import psycopg
from psycopg.rows import dict_row
from contextlib import contextmanager
from pywebpush import webpush, WebPushException
import bcrypt
import secrets

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)
app.secret_key = os.environ.get("SECRET_KEY", "eigyo-news-secret-2026")
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_HTTPONLY"] = True
DATABASE_URL = os.environ.get("DATABASE_URL")


VAPID_PUBLIC_KEY = os.environ.get("VAPID_PUBLIC_KEY", "")
VAPID_PRIVATE_KEY = os.environ.get("VAPID_PRIVATE_KEY", "")
VAPID_CLAIMS = {"sub": "mailto:admin@example.com"}

login_manager = LoginManager(app)
login_manager.login_view = "login_page"

@login_manager.unauthorized_handler
def unauthorized():
    if request.path.startswith("/api/"):
        return jsonify({"error": "unauthorized"}), 401
    return redirect("/login")

clients: list[queue.Queue] = []
clients_lock = threading.Lock()


class User(UserMixin):
    def __init__(self, id, username):
        self.id = id
        self.username = username


@login_manager.user_loader
def load_user(user_id):
    with get_db() as conn:
        row = conn.execute("SELECT id, username FROM users WHERE id = %s", (int(user_id),)).fetchone()
    if row:
        return User(row["id"], row["username"])
    return None


@contextmanager
def get_db():
    """リクエストごとに新規接続を作成して解放（プールなし）"""
    conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _init_on_startup():
    try:
        with get_db() as conn:
            # usersテーブルがなければ完全初期化（旧データを削除）
            has_users = conn.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = 'users'
                )
            """).fetchone()["exists"]

            if not has_users:
                # 旧テーブルを削除して再構築
                conn.execute("DROP TABLE IF EXISTS news CASCADE")
                conn.execute("DROP TABLE IF EXISTS companies CASCADE")
                conn.execute("DROP TABLE IF EXISTS push_subscriptions CASCADE")

            conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    username TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)

            # companiesにuser_idがなければ再構築
            has_user_id = conn.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'companies' AND column_name = 'user_id'
                )
            """).fetchone()["exists"]

            if not has_user_id:
                conn.execute("DROP TABLE IF EXISTS news CASCADE")
                conn.execute("DROP TABLE IF EXISTS companies CASCADE")
                conn.execute("DROP TABLE IF EXISTS push_subscriptions CASCADE")

            conn.execute("""
                CREATE TABLE IF NOT EXISTS companies (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    name TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(user_id, name)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS news (
                    id SERIAL PRIMARY KEY,
                    company_id INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
                    title TEXT NOT NULL,
                    url TEXT NOT NULL,
                    published TEXT,
                    published_at TIMESTAMPTZ,
                    summary TEXT,
                    fetched_at TIMESTAMP DEFAULT NOW(),
                    is_new BOOLEAN DEFAULT TRUE,
                    UNIQUE(company_id, url)
                )
            """)
            # 既存テーブルにpublished_atカラムがなければ追加
            conn.execute("""
                ALTER TABLE news ADD COLUMN IF NOT EXISTS published_at TIMESTAMPTZ
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS push_subscriptions (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    endpoint TEXT UNIQUE NOT NULL,
                    subscription JSONB NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            # インデックス追加（検索・ソートを高速化）
            conn.execute("CREATE INDEX IF NOT EXISTS idx_news_company_id ON news(company_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_news_published_at ON news(published_at DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_news_fetched_at ON news(fetched_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_companies_user_id ON companies(user_id)")
    except Exception as e:
        print(f"DB init error: {e}")
        raise  # リトライのため再送出


def _init_with_retry():
    import time
    for attempt in range(5):
        try:
            _init_on_startup()
            print("DB init OK")
            return
        except Exception as e:
            print(f"DB init attempt {attempt+1} failed: {e}")
            time.sleep(3)
    print("DB init failed after 5 attempts")

threading.Thread(target=_init_with_retry, daemon=True).start()


def fetch_news_for_company(company_id: int, company_name: str, today_only: bool = False) -> int:
    from datetime import date, datetime, timezone, timedelta
    import email.utils

    today = date.today()
    one_month_ago = datetime.now(timezone.utc) - timedelta(days=31)

    def parse_dt(published_str):
        if not published_str:
            return None
        try:
            return email.utils.parsedate_to_datetime(published_str).astimezone(timezone.utc)
        except Exception:
            return None

    query = urllib.parse.quote(company_name)
    url = f"https://news.google.com/rss/search?q={query}&hl=ja&gl=JP&ceid=JP:ja"
    try:
        feed = feedparser.parse(url)
    except Exception:
        return 0

    # フィルタリングしてバッチINSERT用データを作成
    rows = []
    for entry in feed.entries[:50]:
        published = entry.get("published", "")
        dt = parse_dt(published)
        if today_only:
            if dt is None or dt.date() != today:
                continue
        else:
            if dt is not None and dt < one_month_ago:
                continue
        rows.append((
            company_id,
            entry.get("title", ""),
            entry.get("link", ""),
            published,
            dt,
            entry.get("summary", "")[:300],
        ))

    if not rows:
        return 0

    # バッチINSERT（ON CONFLICT DO NOTHING で重複スキップ）
    new_count = 0
    with get_db() as conn:
        for row in rows:
            try:
                cur = conn.execute(
                    "INSERT INTO news (company_id, title, url, published, published_at, summary)"
                    " VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (company_id, url) DO NOTHING",
                    row,
                )
                new_count += cur.rowcount  # 1=新規挿入, 0=重複スキップ
            except Exception:
                conn.rollback()

    return new_count


def fetch_all_news():
    with get_db() as conn:
        companies = conn.execute("SELECT id, name, user_id FROM companies").fetchall()

    # 並列でニュース取得（最大8スレッド）
    def fetch_one(company):
        count = fetch_news_for_company(company["id"], company["name"], today_only=True)
        return company, count

    user_new_items: dict[int, list] = {}
    with ThreadPoolExecutor(max_workers=3) as executor:
        for company, count in executor.map(fetch_one, companies):
            if count > 0:
                uid = company["user_id"]
                user_new_items.setdefault(uid, []).append({"company": company["name"], "count": count})

    for uid, items in user_new_items.items():
        broadcast_sse({"type": "new_news", "items": items, "total": sum(i["count"] for i in items)}, uid)
        body = "、".join([f'{i["company"]}({i["count"]}件)' for i in items])
        send_push_notifications("📰 新着ニュース", body, uid)


def broadcast_sse(data: dict, user_id: int = None):
    msg = f"data: {json.dumps(data, ensure_ascii=False)}\n\n"
    with clients_lock:
        dead = []
        for uid, q in clients:
            if user_id is None or uid == user_id:
                try:
                    q.put_nowait(msg)
                except queue.Full:
                    dead.append((uid, q))
        for item in dead:
            clients.remove(item)


def send_push_notifications(title: str, body: str, user_id: int):
    if not VAPID_PRIVATE_KEY:
        return
    with get_db() as conn:
        subs = conn.execute("SELECT subscription FROM push_subscriptions WHERE user_id = %s", (user_id,)).fetchall()
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


# ─── 認証ページ ───

@app.route("/login", methods=["GET"])
def login_page():
    if current_user.is_authenticated:
        return redirect("/")
    return render_template("auth.html", mode="login")


@app.route("/register", methods=["GET"])
def register_page():
    if current_user.is_authenticated:
        return redirect("/")
    return render_template("auth.html", mode="register")


@app.route("/api/auth/register", methods=["POST"])
def api_register():
    data = request.json
    username = (data.get("username") or "").strip()
    password = (data.get("password") or "").strip()
    if not username or not password:
        return jsonify({"error": "ユーザー名とパスワードを入力してください"}), 400
    if len(password) < 6:
        return jsonify({"error": "パスワードは6文字以上にしてください"}), 400

    pw_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    try:
        with get_db() as conn:
            row = conn.execute(
                "INSERT INTO users (username, password_hash) VALUES (%s, %s) RETURNING id",
                (username, pw_hash)
            ).fetchone()
            user_id = row["id"]
    except Exception:
        return jsonify({"error": "このユーザー名は既に使われています"}), 409

    user = User(user_id, username)
    login_user(user, remember=True)
    return jsonify({"ok": True})


@app.route("/api/auth/login", methods=["POST"])
def api_login():
    data = request.json
    username = (data.get("username") or "").strip()
    password = (data.get("password") or "").strip()

    with get_db() as conn:
        row = conn.execute("SELECT id, username, password_hash FROM users WHERE username = %s", (username,)).fetchone()

    if not row or not bcrypt.checkpw(password.encode(), row["password_hash"].encode()):
        return jsonify({"error": "ユーザー名またはパスワードが違います"}), 401

    user = User(row["id"], row["username"])
    login_user(user, remember=True)
    return jsonify({"ok": True})


@app.route("/api/auth/logout", methods=["POST"])
@login_required
def api_logout():
    logout_user()
    return jsonify({"ok": True})


# ─── メインアプリ ───

@app.route("/")
@login_required
def index():
    return render_template("index.html", username=current_user.username)


@app.route("/api/companies", methods=["GET"])
@login_required
def list_companies():
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, name, created_at FROM companies WHERE user_id = %s ORDER BY name",
            (current_user.id,)
        ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/companies", methods=["POST"])
@login_required
def add_company():
    data = request.json
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"error": "企業名を入力してください"}), 400

    try:
        with get_db() as conn:
            row = conn.execute(
                "INSERT INTO companies (user_id, name) VALUES (%s, %s) RETURNING id",
                (current_user.id, name)
            ).fetchone()
            company_id = row["id"]
    except Exception:
        return jsonify({"error": "すでに登録されています"}), 409

    uid = current_user.id
    # ニュース取得はバックグラウンドで実行（レスポンスをブロックしない）
    def _fetch_and_notify():
        fetch_news_for_company(company_id, name)
        broadcast_sse({"type": "company_added", "name": name}, uid)
    threading.Thread(target=_fetch_and_notify, daemon=True).start()
    return jsonify({"id": company_id, "name": name}), 201


@app.route("/api/companies/<int:company_id>", methods=["DELETE"])
@login_required
def delete_company(company_id):
    with get_db() as conn:
        conn.execute("DELETE FROM companies WHERE id = %s AND user_id = %s", (company_id, current_user.id))
    return jsonify({"ok": True})


@app.route("/api/news")
@login_required
def list_news():
    # company_ids=1,2,3 形式で複数指定可、未指定はすべて
    ids_param = request.args.get("company_ids", "")
    company_ids = [int(x) for x in ids_param.split(",") if x.strip().isdigit()]
    limit = request.args.get("limit", 50, type=int)

    with get_db() as conn:
        if company_ids:
            rows = conn.execute(
                """SELECT n.id, c.name as company_name, n.title, n.url, n.published,
                          n.summary, n.fetched_at, n.is_new
                   FROM news n JOIN companies c ON c.id = n.company_id
                   WHERE n.company_id = ANY(%s) AND c.user_id = %s
                     AND n.fetched_at >= NOW() - INTERVAL '1 month'
                   ORDER BY n.published_at DESC NULLS LAST, n.id DESC LIMIT %s""",
                (company_ids, current_user.id, limit),
            ).fetchall()
            conn.execute(
                "UPDATE news SET is_new = FALSE WHERE company_id = ANY(%s)",
                (company_ids,)
            )
        else:
            rows = conn.execute(
                """SELECT n.id, c.name as company_name, n.title, n.url, n.published,
                          n.summary, n.fetched_at, n.is_new
                   FROM news n JOIN companies c ON c.id = n.company_id
                   WHERE c.user_id = %s
                     AND n.fetched_at >= NOW() - INTERVAL '1 month'
                   ORDER BY n.published_at DESC NULLS LAST, n.id DESC LIMIT %s""",
                (current_user.id, limit),
            ).fetchall()
            conn.execute(
                "UPDATE news SET is_new = FALSE WHERE company_id IN (SELECT id FROM companies WHERE user_id = %s)",
                (current_user.id,)
            )

    return jsonify([dict(r) for r in rows])


@app.route("/api/news/unread-counts")
@login_required
def unread_counts():
    with get_db() as conn:
        rows = conn.execute(
            """SELECT n.company_id, COUNT(*) as count FROM news n
               JOIN companies c ON c.id = n.company_id
               WHERE n.is_new = TRUE AND c.user_id = %s
               GROUP BY n.company_id""",
            (current_user.id,)
        ).fetchall()
    return jsonify({str(r["company_id"]): r["count"] for r in rows})


@app.route("/api/news/refresh", methods=["POST"])
@login_required
def refresh_news():
    with get_db() as conn:
        companies = conn.execute(
            "SELECT id, name FROM companies WHERE user_id = %s", (current_user.id,)
        ).fetchall()
    # 並列取得で高速化
    with ThreadPoolExecutor(max_workers=3) as executor:
        list(executor.map(lambda c: fetch_news_for_company(c["id"], c["name"]), companies))
    return jsonify({"ok": True})


@app.route("/api/vapid-public-key")
def vapid_public_key():
    return jsonify({"key": VAPID_PUBLIC_KEY})


@app.route("/api/push/subscribe", methods=["POST"])
@login_required
def push_subscribe():
    sub = request.json
    endpoint = sub.get("endpoint", "")
    with get_db() as conn:
        conn.execute(
            "INSERT INTO push_subscriptions (user_id, endpoint, subscription) VALUES (%s, %s, %s) ON CONFLICT (endpoint) DO NOTHING",
            (current_user.id, endpoint, json.dumps(sub))
        )
    return jsonify({"ok": True})


@app.route("/api/push/unsubscribe", methods=["POST"])
@login_required
def push_unsubscribe():
    endpoint = (request.json or {}).get("endpoint", "")
    with get_db() as conn:
        conn.execute("DELETE FROM push_subscriptions WHERE endpoint = %s AND user_id = %s", (endpoint, current_user.id))
    return jsonify({"ok": True})


@app.route("/api/events")
@login_required
def sse():
    uid = current_user.id
    q: queue.Queue = queue.Queue(maxsize=20)
    with clients_lock:
        clients.append((uid, q))

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


# gunicornでも動くようモジュールレベルでスケジューラ起動（全関数定義後）
_scheduler = BackgroundScheduler()
_scheduler.add_job(fetch_all_news, "interval", minutes=15)
_scheduler.start()

if __name__ == "__main__":
    print("営業ニュースアプリ起動中... http://localhost:5100")
    app.run(debug=False, port=5100, threaded=True)
