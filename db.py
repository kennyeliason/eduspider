import sqlite3
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "eduspider.db")


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS crawls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            seed_url TEXT NOT NULL,
            max_depth INTEGER NOT NULL,
            pages_found INTEGER DEFAULT 0,
            started_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'running'
        );

        CREATE TABLE IF NOT EXISTS pages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT NOT NULL UNIQUE,
            title TEXT,
            description TEXT,
            domain TEXT,
            crawl_depth INTEGER,
            crawl_id INTEGER REFERENCES crawls(id),
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS topics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS page_topics (
            page_id INTEGER REFERENCES pages(id),
            topic_id INTEGER REFERENCES topics(id),
            PRIMARY KEY (page_id, topic_id)
        );

        CREATE INDEX IF NOT EXISTS idx_pages_url ON pages(url);
        CREATE INDEX IF NOT EXISTS idx_pages_domain ON pages(domain);
        CREATE INDEX IF NOT EXISTS idx_topics_name ON topics(name);
    """)
    conn.commit()
    conn.close()


def create_crawl(seed_url, max_depth):
    conn = get_conn()
    cur = conn.execute(
        "INSERT INTO crawls (seed_url, max_depth, started_at, status) VALUES (?, ?, ?, ?)",
        (seed_url, max_depth, datetime.now().isoformat(), "running"),
    )
    crawl_id = cur.lastrowid
    conn.commit()
    conn.close()
    return crawl_id


def finish_crawl(crawl_id, pages_found, status="done"):
    conn = get_conn()
    conn.execute(
        "UPDATE crawls SET pages_found = ?, status = ? WHERE id = ?",
        (pages_found, status, crawl_id),
    )
    conn.commit()
    conn.close()


def save_page(url, title, description, domain, crawl_depth, crawl_id):
    conn = get_conn()
    try:
        cur = conn.execute(
            """INSERT INTO pages (url, title, description, domain, crawl_depth, crawl_id, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (url, title, description, domain, crawl_depth, crawl_id, datetime.now().isoformat()),
        )
        page_id = cur.lastrowid
        conn.commit()
        conn.close()
        return page_id
    except sqlite3.IntegrityError:
        conn.close()
        return None


def page_exists(url):
    conn = get_conn()
    row = conn.execute("SELECT id FROM pages WHERE url = ?", (url,)).fetchone()
    conn.close()
    return row is not None


def get_or_create_topic(name):
    conn = get_conn()
    row = conn.execute("SELECT id FROM topics WHERE name = ?", (name,)).fetchone()
    if row:
        topic_id = row["id"]
    else:
        cur = conn.execute(
            "INSERT INTO topics (name, created_at) VALUES (?, ?)",
            (name, datetime.now().isoformat()),
        )
        topic_id = cur.lastrowid
    conn.commit()
    conn.close()
    return topic_id


def link_page_topic(page_id, topic_id):
    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO page_topics (page_id, topic_id) VALUES (?, ?)",
            (page_id, topic_id),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        pass
    conn.close()


def get_topics():
    conn = get_conn()
    rows = conn.execute("""
        SELECT t.name, COUNT(pt.page_id) AS page_count
        FROM topics t
        JOIN page_topics pt ON t.id = pt.topic_id
        GROUP BY t.id
        ORDER BY page_count DESC
    """).fetchall()
    conn.close()
    return rows


def get_pages_for_topic(topic_name):
    conn = get_conn()
    rows = conn.execute("""
        SELECT p.url, p.title, p.description, p.domain
        FROM pages p
        JOIN page_topics pt ON p.id = pt.page_id
        JOIN topics t ON t.id = pt.topic_id
        WHERE t.name = ?
        ORDER BY p.created_at DESC
    """, (topic_name,)).fetchall()
    conn.close()
    return rows


def get_crawls():
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM crawls ORDER BY started_at DESC"
    ).fetchall()
    conn.close()
    return rows
