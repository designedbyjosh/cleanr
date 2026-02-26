"""
core.db — database initialisation, migrations, credentials and settings.
"""

import os
import sqlite3
from datetime import datetime

DB_PATH = os.environ.get('DB_PATH', '/data/inbox_cleaner.db')


def get_db() -> sqlite3.Connection:
    os.makedirs('/data', exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    db = get_db()
    db.executescript('''
        CREATE TABLE IF NOT EXISTS credentials (
            key        TEXT PRIMARY KEY,
            value      TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS settings (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS runs (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at    TEXT NOT NULL,
            finished_at   TEXT,
            status        TEXT NOT NULL DEFAULT 'running',
            run_type      TEXT NOT NULL DEFAULT 'manual',
            source_folder TEXT DEFAULT 'INBOX',
            total         INTEGER DEFAULT 0,
            kept          INTEGER DEFAULT 0,
            filed         INTEGER DEFAULT 0,
            trashed       INTEGER DEFAULT 0,
            errors        INTEGER DEFAULT 0,
            skipped       INTEGER DEFAULT 0,
            job_id        INTEGER
        );
        CREATE TABLE IF NOT EXISTS actions (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id       INTEGER NOT NULL,
            uid          TEXT NOT NULL,
            from_addr    TEXT,
            subject      TEXT,
            action       TEXT NOT NULL,
            folder       TEXT,
            reason       TEXT,
            error_detail TEXT,
            created_at   TEXT NOT NULL,
            FOREIGN KEY (run_id) REFERENCES runs(id)
        );
        CREATE TABLE IF NOT EXISTS email_cache (
            hash          TEXT PRIMARY KEY,
            action        TEXT NOT NULL,
            folder        TEXT,
            reason        TEXT,
            classified_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS schedules (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            name                    TEXT NOT NULL,
            enabled                 INTEGER NOT NULL DEFAULT 1,
            interval_hours          INTEGER NOT NULL DEFAULT 24,
            limit_per_run           INTEGER NOT NULL DEFAULT 50,
            folder                  TEXT NOT NULL DEFAULT 'INBOX',
            custom_prompt           TEXT DEFAULT '',
            delete_marketing_unread INTEGER NOT NULL DEFAULT 0,
            skip_flagged            INTEGER NOT NULL DEFAULT 1,
            next_run                TEXT,
            last_run                TEXT,
            created_at              TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS folder_jobs (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            name                    TEXT NOT NULL,
            folder                  TEXT NOT NULL,
            enabled                 INTEGER NOT NULL DEFAULT 1,
            status                  TEXT NOT NULL DEFAULT 'idle',
            batch_size              INTEGER NOT NULL DEFAULT 20,
            rate_limit_per_hour     INTEGER NOT NULL DEFAULT 60,
            oldest_first            INTEGER NOT NULL DEFAULT 1,
            start_from_days_ago     INTEGER,
            max_emails              INTEGER,
            custom_prompt           TEXT DEFAULT '',
            delete_marketing_unread INTEGER NOT NULL DEFAULT 0,
            skip_flagged            INTEGER NOT NULL DEFAULT 1,
            aggressive_trash        INTEGER NOT NULL DEFAULT 0,
            total_processed         INTEGER DEFAULT 0,
            total_remaining         INTEGER DEFAULT -1,
            last_run                TEXT,
            created_at              TEXT NOT NULL,
            completed_at            TEXT,
            session_id              TEXT
        );
        CREATE TABLE IF NOT EXISTS job_events (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id     INTEGER,
            run_id     INTEGER,
            session_id TEXT NOT NULL,
            event      TEXT NOT NULL,
            data       TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS worker_containers (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id         INTEGER NOT NULL,
            run_id         INTEGER,
            container_id   TEXT,
            container_name TEXT,
            status         TEXT DEFAULT 'starting',
            created_at     TEXT NOT NULL,
            finished_at    TEXT
        );
    ''')

    # Safe migrations — ignore errors if column already exists
    migrations = [
        "ALTER TABLE runs ADD COLUMN run_type TEXT NOT NULL DEFAULT 'manual'",
        "ALTER TABLE runs ADD COLUMN source_folder TEXT DEFAULT 'INBOX'",
        "ALTER TABLE runs ADD COLUMN skipped INTEGER DEFAULT 0",
        "ALTER TABLE runs ADD COLUMN job_id INTEGER",
        "ALTER TABLE folder_jobs ADD COLUMN session_id TEXT",
        "ALTER TABLE folder_jobs ADD COLUMN total_remaining INTEGER DEFAULT -1",
        "ALTER TABLE folder_jobs ADD COLUMN completed_at TEXT",
        "ALTER TABLE folder_jobs ADD COLUMN start_from_days_ago INTEGER",
        "ALTER TABLE folder_jobs ADD COLUMN max_emails INTEGER",
        "ALTER TABLE folder_jobs ADD COLUMN custom_prompt TEXT DEFAULT ''",
        "ALTER TABLE folder_jobs ADD COLUMN delete_marketing_unread INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE folder_jobs ADD COLUMN skip_flagged INTEGER NOT NULL DEFAULT 1",
        "ALTER TABLE folder_jobs ADD COLUMN aggressive_trash INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE schedules ADD COLUMN folder TEXT NOT NULL DEFAULT 'INBOX'",
        "ALTER TABLE schedules ADD COLUMN custom_prompt TEXT DEFAULT ''",
        "ALTER TABLE schedules ADD COLUMN delete_marketing_unread INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE schedules ADD COLUMN skip_flagged INTEGER NOT NULL DEFAULT 1",
        # Tables added in previous iterations — safe no-ops if they already exist
        "CREATE TABLE IF NOT EXISTS job_events (id INTEGER PRIMARY KEY AUTOINCREMENT, job_id INTEGER, run_id INTEGER, session_id TEXT NOT NULL, event TEXT NOT NULL, data TEXT NOT NULL, created_at TEXT NOT NULL)",
        "CREATE TABLE IF NOT EXISTS worker_containers (id INTEGER PRIMARY KEY AUTOINCREMENT, job_id INTEGER NOT NULL, run_id INTEGER, container_id TEXT, container_name TEXT, status TEXT DEFAULT 'starting', created_at TEXT NOT NULL, finished_at TEXT)",
    ]
    for sql in migrations:
        try:
            db.execute(sql)
        except Exception:
            pass

    defaults = {
        'rate_limit_per_hour': '200',
        'batch_delay_seconds': '5',
        'inbox_zero_mode':     '1',
        'default_limit':       '50',
        'parallel_batches':    '3',
        'cache_ttl_days':      '30',
    }
    for k, v in defaults.items():
        db.execute('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)', (k, v))

    db.commit()
    db.close()


# ── Credentials ───────────────────────────────────────────────────────────────

def save_credential(key: str, value: str) -> None:
    db = get_db()
    db.execute(
        'INSERT OR REPLACE INTO credentials (key, value, updated_at) VALUES (?, ?, ?)',
        (key, value, datetime.utcnow().isoformat()),
    )
    db.commit()
    db.close()


def get_credential(key: str) -> str | None:
    db = get_db()
    row = db.execute('SELECT value FROM credentials WHERE key = ?', (key,)).fetchone()
    db.close()
    return row['value'] if row else None


def get_credential_meta(key: str) -> dict:
    db = get_db()
    row = db.execute('SELECT updated_at FROM credentials WHERE key = ?', (key,)).fetchone()
    db.close()
    return {'set': True, 'updated_at': row['updated_at']} if row else {'set': False}


# ── Settings ──────────────────────────────────────────────────────────────────

def get_setting(key: str, default=None):
    db = get_db()
    row = db.execute('SELECT value FROM settings WHERE key = ?', (key,)).fetchone()
    db.close()
    return row['value'] if row else default


def save_setting(key: str, value) -> None:
    db = get_db()
    db.execute('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)', (key, str(value)))
    db.commit()
    db.close()
