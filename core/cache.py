"""
core.cache — email deduplication cache backed by SQLite.

Hashes sender + normalised subject to avoid classifying the same
thread repeatedly.  Cache entries expire after cache_ttl_days (default 30).
"""

import hashlib
import re
from datetime import datetime, timedelta

from .db import get_db, get_setting


def email_hash(from_addr: str, subject: str) -> str:
    """Stable SHA-256 hash for deduplication: sender + normalised subject."""
    norm = re.sub(r'\b(re|fwd?|fw):\s*', '', subject.lower().strip())
    norm = re.sub(r'\s+', ' ', norm)
    key  = f"{from_addr.lower().strip()}|||{norm}"
    return hashlib.sha256(key.encode()).hexdigest()


def check_cache(emails: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Split emails into (cached_classifications, uncached_emails).

    Cached items already have action/folder/reason attached.
    """
    db      = get_db()
    ttl_days = int(get_setting('cache_ttl_days', 30))
    cutoff  = (datetime.utcnow() - timedelta(days=ttl_days)).isoformat()

    cached   = []
    uncached = []
    for e in emails:
        h   = email_hash(e['from'], e['subject'])
        row = db.execute(
            'SELECT action, folder, reason FROM email_cache '
            'WHERE hash = ? AND classified_at > ?',
            (h, cutoff),
        ).fetchone()
        if row:
            cached.append({
                **e,
                'action':     row['action'],
                'folder':     row['folder'] or '',
                'reason':     row['reason'],
                'from_cache': True,
            })
        else:
            uncached.append(e)

    db.close()
    return cached, uncached


def store_cache(classifications: list[dict], emails: list[dict]) -> None:
    """Persist newly computed classifications to the cache."""
    db        = get_db()
    now       = datetime.utcnow().isoformat()
    email_map = {e['uid']: e for e in emails}

    for c in classifications:
        e = email_map.get(c.get('uid', ''), {})
        if not e:
            continue
        h = email_hash(e.get('from', ''), e.get('subject', ''))
        db.execute(
            'INSERT OR REPLACE INTO email_cache '
            '(hash, action, folder, reason, classified_at) VALUES (?,?,?,?,?)',
            (h, c.get('action', 'keep'), c.get('folder', ''),
             c.get('reason', ''), now),
        )

    db.commit()
    db.close()


def get_cache_stats() -> dict:
    db       = get_db()
    ttl_days = int(get_setting('cache_ttl_days', 30))
    cutoff   = (datetime.utcnow() - timedelta(days=ttl_days)).isoformat()
    row      = db.execute(
        'SELECT COUNT(*) as c FROM email_cache WHERE classified_at > ?', (cutoff,)
    ).fetchone()
    db.close()
    return {'active_entries': row['c'] if row else 0}


def clear_cache() -> None:
    db = get_db()
    db.execute('DELETE FROM email_cache')
    db.commit()
    db.close()
