#!/usr/bin/env python3
"""
Inbox Cleaner — generic batch worker.

Driven entirely by the MANIFEST environment variable (JSON-encoded JobManifest).
Launched as an ephemeral sibling Docker container by the Flask orchestrator or
the scheduler.  Never reads individual JOB_ID / FOLDER env vars — all
parameters come from the manifest.

Supported job_type values:
  folder_cleanup    — process a batch from an arbitrary IMAP folder
  inbox_cleanup     — process read (and optionally unread) INBOX emails
  scheduled_cleanup — same as inbox_cleanup but triggered by the scheduler
"""

import json
import logging
import os
import sys
import traceback
from datetime import datetime

# ── logging must be configured before any core import so that our format wins ──
logging.basicConfig(
    level   = logging.INFO,
    format  = '[%(asctime)s] %(levelname)-8s worker  %(message)s',
    datefmt = '%Y-%m-%d %H:%M:%S',
    force   = True,
)
log = logging.getLogger('worker')

sys.path.insert(0, '/app')

# Allow manifest.db_path to override DB_PATH before core.db is imported
_raw_manifest = os.environ.get('MANIFEST', '{}')
try:
    _db_path = json.loads(_raw_manifest).get('db_path')
    if _db_path:
        os.environ.setdefault('DB_PATH', _db_path)
except Exception:
    pass

from core.manifest import JobManifest                      # noqa: E402
from core.db import get_db, get_credential, get_setting    # noqa: E402
from core.imap import get_imap_conn, fetch_emails_from_folder, fetch_inbox_emails  # noqa: E402
from core.classifier import classify_emails_parallel       # noqa: E402
from core.apply import apply_classifications               # noqa: E402


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_helpers(db, manifest: JobManifest):
    """Return (emit, log_action, update_run) callables bound to this run."""

    def emit(event: str, data: dict) -> None:
        db.execute(
            'INSERT INTO job_events '
            '(job_id, run_id, session_id, event, data, created_at) '
            'VALUES (?,?,?,?,?,?)',
            (manifest.job_id, manifest.run_id, manifest.session_id,
             event, json.dumps(data), datetime.utcnow().isoformat()),
        )
        db.commit()

    def log_action(uid, from_addr, subject, action, folder, reason) -> None:
        db.execute(
            'INSERT INTO actions '
            '(run_id, uid, from_addr, subject, action, folder, reason, created_at) '
            'VALUES (?,?,?,?,?,?,?,?)',
            (manifest.run_id, uid, from_addr, subject,
             action, folder, reason, datetime.utcnow().isoformat()),
        )
        db.commit()

    def update_run(**kwargs) -> None:
        sets = ', '.join(f'{k}=?' for k in kwargs)
        db.execute(f'UPDATE runs SET {sets} WHERE id=?', (*kwargs.values(), manifest.run_id))
        db.commit()

    return emit, log_action, update_run


def _load_credentials():
    """Read IMAP + API credentials from the shared DB."""
    email_addr   = get_credential('email')
    app_password = get_credential('app_password')
    api_key      = get_credential('api_key')
    if not all([email_addr, app_password, api_key]):
        raise RuntimeError(
            'One or more credentials are missing. '
            'Set email, app_password, and api_key in Settings.'
        )
    log.info(f'Credentials loaded  email={email_addr!r}')
    return email_addr, app_password, api_key


def _finalise_run(db, manifest: JobManifest, kept, filed, trashed, errors, skipped) -> None:
    db.execute(
        'UPDATE runs SET status="done", finished_at=?, '
        'kept=?, filed=?, trashed=?, errors=?, skipped=? WHERE id=?',
        (datetime.utcnow().isoformat(),
         kept, filed, trashed, errors, skipped, manifest.run_id),
    )
    db.commit()


# ─────────────────────────────────────────────────────────────────────────────
# Job runners
# ─────────────────────────────────────────────────────────────────────────────

def run_folder_batch(manifest: JobManifest, db, emit, log_action, update_run) -> None:
    """Process one batch for a folder_cleanup job."""
    email_addr, app_password, api_key = _load_credentials()

    log.info('Connecting to IMAP…')
    emit('status', {'msg': 'Connecting to IMAP…', 'stage': 'connect'})
    conn = get_imap_conn(email_addr, app_password)
    log.info('IMAP connected')

    log.info(
        f'Fetching from {manifest.folder!r}  '
        f'limit={manifest.batch_size}  oldest_first={manifest.oldest_first}  '
        f'since_days_ago={manifest.start_from_days_ago}  skip_flagged={manifest.skip_flagged}'
    )
    emit('pipeline', {'stage': 'fetch', 'status': 'running'})
    emails, total_in_folder = fetch_emails_from_folder(
        conn,
        folder         = manifest.folder,
        limit          = manifest.batch_size,
        oldest_first   = manifest.oldest_first,
        since_days_ago = manifest.start_from_days_ago,
        skip_flagged   = manifest.skip_flagged,
    )
    log.info(f'Fetched {len(emails)} email(s)  total_in_folder={total_in_folder}')
    emit('pipeline', {
        'stage': 'fetch', 'status': 'done',
        'count': len(emails), 'total': total_in_folder,
    })

    # Update folder-level remaining count so the UI can track progress
    if manifest.job_id:
        db.execute(
            'UPDATE folder_jobs SET total_remaining=? WHERE id=?',
            (total_in_folder, manifest.job_id),
        )
        db.commit()

    # Empty folder → mark done and exit
    if not emails:
        log.info('Folder is empty — signalling completion')
        db.execute(
            'UPDATE runs SET status="done", finished_at=?, total=0 WHERE id=?',
            (datetime.utcnow().isoformat(), manifest.run_id),
        )
        db.commit()
        emit('done', {'empty': True, 'total_in_folder': 0})
        conn.logout()
        return

    update_run(total=len(emails))

    log.info(f'Classifying {len(emails)} email(s)…')
    classifications = classify_emails_parallel(
        api_key, emails, manifest.folder, emit, manifest=manifest,
    )
    log.info(f'Classification complete  results={len(classifications)}')

    log.info('Applying classifications…')
    kept, filed, trashed, errors, skipped = apply_classifications(
        conn, classifications, emails, manifest.folder,
        emit, log_action, update_run, manifest=manifest,
    )
    log.info(
        f'Apply complete  kept={kept}  filed={filed}  '
        f'trashed={trashed}  errors={errors}  skipped={skipped}'
    )

    conn.logout()
    log.info('IMAP logged out')

    _finalise_run(db, manifest, kept, filed, trashed, errors, skipped)

    if manifest.job_id:
        db.execute(
            'UPDATE folder_jobs SET total_processed=total_processed+?, last_run=? WHERE id=?',
            (kept + filed + trashed, datetime.utcnow().isoformat(), manifest.job_id),
        )
        db.commit()

    remaining = max(0, total_in_folder - len(emails))
    emit('done', {
        'kept': kept, 'filed': filed, 'trashed': trashed,
        'errors': errors, 'skipped': skipped,
        'remaining': remaining,
    })
    log.info(f'Worker finished successfully  remaining={remaining}')


def run_inbox_batch(manifest: JobManifest, db, emit, log_action, update_run) -> None:
    """Process inbox or scheduled cleanup."""
    email_addr, app_password, api_key = _load_credentials()

    log.info('Connecting to IMAP…')
    emit('status', {'msg': 'Connecting to iCloud Mail…', 'stage': 'connect'})
    conn = get_imap_conn(email_addr, app_password)
    log.info('IMAP connected')

    log.info(
        f'Fetching inbox  folder={manifest.folder!r}  limit={manifest.batch_size}  '
        f'include_unread={manifest.delete_marketing_unread}  '
        f'since_days_ago={manifest.start_from_days_ago}  skip_flagged={manifest.skip_flagged}'
    )
    emit('pipeline', {'stage': 'fetch', 'status': 'running'})
    emails = fetch_inbox_emails(
        conn,
        limit          = manifest.batch_size,
        folder         = manifest.folder,
        oldest_first   = manifest.oldest_first,
        include_unread = manifest.delete_marketing_unread,
        since_days_ago = manifest.start_from_days_ago,
        skip_flagged   = manifest.skip_flagged,
    )
    log.info(f'Fetched {len(emails)} email(s)')
    emit('pipeline', {'stage': 'fetch', 'status': 'done', 'count': len(emails)})

    if not emails:
        log.info('No emails to process')
        db.execute(
            'UPDATE runs SET status="done", finished_at=?, total=0 WHERE id=?',
            (datetime.utcnow().isoformat(), manifest.run_id),
        )
        db.commit()
        emit('done', {'total': 0, 'kept': 0, 'filed': 0,
                      'trashed': 0, 'errors': 0, 'skipped': 0})
        conn.logout()
        return

    update_run(total=len(emails))

    log.info(f'Classifying {len(emails)} email(s)…')
    classifications = classify_emails_parallel(
        api_key, emails, manifest.folder, emit, manifest=manifest,
    )
    log.info(f'Classification complete  results={len(classifications)}')

    log.info('Applying classifications…')
    kept, filed, trashed, errors, skipped = apply_classifications(
        conn, classifications, emails, manifest.folder,
        emit, log_action, update_run, manifest=manifest,
    )
    log.info(
        f'Apply complete  kept={kept}  filed={filed}  '
        f'trashed={trashed}  errors={errors}  skipped={skipped}'
    )

    conn.logout()
    log.info('IMAP logged out')

    _finalise_run(db, manifest, kept, filed, trashed, errors, skipped)

    emit('done', {
        'total': len(emails), 'kept': kept, 'filed': filed,
        'trashed': trashed, 'errors': errors, 'skipped': skipped,
    })
    log.info('Worker finished successfully')


# ─────────────────────────────────────────────────────────────────────────────
# Entrypoint
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    manifest = JobManifest.from_env()
    log.info(
        f'Worker starting  job_type={manifest.job_type!r}  '
        f'run_id={manifest.run_id}  session={manifest.session_id}  '
        f'folder={manifest.folder!r}  batch_size={manifest.batch_size}  '
        f'skip_flagged={manifest.skip_flagged}  aggressive_trash={manifest.aggressive_trash}'
    )

    db = get_db()
    emit, log_action, update_run = _make_helpers(db, manifest)

    try:
        if manifest.job_type == 'folder_cleanup':
            run_folder_batch(manifest, db, emit, log_action, update_run)
        elif manifest.job_type in ('inbox_cleanup', 'scheduled_cleanup'):
            run_inbox_batch(manifest, db, emit, log_action, update_run)
        else:
            raise RuntimeError(f'Unknown job_type: {manifest.job_type!r}')

    except Exception as e:
        tb = traceback.format_exc()
        log.error(f'Worker crashed: {e}\n{tb}')
        emit('error', {
            'code':        'WORKER_CRASH',
            'message':     str(e),
            'remediation': tb[-500:],
        })
        try:
            db.execute(
                'UPDATE runs SET status="error", finished_at=? WHERE id=?',
                (datetime.utcnow().isoformat(), manifest.run_id),
            )
            db.commit()
        except Exception:
            pass
        sys.exit(1)

    finally:
        try:
            db.close()
        except Exception:
            pass


if __name__ == '__main__':
    main()
