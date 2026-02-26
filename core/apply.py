"""
core.apply — apply Claude's classifications to emails via IMAP.

Respects manifest options:
  - delete_marketing_unread: delete unread marketing/spam (skips keep for unread non-trash)
  - skip_flagged:            already filtered at fetch time; double-checked here
"""

import logging
import threading
import time
from typing import Callable, Optional

from .db import get_setting
from .imap import delete_email, ensure_folder, move_email
from .manifest import JobManifest

log = logging.getLogger('inbox')

_TRASH_ACTIONS = {'marketing', 'ephemeral', 'spam'}
_FILE_ACTIONS  = {'receipt', 'travel', 'finance', 'medical', 'recruitment', 'file'}


# ── Rate limiter ──────────────────────────────────────────────────────────────

class RateLimiter:
    def __init__(self) -> None:
        self._lock       = threading.Lock()
        self._timestamps: list[float] = []

    def check_and_record(self, max_per_hour: int) -> tuple[bool, float]:
        now    = time.time()
        cutoff = now - 3600
        with self._lock:
            self._timestamps = [t for t in self._timestamps if t > cutoff]
            if len(self._timestamps) >= max_per_hour:
                wait = self._timestamps[0] + 3600 - now
                return False, wait
            self._timestamps.append(now)
            return True, 0.0


_rate_limiter = RateLimiter()


# ── Apply ─────────────────────────────────────────────────────────────────────

def apply_classifications(
    conn,
    classifications: list[dict],
    emails:          list[dict],
    source_folder:   str,
    emit:            Callable,
    log_action:      Callable,
    update_run:      Callable,
    manifest:        Optional[JobManifest] = None,
) -> tuple[int, int, int, int, int]:
    """
    Execute IMAP operations for each classified email.

    Returns (kept, filed, trashed, errors, skipped).
    """
    is_folder_job       = manifest.job_type == 'folder_cleanup' if manifest else False
    del_mkt_unread      = manifest.delete_marketing_unread if manifest else False
    skip_flagged        = manifest.skip_flagged if manifest else True
    max_per_hour        = int(get_setting('rate_limit_per_hour', 200))

    kept = filed = trashed = errors = skipped = 0
    email_map = {e['uid']: e for e in emails}

    emit('pipeline', {'stage': 'apply', 'total': len(classifications)})

    for c in classifications:
        # ── Rate limit ────────────────────────────────────────────────────────
        allowed, wait_secs = _rate_limiter.check_and_record(max_per_hour)
        if not allowed:
            emit('status', {'msg': f'Rate limit — waiting {int(wait_secs)}s…'})
            time.sleep(min(wait_secs, 60))

        uid        = c['uid']
        action     = c.get('action', 'keep')
        folder     = c.get('folder', '')
        reason     = c.get('reason', '')
        from_cache = c.get('from_cache', False)
        orig       = email_map.get(uid, {})
        from_addr  = orig.get('from',  c.get('from', ''))
        subject    = orig.get('subject', c.get('subject', ''))
        is_seen    = orig.get('is_seen', True)
        is_flagged = orig.get('is_flagged', False)

        # ── Skip flagged emails (double-check — fetch already filtered) ────────
        if skip_flagged and is_flagged:
            skipped += 1
            log_action(uid, from_addr, subject, 'skip', '', 'Flagged email — skipped')
            emit('action', {
                'uid': uid, 'from': from_addr, 'subject': subject,
                'action': 'skip', 'stage': 'skip', 'reason': 'Flagged email — skipped',
                'from_cache': from_cache,
            })
            update_run(kept=kept, filed=filed, trashed=trashed, errors=errors, skipped=skipped)
            continue

        # ── Folder-job guarantee: "keep" → move to INBOX ─────────────────────
        if is_folder_job and action == 'keep':
            action = 'inbox'
            folder = 'INBOX'

        # ── Unread-marketing gate ─────────────────────────────────────────────
        # For inbox runs: if the email is unread AND the action is NOT a trash
        # action, skip it (preserve unread non-marketing emails).
        # Unless delete_marketing_unread is on — in which case we allow trashing
        # unread emails classified as marketing/spam/ephemeral.
        if not is_folder_job and not is_seen:
            if action not in _TRASH_ACTIONS:
                skipped += 1
                log_action(uid, from_addr, subject, 'skip', '', 'Unread — skipped')
                emit('action', {
                    'uid': uid, 'from': from_addr, 'subject': subject,
                    'action': 'skip', 'stage': 'skip', 'reason': 'Unread email — skipped',
                    'from_cache': from_cache,
                })
                update_run(kept=kept, filed=filed, trashed=trashed, errors=errors, skipped=skipped)
                continue
            elif not del_mkt_unread:
                # Unread marketing but feature is disabled
                skipped += 1
                log_action(uid, from_addr, subject, 'skip', '',
                           'Unread marketing — skipped (enable delete_marketing_unread to trash)')
                emit('action', {
                    'uid': uid, 'from': from_addr, 'subject': subject,
                    'action': 'skip', 'stage': 'skip',
                    'reason': 'Unread marketing — feature disabled',
                    'from_cache': from_cache,
                })
                update_run(kept=kept, filed=filed, trashed=trashed, errors=errors, skipped=skipped)
                continue

        base = {
            'uid': uid, 'from': from_addr, 'subject': subject,
            'action': action, 'folder': folder, 'reason': reason,
            'from_cache': from_cache,
        }

        try:
            if action == 'keep':
                kept += 1
                log_action(uid, from_addr, subject, action, folder, reason)
                emit('action', {**base, 'stage': 'keep'})

            elif action == 'inbox':
                dest = 'INBOX'
                move_email(conn, uid, source_folder, dest)
                filed += 1
                log_action(uid, from_addr, subject, action, dest, reason)
                emit('action', {**base, 'folder': dest, 'stage': 'filed'})

            elif action in _FILE_ACTIONS:
                if folder:
                    ensure_folder(conn, folder)
                    move_email(conn, uid, source_folder, folder)
                    filed += 1
                    log_action(uid, from_addr, subject, action, folder, reason)
                    emit('action', {**base, 'stage': 'filed'})
                else:
                    dest = 'INBOX'
                    move_email(conn, uid, source_folder, dest)
                    filed += 1
                    log_action(uid, from_addr, subject, 'inbox', dest,
                               'No folder assigned — sent to INBOX')
                    emit('action', {**base, 'action': 'inbox', 'folder': dest,
                                    'stage': 'filed', 'reason': 'No folder — sent to INBOX'})

            elif action in _TRASH_ACTIONS:
                delete_email(conn, uid, source_folder)
                trashed += 1
                log_action(uid, from_addr, subject, action, '', reason)
                emit('action', {**base, 'stage': 'trash'})

            else:
                # Unknown action — keep safely
                kept += 1
                log_action(uid, from_addr, subject, 'keep', '', f'Unknown action: {action}')
                emit('action', {**base, 'action': 'keep', 'stage': 'keep'})

            update_run(kept=kept, filed=filed, trashed=trashed, errors=errors, skipped=skipped)

        except RuntimeError as e:
            errors += 1
            log.warning(f'IMAP_MOVE_FAILED uid={uid} subject={subject[:40]!r}: {e}')
            emit('error', {
                'code':        'IMAP_MOVE_FAILED',
                'message':     str(e),
                'remediation': 'The destination folder may not exist or the IMAP server rejected '
                               'the operation. Check your folder structure in Folder Manager.',
                'uid':     uid,
                'subject': subject,
            })
        except Exception as e:
            errors += 1
            log.warning(f'UNKNOWN error uid={uid}: {e}')
            emit('error', {
                'code':        'UNKNOWN',
                'message':     f'Unexpected error processing {subject[:40]}: {e}',
                'remediation': 'This is likely a transient error. Check the run log and retry.',
                'uid': uid,
            })

    emit('pipeline', {
        'stage': 'done', 'kept': kept, 'filed': filed, 'trashed': trashed,
        'errors': errors, 'skipped': skipped,
    })
    return kept, filed, trashed, errors, skipped
