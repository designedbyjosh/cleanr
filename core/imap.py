"""
core.imap — IMAP connection helpers and email fetch/move/delete operations.

fetch_emails() returns FLAGS so callers can honour skip_flagged and
delete_marketing_unread without a second round-trip to the server.
"""

import email
import imaplib
import logging
import re
from datetime import datetime, timedelta
from email.header import decode_header
from typing import Optional

log = logging.getLogger('inbox')


# ── Encoding ──────────────────────────────────────────────────────────────────

def decode_str(s: str) -> str:
    if not s:
        return ''
    parts = decode_header(s)
    result = []
    for p, enc in parts:
        if isinstance(p, bytes):
            result.append(p.decode(enc or 'utf-8', errors='replace'))
        else:
            result.append(str(p))
    return ''.join(result)


# ── Connection ────────────────────────────────────────────────────────────────

def get_imap_conn(email_addr: str, app_password: str) -> imaplib.IMAP4_SSL:
    try:
        conn = imaplib.IMAP4_SSL('imap.mail.me.com', 993)
        conn.login(email_addr, app_password)
        return conn
    except imaplib.IMAP4.error as e:
        msg = str(e)
        if 'AUTHENTICATIONFAILED' in msg or 'Invalid credentials' in msg:
            raise ConnectionError(
                'IMAP authentication failed. Check your iCloud email and app-specific password. '
                'Note: you must use an App-Specific Password from appleid.apple.com, '
                'not your Apple ID password.'
            )
        raise ConnectionError(f'IMAP connection failed: {msg}')


# ── Folder management ─────────────────────────────────────────────────────────

def list_imap_folders(conn: imaplib.IMAP4_SSL) -> list[str]:
    _, data = conn.list()
    folders = []
    for item in data:
        if not isinstance(item, bytes):
            continue
        parts = item.decode('utf-8', errors='replace')
        match = re.search(r'"([^"]+)"\s*$', parts) or re.search(r'(\S+)\s*$', parts)
        if match:
            name = match.group(1).strip('"')
            if name not in ('', '[Gmail]'):
                folders.append(name)
    return sorted(set(folders))


def get_folder_counts(conn: imaplib.IMAP4_SSL, folders: list[str]) -> dict[str, int]:
    counts = {}
    for folder in folders:
        try:
            typ, data = conn.select(f'"{folder}"', readonly=True)
            counts[folder] = int(data[0]) if typ == 'OK' and data[0] else 0
        except Exception:
            counts[folder] = 0
    return counts


def ensure_folder(conn: imaplib.IMAP4_SSL, folder: str) -> None:
    try:
        conn.create(f'"{folder}"')
    except Exception:
        pass


def rename_folder(conn: imaplib.IMAP4_SSL, old_name: str, new_name: str) -> bool:
    tag = conn._new_tag().decode()
    conn.send(f'{tag} RENAME "{old_name}" "{new_name}"\r\n'.encode())
    resp = conn.readline()
    while not resp.startswith(tag.encode()):
        resp = conn.readline()
    return b'OK' in resp


# ── Raw IMAP helpers ──────────────────────────────────────────────────────────

def _raw_imap_cmd(conn: imaplib.IMAP4_SSL, cmd_str: str) -> bytes:
    tag = conn._new_tag().decode()
    conn.send(f'{tag} {cmd_str}\r\n'.encode())
    resp = conn.readline()
    while not resp.startswith(tag.encode()):
        resp = conn.readline()
    return resp


def _parse_flags(raw: bytes) -> set[str]:
    """Extract IMAP flag names (e.g. '\\Seen', '\\Flagged') from a fetch response line."""
    m = re.search(rb'FLAGS \(([^)]*)\)', raw)
    if not m:
        return set()
    return {f.decode() for f in m.group(1).split()}


# ── Fetch ─────────────────────────────────────────────────────────────────────

def _select_folder(conn: imaplib.IMAP4_SSL, folder: str, readonly: bool = True) -> None:
    try:
        conn.select(f'"{folder}"', readonly=readonly)
    except Exception:
        try:
            conn.select(folder, readonly=readonly)
        except Exception:
            conn.select('INBOX', readonly=readonly)


def _since_criteria(days_ago: Optional[int]) -> Optional[str]:
    """Return an IMAP SINCE date string, or None if no filter."""
    if days_ago is None:
        return None
    cutoff = datetime.utcnow() - timedelta(days=days_ago)
    return cutoff.strftime('%d-%b-%Y')   # e.g. "19-Feb-2026"


def fetch_emails_from_folder(
    conn: imaplib.IMAP4_SSL,
    folder: str,
    limit: int = 50,
    oldest_first: bool = True,
    offset: int = 0,
    since_days_ago: Optional[int] = None,
    skip_flagged: bool = True,
) -> tuple[list[dict], int]:
    """
    Fetch email headers from *folder*.

    Returns (emails, total_in_folder).
    Each email dict contains: uid, from, subject, date, flags, is_flagged.
    Flagged emails are included in the count but excluded from the list
    when skip_flagged=True.
    """
    _select_folder(conn, folder, readonly=True)

    since = _since_criteria(since_days_ago)
    criteria = ['SINCE', since] if since else ['ALL']
    _, data = conn.uid('search', None, *criteria)
    if not data or not data[0]:
        return [], 0

    all_uids = data[0].split()
    total = len(all_uids)

    ordered = all_uids[offset:] if oldest_first else list(reversed(all_uids))[offset:]
    target_uids = ordered[:limit]

    if not target_uids:
        return [], total

    uid_range = b','.join(target_uids)
    _, fetch_data = conn.uid(
        'fetch', uid_range,
        '(FLAGS BODY.PEEK[HEADER.FIELDS (FROM SUBJECT DATE)])'
    )

    emails = []
    for item in fetch_data:
        if not isinstance(item, tuple):
            continue
        uid_match = re.search(rb'UID (\d+)', item[0])
        if not uid_match:
            continue
        uid = uid_match.group(1).decode()
        flags = _parse_flags(item[0])
        is_flagged = '\\Flagged' in flags

        if skip_flagged and is_flagged:
            log.debug(f'Skipping flagged email uid={uid}')
            continue

        try:
            msg = email.message_from_bytes(item[1])
            emails.append({
                'uid':        uid,
                'from':       decode_str(msg.get('From', '')),
                'subject':    decode_str(msg.get('Subject', '')),
                'date':       msg.get('Date', ''),
                'flags':      list(flags),
                'is_flagged': is_flagged,
                'is_seen':    '\\Seen' in flags,
            })
        except Exception:
            continue

    return emails, total


def fetch_inbox_emails(
    conn: imaplib.IMAP4_SSL,
    limit: int = 50,
    folder: str = 'INBOX',
    oldest_first: bool = False,
    include_unread: bool = False,
    since_days_ago: Optional[int] = None,
    skip_flagged: bool = True,
) -> list[dict]:
    """
    Fetch emails for inbox-cleanup runs.

    By default fetches only SEEN (read) emails.
    With include_unread=True, also fetches UNSEEN emails so that unread
    marketing can be deleted (they will be filtered during apply).
    """
    _select_folder(conn, folder, readonly=True)

    since = _since_criteria(since_days_ago)
    if include_unread:
        criteria = ['ALL']
    else:
        criteria = ['SEEN']

    if since:
        criteria = criteria + ['SINCE', since]

    _, data = conn.uid('search', None, *criteria)
    if not data or not data[0]:
        return []

    all_uids = data[0].split()
    uids = all_uids[:limit] if oldest_first else all_uids[-limit:][::-1]

    if not uids:
        return []

    uid_range = b','.join(uids)
    _, fetch_data = conn.uid(
        'fetch', uid_range,
        '(FLAGS BODY.PEEK[HEADER.FIELDS (FROM SUBJECT DATE)])'
    )

    emails = []
    for item in fetch_data:
        if not isinstance(item, tuple):
            continue
        uid_match = re.search(rb'UID (\d+)', item[0])
        if not uid_match:
            continue
        uid = uid_match.group(1).decode()
        flags = _parse_flags(item[0])
        is_flagged = '\\Flagged' in flags

        if skip_flagged and is_flagged:
            log.debug(f'Skipping flagged inbox email uid={uid}')
            continue

        try:
            msg = email.message_from_bytes(item[1])
            emails.append({
                'uid':        uid,
                'from':       decode_str(msg.get('From', '')),
                'subject':    decode_str(msg.get('Subject', '')),
                'date':       msg.get('Date', ''),
                'flags':      list(flags),
                'is_flagged': is_flagged,
                'is_seen':    '\\Seen' in flags,
            })
        except Exception:
            continue

    return emails


# ── Move / Delete ─────────────────────────────────────────────────────────────

def move_email(
    conn: imaplib.IMAP4_SSL,
    uid: str,
    source_folder: str,
    dest_folder: str,
) -> bool:
    _select_folder(conn, source_folder, readonly=False)
    r = _raw_imap_cmd(conn, f'UID COPY {uid} "{dest_folder}"')
    if b'OK' in r:
        _raw_imap_cmd(conn, f'UID STORE {uid} +FLAGS (\\Deleted)')
        conn.expunge()
        return True
    raise RuntimeError(
        f'IMAP COPY failed for UID {uid} → {dest_folder}. '
        f'Server: {r.decode(errors="replace")}'
    )


def delete_email(
    conn: imaplib.IMAP4_SSL,
    uid: str,
    source_folder: str,
) -> bool:
    _select_folder(conn, source_folder, readonly=False)
    _raw_imap_cmd(conn, f'UID STORE {uid} +FLAGS (\\Deleted)')
    conn.expunge()
    return True


# ── AI folder suggestion ──────────────────────────────────────────────────────

def suggest_folder_reorganisation(api_key: str, folders: list[str]) -> dict:
    """
    Ask Claude to suggest a cleaner, more logical IMAP folder structure.

    Returns a dict with:
      changes     — list of {"op": "rename", "from": "old", "to": "new"}
      new_folders — list of {"path": "folder/path"} for folders to create
      explanation — short human-readable summary of the suggestion
    """
    import json as _json
    import anthropic

    if not folders:
        return {'changes': [], 'new_folders': [], 'explanation': 'No folders provided.'}

    folder_list = '\n'.join(f'  • {f}' for f in sorted(folders))
    prompt = (
        'You are an email organisation expert.  A user has these IMAP folders:\n\n'
        f'{folder_list}\n\n'
        'Suggest a cleaner, more logical folder hierarchy.  Rules:\n'
        '- Only suggest changes that genuinely improve organisation\n'
        '- Prefer shallow structures (2 levels max)\n'
        '- Preserve folders that already look good\n'
        '- Do not invent folders for mail that does not exist yet\n\n'
        'Return ONLY a JSON object (no markdown fences) with:\n'
        '  "changes":     [ {"op": "rename", "from": "OldName", "to": "NewName"} ]\n'
        '  "new_folders": [ {"path": "Folder/Subfolder"} ]\n'
        '  "explanation": "One short paragraph explaining your suggestions."\n'
    )

    client = anthropic.Anthropic(api_key=api_key)
    response = client.messages.create(
        model      = 'claude-3-haiku-20240307',
        max_tokens = 1024,
        messages   = [{'role': 'user', 'content': prompt}],
    )
    text = response.content[0].text.strip()

    # Strip accidental markdown fences
    if text.startswith('```'):
        text = re.sub(r'^```[a-z]*\n?', '', text)
        text = re.sub(r'\n?```$', '', text.strip())

    try:
        return _json.loads(text)
    except _json.JSONDecodeError:
        log.warning('suggest_folder_reorganisation: Claude returned non-JSON, wrapping as explanation')
        return {'changes': [], 'new_folders': [], 'explanation': text}
