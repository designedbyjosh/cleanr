"""
core.manifest — JobManifest dataclass and prompt-injection sanitiser.

The manifest is the single source of truth for every worker run.
It is serialised to JSON and passed via the MANIFEST environment variable,
keeping the container interface clean and the credential surface minimal.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import asdict, dataclass, field
from typing import Optional

# ── Prompt-injection protection ───────────────────────────────────────────────
# These patterns are stripped from any user-supplied custom_prompt before it is
# embedded in a system prompt.  Defence-in-depth: the prompt is also inserted
# inside an explicit labelled block so Claude can contextualise it correctly.

_INJECTION_PATTERNS = [
    r'</?system\s*>',
    r'\[/?INST\]',
    r'ignore\s+(all\s+)?previous\s+instructions?',
    r'disregard\s+(all\s+)?previous\s+instructions?',
    r'you\s+are\s+now\b',
    r'new\s+instructions?:',
    r'system\s+prompt:',
    r'</?\s*prompt\s*>',
    r'<\|[^|]*\|>',      # special tokens like <|im_start|>
    r'---+\s*system\s*---+',
]
_INJECTION_RE  = re.compile('|'.join(_INJECTION_PATTERNS), re.IGNORECASE)
_MAX_PROMPT_LEN = 500


def sanitise_custom_prompt(text: str) -> str:
    """
    Remove known prompt-injection patterns and enforce a length cap.
    Returns text that is safe to embed inside a system prompt.
    """
    if not text:
        return ''
    cleaned = _INJECTION_RE.sub('', text)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned[:_MAX_PROMPT_LEN]


# ── Manifest ─────────────────────────────────────────────────────────────────

@dataclass
class JobManifest:
    """
    All parameters that govern a single worker run.

    job_type values:
      "folder_cleanup"   — clear an entire folder batch-by-batch (folder jobs)
      "inbox_cleanup"    — process read (and optionally unread) INBOX emails
      "scheduled_cleanup"— same as inbox_cleanup but triggered by the scheduler
    """

    # ── Identity ──────────────────────────────────────────────────────────────
    job_type:   str
    run_id:     int
    session_id: str

    # ── Target ────────────────────────────────────────────────────────────────
    folder:  str          = 'INBOX'
    job_id:  Optional[int] = None

    # ── Volume ────────────────────────────────────────────────────────────────
    batch_size:          int           = 20
    oldest_first:        bool          = True
    start_from_days_ago: Optional[int] = None  # only process emails ≤ N days old
    max_emails:          Optional[int] = None  # hard cap on emails fetched per run

    # ── Classification tuning ─────────────────────────────────────────────────
    custom_prompt:           str  = ''     # extra guidance for Claude (sanitised)
    delete_marketing_unread: bool = False  # delete marketing even when unread
    skip_flagged:            bool = True   # never touch \\Flagged / starred mail
    aggressive_trash:        bool = False  # lean towards deleting borderline emails

    # ── Runtime ───────────────────────────────────────────────────────────────
    parallel_batches: int = 3
    db_path:          str = '/data/inbox_cleaner.db'

    # ─────────────────────────────────────────────────────────────────────────

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, text: str) -> 'JobManifest':
        data = json.loads(text)
        # Sanitise on load — defence-in-depth
        if 'custom_prompt' in data:
            data['custom_prompt'] = sanitise_custom_prompt(data['custom_prompt'])
        # Only pass recognised fields
        known = {f for f in cls.__dataclass_fields__}
        return cls(**{k: v for k, v in data.items() if k in known})

    @classmethod
    def from_env(cls) -> 'JobManifest':
        raw = os.environ.get('MANIFEST', '')
        if not raw:
            raise RuntimeError('MANIFEST environment variable is not set')
        return cls.from_json(raw)

    @classmethod
    def from_folder_job(cls, job: dict, run_id: int, session_id: str,
                        parallel_batches: int = 3,
                        db_path: str = '/data/inbox_cleaner.db') -> 'JobManifest':
        """Build a manifest from a folder_jobs DB row."""
        return cls(
            job_type             = 'folder_cleanup',
            run_id               = run_id,
            session_id           = session_id,
            folder               = job['folder'],
            job_id               = job['id'],
            batch_size           = job['batch_size'],
            oldest_first         = bool(job['oldest_first']),
            start_from_days_ago  = job['start_from_days_ago'],
            max_emails           = job['max_emails'],
            custom_prompt        = sanitise_custom_prompt(job['custom_prompt'] or ''),
            delete_marketing_unread = bool(job['delete_marketing_unread']),
            skip_flagged         = bool(job['skip_flagged']),
            aggressive_trash     = bool(job['aggressive_trash']),
            parallel_batches     = parallel_batches,
            db_path              = db_path,
        )

    @classmethod
    def from_schedule(cls, sched: dict, run_id: int, session_id: str,
                      limit: int, parallel_batches: int = 3,
                      db_path: str = '/data/inbox_cleaner.db') -> 'JobManifest':
        """Build a manifest from a schedules DB row."""
        return cls(
            job_type             = 'scheduled_cleanup',
            run_id               = run_id,
            session_id           = session_id,
            folder               = sched.get('folder', 'INBOX'),
            job_id               = None,
            batch_size           = limit,
            oldest_first         = True,
            custom_prompt        = sanitise_custom_prompt(sched.get('custom_prompt') or ''),
            delete_marketing_unread = bool(sched.get('delete_marketing_unread', 0)),
            skip_flagged         = bool(sched.get('skip_flagged', 1)),
            parallel_batches     = parallel_batches,
            db_path              = db_path,
        )
