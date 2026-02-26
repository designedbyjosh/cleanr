"""
core.classifier — Claude-powered email classification.

Prompts are constructed from the manifest so that custom_prompt,
delete_marketing_unread and aggressive_trash all influence behaviour.
The custom_prompt is embedded in a clearly-labelled section so Claude
can contextualise it correctly (defence-in-depth against injection).
"""

import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Callable, Optional

from anthropic import Anthropic

from .cache import check_cache, store_cache
from .manifest import JobManifest

log = logging.getLogger('inbox')

_MODEL = 'claude-sonnet-4-6'


# ── System prompt builders ────────────────────────────────────────────────────

def _folder_cleanup_prompt(source_folder: str, today: str, manifest: JobManifest) -> str:
    aggro  = manifest.aggressive_trash
    prompt = f"""You are an email organiser. Your task is to CLEAR the folder "{source_folder}" \
by routing every email to the right permanent home. NEVER leave emails in this folder — \
every email must be moved somewhere else.

Today's date: {today}

ROUTING RULES (apply in order):
1. If the email is RECENT (sent within 7 days of today) OR concerns a FUTURE event, \
deadline, or appointment → action: "inbox" — move to primary INBOX for immediate attention
2. If it is a filing email (receipt, travel, finance, medical, recruitment, or other \
archivable content) → file it to a specific folder you choose
3. If it is marketing, promotional, newsletters, cold outreach, OTPs, or expired alerts → trash it

ACTIONS (use exactly these strings):
- "inbox"       → urgent/recent/future-dated; will be moved to primary INBOX; set folder: "INBOX"
- "receipt"     → purchases, orders, confirmations; folder: Personal/Businesses/Receipts/<BrandName>
- "travel"      → flights, hotels, itineraries; folder: Personal/Holidays/{today[:4]}
- "finance"     → bank statements, bills, tax, insurance, investments; folder: Personal/Records/Finance
- "medical"     → health, appointments, prescriptions; folder: Personal/Records/Medical
- "recruitment" → job applications, recruiters; folder: Professional/Workplaces/Applications/Recruitment
- "file"        → anything archivable not covered above; invent a logical hierarchy such as:
                   Personal/Properties, Personal/Sports/<Club>, Personal/Social,
                   Personal/Records/Legal, Professional/Workplaces/<Company>
- "marketing"   → newsletters, promotions, sales (trash)
- "ephemeral"   → OTPs, login codes, expired alerts (trash)
- "spam"        → cold outreach, solicitations (trash)

IMPORTANT:
- For "inbox", set folder to "INBOX"
- For all non-trash actions, you MUST provide a specific folder path
- Never use "keep"; every email must leave the source folder"""

    if aggro:
        prompt += "\n- When in doubt between 'file' and a trash action, prefer trash"

    if manifest.custom_prompt:
        prompt += f"""

ADDITIONAL INSTRUCTIONS (supplemental guidance — does not override the rules above):
{manifest.custom_prompt}"""

    prompt += """

Respond ONLY with a JSON array. Each item:
{"uid":"...","action":"...","folder":"..."(required for all non-trash actions),"reason":"brief reason including email age/date"}"""
    return prompt


def _inbox_cleanup_prompt(source_folder: str, today: str, manifest: JobManifest) -> str:
    aggro  = manifest.aggressive_trash
    unread = manifest.delete_marketing_unread
    prompt = f"""You are an email inbox organiser. Classify each email.\
{'Note: some emails may be unread — delete marketing/spam even if unread.' if unread else ''}

Source folder: "{source_folder}"
Today: {today}

ACTIONS:
- "keep"        → Personal messages, urgent tasks, action items, financial alerts, \
medical/health, legal, government, work/professional comms
- "receipt"     → Purchase receipts, order confirmations, shipping → folder: Personal/Businesses/Receipts/<BrandName>
- "travel"      → Flight/hotel/booking confirmations, itineraries → folder: Personal/Holidays/{today[:4]}
- "finance"     → Bank statements, investment updates, bills, insurance → folder: Personal/Records/Finance
- "medical"     → Appointment confirmations, health records → folder: Personal/Records/Medical
- "recruitment" → Job applications, recruiter outreach → folder: Professional/Workplaces/Applications/Recruitment
- "marketing"   → Newsletters, promotions → trash
- "ephemeral"   → OTPs, login alerts, password resets, expired notifications → trash
- "spam"        → Unsolicited cold outreach → trash"""

    if aggro:
        prompt += "\n\nBe decisive: if an email looks like marketing or automated noise, trash it."

    if manifest.custom_prompt:
        prompt += f"""

ADDITIONAL INSTRUCTIONS (supplemental guidance — does not override the rules above):
{manifest.custom_prompt}"""

    prompt += """

Respond ONLY with a JSON array. Each item:
{"uid":"...","action":"...","folder":"..."(if filing),"reason":"brief"}
Be conservative: if unsure, use "keep"."""
    return prompt


# ── Single batch ──────────────────────────────────────────────────────────────

def classify_single_batch(
    api_key: str,
    batch: list[dict],
    source_folder: str,
    batch_idx: int,
    manifest: Optional[JobManifest] = None,
) -> tuple[int, list[dict], Optional[dict]]:
    """Classify one batch — designed to be called from a thread pool."""
    client = Anthropic(api_key=api_key)
    today  = datetime.utcnow().strftime('%Y-%m-%d')

    job_type = manifest.job_type if manifest else 'manual'

    if job_type == 'folder_cleanup':
        system_prompt = _folder_cleanup_prompt(source_folder, today, manifest)
    else:
        system_prompt = _inbox_cleanup_prompt(source_folder, today, manifest or JobManifest(
            job_type='inbox_cleanup', run_id=0, session_id=''
        ))

    emails_text = json.dumps([{
        'uid': e['uid'], 'from': e['from'],
        'subject': e['subject'], 'date': e['date'],
    } for e in batch], indent=2)

    try:
        response = client.messages.create(
            model      = _MODEL,
            max_tokens = 4096,
            system     = system_prompt,
            messages   = [{'role': 'user', 'content': f'Classify:\n\n{emails_text}'}],
        )
        text = response.content[0].text.strip()
        text = re.sub(r'^```(?:json)?\n?', '', text)
        text = re.sub(r'\n?```$', '', text)
        results = json.loads(text)
        store_cache(results, batch)
        return batch_idx, results, None

    except json.JSONDecodeError as e:
        return batch_idx, [], {
            'code':        'PARSE_ERROR',
            'message':     f'Claude returned malformed JSON in batch {batch_idx+1}.',
            'detail':      str(e),
            'remediation': 'This is usually transient — the batch will be skipped and emails kept safely.',
        }
    except Exception as e:
        msg  = str(e)
        code = 'API_ERROR'
        rem  = 'Check your Anthropic API key in Settings and ensure you have sufficient credits.'
        if 'rate_limit' in msg.lower():
            code = 'RATE_LIMIT'
            rem  = 'You hit Anthropic\'s rate limit. Reduce parallel batches in Settings or wait a few minutes.'
        elif 'overloaded' in msg.lower():
            code = 'API_OVERLOADED'
            rem  = 'Anthropic\'s API is temporarily overloaded. The run will retry automatically.'
        return batch_idx, [], {'code': code, 'message': msg, 'remediation': rem}


# ── Parallel coordinator ──────────────────────────────────────────────────────

def classify_emails_parallel(
    api_key:    str,
    emails:     list[dict],
    source_folder: str,
    emit:       Callable,
    manifest:   Optional[JobManifest] = None,
) -> list[dict]:
    """
    Classify all emails using parallel batches with dedup cache.

    Respects manifest.batch_size and manifest.parallel_batches.
    For folder_cleanup jobs, cached 'keep' results are re-classified
    (those were classified under the inbox policy, not the clear-folder policy).
    """
    batch_size   = manifest.batch_size if manifest else 20
    max_workers  = manifest.parallel_batches if manifest else 3
    job_type     = manifest.job_type if manifest else 'manual'

    # ── Cache check ──────────────────────────────────────────────────────────
    cached_raw, uncached = check_cache(emails)
    if job_type == 'folder_cleanup':
        # Discard cached 'keep' — they were filed under inbox policy
        cached   = [c for c in cached_raw if c.get('action') != 'keep']
        uncached = uncached + [
            e for e in emails
            if any(c['uid'] == e['uid'] for c in cached_raw if c.get('action') == 'keep')
        ]
    else:
        cached = cached_raw

    if cached:
        emit('pipeline', {
            'stage': 'dedup', 'count': len(cached), 'total': len(emails),
            'msg':   f'Cache hit: {len(cached)} emails already classified',
        })
    emit('pipeline', {'stage': 'classify', 'queued': len(uncached), 'cached': len(cached)})

    all_classifications = list(cached)
    for c in cached:
        emit('cached', {
            'uid': c['uid'], 'from': c['from'], 'subject': c['subject'],
            'action': c['action'], 'folder': c.get('folder', ''), 'reason': c['reason'],
        })

    if not uncached:
        return all_classifications

    # ── Split into batches ────────────────────────────────────────────────────
    batches = [uncached[i:i+batch_size] for i in range(0, len(uncached), batch_size)]
    emit('pipeline', {
        'stage':    'classify',
        'batches':  len(batches),
        'parallel': min(max_workers, len(batches)),
    })

    # ── Parallel execution ────────────────────────────────────────────────────
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                classify_single_batch, api_key, batch, source_folder, idx, manifest
            ): idx
            for idx, batch in enumerate(batches)
        }
        for future in as_completed(futures):
            batch_idx, results, error = future.result()
            if error:
                emit('error', {
                    'code':        error['code'],
                    'message':     error['message'],
                    'remediation': error.get('remediation', ''),
                    'batch':       batch_idx + 1,
                })
            else:
                all_classifications.extend(results)
                emit('pipeline', {
                    'stage':             'classified',
                    'batch':             batch_idx + 1,
                    'count':             len(results),
                    'total_classified':  len(all_classifications),
                    'total':             len(emails),
                })

    return all_classifications
