"""
app.py — Flask application factory and API routes.

All business logic lives in core/.  This file registers routes and
wires up the in-process progress store for manual (non-containerised) runs.
"""

import json
import logging
import re
import secrets
import subprocess
import threading
import time
from datetime import datetime

from flask import Flask, Response, jsonify, render_template, request, stream_with_context

from core.apply import apply_classifications
from core.cache import clear_cache, get_cache_stats
from core.classifier import classify_emails_parallel
from core.db import (
    DB_PATH,
    get_credential, get_credential_meta, get_db, get_setting,
    init_db, save_credential, save_setting,
)
from core.docker_runner import docker_available, list_worker_containers
from core.imap import (
    ensure_folder, fetch_inbox_emails, get_folder_counts,
    get_imap_conn, list_imap_folders, rename_folder,
    suggest_folder_reorganisation,
)
from core.manifest import JobManifest, sanitise_custom_prompt
from core.orchestrator import recover_running_jobs, run_folder_job, start_job_thread
from core.scheduler import start_scheduler

log = logging.getLogger('inbox')

app = Flask(__name__)

# In-memory SSE store for manual (non-containerised) runs
progress_store: dict[str, list] = {}


# ─────────────────────────────────────────────────────────────────────────────
# Manual cleanup (in-process, streams via progress_store)
# ─────────────────────────────────────────────────────────────────────────────

def _run_manual_cleanup(session_id, run_id, email_addr, app_password, api_key,
                        limit, source_folder='INBOX',
                        run_type='manual', manifest=None):
    db = get_db()

    def emit(event, data):
        if session_id not in progress_store:
            progress_store[session_id] = []
        progress_store[session_id].append({'event': event, 'data': data, 'ts': time.time()})

    def update_run(**kwargs):
        sets = ', '.join(f'{k} = ?' for k in kwargs)
        db.execute(f'UPDATE runs SET {sets} WHERE id = ?', (*kwargs.values(), run_id))
        db.commit()

    def log_action(uid, from_addr, subject, action, folder, reason):
        db.execute(
            'INSERT INTO actions '
            '(run_id, uid, from_addr, subject, action, folder, reason, created_at) '
            'VALUES (?,?,?,?,?,?,?,?)',
            (run_id, uid, from_addr, subject, action, folder, reason,
             datetime.utcnow().isoformat()),
        )
        db.commit()

    try:
        m = manifest or JobManifest(
            job_type='inbox_cleanup', run_id=run_id, session_id=session_id,
            folder=source_folder, batch_size=int(limit),
        )

        emit('status', {'msg': 'Connecting to iCloud Mail…', 'stage': 'connect'})
        emit('pipeline', {'stage': 'connect', 'status': 'running'})
        conn = get_imap_conn(email_addr, app_password)

        emit('status', {'msg': f'Fetching emails from {source_folder}…', 'stage': 'fetch'})
        emit('pipeline', {'stage': 'fetch', 'status': 'running'})

        emails = fetch_inbox_emails(
            conn,
            limit           = int(limit),
            folder          = source_folder,
            oldest_first    = get_setting('inbox_zero_mode', '1') == '1',
            include_unread  = m.delete_marketing_unread,
            since_days_ago  = m.start_from_days_ago,
            skip_flagged    = m.skip_flagged,
        )

        if not emails:
            emit('status', {'msg': 'No emails to process.', 'stage': 'done'})
            emit('pipeline', {'stage': 'fetch', 'status': 'done', 'count': 0})
            emit('done', {'total': 0, 'kept': 0, 'filed': 0, 'trashed': 0,
                          'errors': 0, 'skipped': 0})
            update_run(status='done', finished_at=datetime.utcnow().isoformat(), total=0)
            conn.logout()
            db.close()
            return

        total = len(emails)
        update_run(total=total)
        emit('pipeline', {'stage': 'fetch', 'status': 'done', 'count': total})
        emit('status', {'msg': f'Fetched {total} emails. Classifying…', 'stage': 'classify'})

        all_classifications = classify_emails_parallel(
            api_key, emails, source_folder, emit, manifest=m,
        )

        emit('status', {
            'msg': f'Classified {len(all_classifications)}. Applying…', 'stage': 'apply',
        })
        update_run(total=total)

        kept, filed, trashed, errors, skipped = apply_classifications(
            conn, all_classifications, emails, source_folder,
            emit, log_action, update_run, manifest=m,
        )

        conn.logout()
        update_run(status='done', finished_at=datetime.utcnow().isoformat(),
                   kept=kept, filed=filed, trashed=trashed, errors=errors, skipped=skipped)
        emit('status', {'msg': 'Complete ✓', 'stage': 'done'})
        emit('done', {'total': total, 'kept': kept, 'filed': filed,
                      'trashed': trashed, 'errors': errors, 'skipped': skipped})

    except ConnectionError as e:
        update_run(status='error', finished_at=datetime.utcnow().isoformat())
        emit('error', {
            'code':        'CONNECTION_FAILED',
            'message':     str(e),
            'remediation': 'Go to Settings and verify your iCloud email and app-specific password.',
        })
        emit('done', {'total': 0, 'kept': 0, 'filed': 0, 'trashed': 0, 'errors': 1, 'skipped': 0})
    except Exception as e:
        update_run(status='error', finished_at=datetime.utcnow().isoformat())
        emit('error', {
            'code':        'FATAL',
            'message':     str(e),
            'remediation': 'An unexpected error occurred. Check the logs and try again.',
        })
        emit('done', {'total': 0, 'kept': 0, 'filed': 0, 'trashed': 0, 'errors': 1, 'skipped': 0})
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────────────────────
# Routes — UI
# ─────────────────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('index.html')


# ─────────────────────────────────────────────────────────────────────────────
# Routes — Credentials & Settings
# ─────────────────────────────────────────────────────────────────────────────

@app.route('/api/credentials', methods=['GET'])
def get_credentials_meta():
    return jsonify({k: get_credential_meta(k) for k in ('email', 'app_password', 'api_key')})


@app.route('/api/credentials', methods=['POST'])
def save_credentials():
    data = request.json or {}
    for key in ('email', 'app_password', 'api_key'):
        if data.get(key):
            save_credential(key, data[key])
    return jsonify({'ok': True})


@app.route('/api/settings', methods=['GET'])
def get_settings():
    keys = ['rate_limit_per_hour', 'batch_delay_seconds', 'inbox_zero_mode',
            'default_limit', 'parallel_batches', 'cache_ttl_days']
    return jsonify({k: get_setting(k) for k in keys})


@app.route('/api/settings', methods=['POST'])
def save_settings():
    data    = request.json or {}
    allowed = ['rate_limit_per_hour', 'batch_delay_seconds', 'inbox_zero_mode',
               'default_limit', 'parallel_batches', 'cache_ttl_days']
    for k in allowed:
        if k in data:
            save_setting(k, data[k])
    return jsonify({'ok': True})


# ─────────────────────────────────────────────────────────────────────────────
# Routes — Stats & Runs
# ─────────────────────────────────────────────────────────────────────────────

@app.route('/api/stats')
def get_stats():
    from datetime import timedelta
    db     = get_db()
    totals = db.execute(
        'SELECT SUM(kept) as kept, SUM(filed) as filed, SUM(trashed) as trashed, '
        'SUM(errors) as errors FROM runs WHERE status="done"'
    ).fetchone()
    rows = db.execute('''
        SELECT DATE(started_at) as day,
               SUM(kept) as kept, SUM(filed) as filed,
               SUM(trashed) as trashed, SUM(errors) as errors
        FROM runs WHERE status="done" AND started_at >= datetime("now", "-14 days")
        GROUP BY day ORDER BY day
    ''').fetchall()
    recent_runs   = db.execute('SELECT * FROM runs ORDER BY started_at DESC LIMIT 10').fetchall()
    action_counts = db.execute(
        'SELECT action, COUNT(*) as count FROM actions GROUP BY action ORDER BY count DESC'
    ).fetchall()
    cache_stats = get_cache_stats()

    daily = []
    for i in range(14):
        d     = (datetime.utcnow() - timedelta(days=13 - i)).strftime('%Y-%m-%d')
        match = next((r for r in rows if r['day'] == d), None)
        daily.append({
            'day':     d,
            'label':   (datetime.utcnow() - timedelta(days=13 - i)).strftime('%b %d'),
            'kept':    match['kept']    if match else 0,
            'filed':   match['filed']   if match else 0,
            'trashed': match['trashed'] if match else 0,
            'errors':  match['errors']  if match else 0,
        })
    db.close()
    return jsonify({
        'totals':           {'kept':    totals['kept']    or 0,
                             'filed':   totals['filed']   or 0,
                             'trashed': totals['trashed'] or 0,
                             'errors':  totals['errors']  or 0},
        'daily':            daily,
        'recent_runs':      [dict(r) for r in recent_runs],
        'action_breakdown': [dict(r) for r in action_counts],
        'cache':            cache_stats,
    })


@app.route('/api/runs')
def list_runs():
    db   = get_db()
    runs = db.execute('SELECT * FROM runs ORDER BY started_at DESC LIMIT 50').fetchall()
    db.close()
    return jsonify([dict(r) for r in runs])


@app.route('/api/runs/<int:run_id>/actions')
def run_actions(run_id):
    db      = get_db()
    actions = db.execute(
        'SELECT * FROM actions WHERE run_id = ? ORDER BY id DESC', (run_id,)
    ).fetchall()
    db.close()
    return jsonify([dict(a) for a in actions])


# ─────────────────────────────────────────────────────────────────────────────
# Routes — Manual run + SSE progress
# ─────────────────────────────────────────────────────────────────────────────

@app.route('/api/run', methods=['POST'])
def start_run():
    data          = request.json or {}
    email_addr    = get_credential('email')
    app_password  = get_credential('app_password')
    api_key       = get_credential('api_key')
    if not all([email_addr, app_password, api_key]):
        return jsonify({'error': 'Credentials not configured. Go to Settings.'}), 400

    source_folder = data.get('source_folder', 'INBOX')
    limit         = data.get('limit', int(get_setting('default_limit', 50)))

    manifest = JobManifest(
        job_type                = 'inbox_cleanup',
        run_id                  = 0,            # patched below
        session_id              = '',           # patched below
        folder                  = source_folder,
        batch_size              = int(limit),
        custom_prompt           = sanitise_custom_prompt(data.get('custom_prompt', '')),
        delete_marketing_unread = bool(data.get('delete_marketing_unread', False)),
        skip_flagged            = bool(data.get('skip_flagged', True)),
        aggressive_trash        = bool(data.get('aggressive_trash', False)),
        start_from_days_ago     = data.get('start_from_days_ago'),
        parallel_batches        = int(get_setting('parallel_batches', 3) or 3),
    )

    db        = get_db()
    cursor    = db.execute(
        'INSERT INTO runs (started_at, status, run_type, source_folder) '
        'VALUES (?, "running", "manual", ?)',
        (datetime.utcnow().isoformat(), source_folder),
    )
    run_id    = cursor.lastrowid
    db.commit()
    db.close()

    session_id         = f'run_{run_id}_{secrets.token_hex(8)}'
    progress_store[session_id] = []
    manifest.run_id    = run_id
    manifest.session_id = session_id

    threading.Thread(
        target = _run_manual_cleanup,
        args   = (session_id, run_id, email_addr, app_password, api_key,
                  limit, source_folder, 'manual', manifest),
        daemon = True,
    ).start()
    return jsonify({'session_id': session_id, 'run_id': run_id})


@app.route('/api/progress/<session_id>')
def stream_progress(session_id):
    def generate():
        mem_sent   = 0
        db_last_id = 0
        reconnect  = request.headers.get('Last-Event-ID', '')
        if reconnect.isdigit():
            db_last_id = int(reconnect)

        start     = time.time()
        last_ping = time.time()

        while time.time() - start < 3600:
            done = False

            # 1. In-memory (manual runs)
            events = progress_store.get(session_id, [])
            while mem_sent < len(events):
                e = events[mem_sent]
                yield f"data: {json.dumps(e)}\n\n"
                mem_sent += 1
                if e['event'] == 'done':
                    done = True
                    break

            # 2. DB events (containerised runs)
            if not done:
                edb  = get_db()
                rows = edb.execute(
                    'SELECT id, event, data FROM job_events '
                    'WHERE session_id=? AND id>? ORDER BY id',
                    (session_id, db_last_id),
                ).fetchall()
                edb.close()
                for row in rows:
                    db_last_id = row['id']
                    envelope   = {
                        'event': row['event'],
                        'data':  json.loads(row['data']),
                        'ts':    time.time(),
                    }
                    yield f"id: {db_last_id}\ndata: {json.dumps(envelope)}\n\n"
                    if row['event'] == 'done':
                        done = True
                        break

            if done:
                return

            if time.time() - last_ping > 5:
                yield ": keepalive\n\n"
                last_ping = time.time()

            time.sleep(0.15)

    return Response(
        stream_with_context(generate()),
        mimetype='text/event-stream',
        headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no',
                 'Connection': 'keep-alive'},
    )


# ─────────────────────────────────────────────────────────────────────────────
# Routes — Schedules
# ─────────────────────────────────────────────────────────────────────────────

_SCHEDULE_FIELDS = ['name', 'enabled', 'interval_hours', 'interval_minutes', 'limit_per_run',
                    'folder', 'custom_prompt', 'delete_marketing_unread', 'skip_flagged']


@app.route('/api/schedules', methods=['GET'])
def list_schedules():
    db   = get_db()
    rows = db.execute('SELECT * FROM schedules ORDER BY created_at DESC').fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/schedules', methods=['POST'])
def create_schedule():
    data = request.json or {}
    db   = get_db()
    interval_minutes = data.get('interval_minutes')
    interval_hours   = data.get('interval_hours', 24)
    cursor = db.execute(
        'INSERT INTO schedules '
        '(name, enabled, interval_hours, interval_minutes, limit_per_run, folder, '
        ' custom_prompt, delete_marketing_unread, skip_flagged, created_at) '
        'VALUES (?,?,?,?,?,?,?,?,?,?)',
        (data.get('name', 'Auto cleanup'),
         data.get('enabled', 1),
         interval_hours,
         int(interval_minutes) if interval_minutes is not None else None,
         data.get('limit_per_run', 50),
         data.get('folder', 'INBOX'),
         sanitise_custom_prompt(data.get('custom_prompt', '')),
         int(data.get('delete_marketing_unread', 0)),
         int(data.get('skip_flagged', 1)),
         datetime.utcnow().isoformat()),
    )
    db.commit()
    row = db.execute('SELECT * FROM schedules WHERE id = ?', (cursor.lastrowid,)).fetchone()
    db.close()
    return jsonify(dict(row))


@app.route('/api/schedules/<int:sid>', methods=['PATCH'])
def update_schedule(sid):
    data = request.json or {}
    db   = get_db()
    for k in _SCHEDULE_FIELDS:
        if k in data:
            v = sanitise_custom_prompt(data[k]) if k == 'custom_prompt' else data[k]
            db.execute(f'UPDATE schedules SET {k} = ? WHERE id = ?', (v, sid))
    db.commit()
    row = db.execute('SELECT * FROM schedules WHERE id = ?', (sid,)).fetchone()
    db.close()
    return jsonify(dict(row) if row else {})


@app.route('/api/schedules/<int:sid>', methods=['DELETE'])
def delete_schedule(sid):
    db = get_db()
    db.execute('DELETE FROM schedules WHERE id = ?', (sid,))
    db.commit()
    db.close()
    return jsonify({'ok': True})


# ─────────────────────────────────────────────────────────────────────────────
# Routes — Folders
# ─────────────────────────────────────────────────────────────────────────────

@app.route('/api/folders', methods=['GET'])
def list_folders():
    ea = get_credential('email')
    ap = get_credential('app_password')
    if not ea or not ap:
        return jsonify({'error': 'Credentials not set'}), 400
    want_counts = request.args.get('counts', 'true').lower() != 'false'
    try:
        conn    = get_imap_conn(ea, ap)
        folders = list_imap_folders(conn)
        if not want_counts:
            conn.logout()
            return jsonify({'folders': folders, 'counts': {}})
        counts = get_folder_counts(conn, folders)
        conn.logout()
        return jsonify({'folders': folders, 'counts': counts})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/folders/create', methods=['POST'])
def create_folder():
    data   = request.json or {}
    folder = data.get('folder', '').strip()
    if not folder:
        return jsonify({'error': 'folder required'}), 400
    ea = get_credential('email')
    ap = get_credential('app_password')
    try:
        conn = get_imap_conn(ea, ap)
        conn.create(f'"{folder}"')
        conn.logout()
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/folders/rename', methods=['POST'])
def do_rename_folder():
    data = request.json or {}
    old  = data.get('from', '').strip()
    new  = data.get('to',   '').strip()
    if not old or not new:
        return jsonify({'error': 'from and to required'}), 400
    ea = get_credential('email')
    ap = get_credential('app_password')
    try:
        conn = get_imap_conn(ea, ap)
        ok   = rename_folder(conn, old, new)
        conn.logout()
        return jsonify({'ok': ok})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/folders/suggest', methods=['POST'])
def suggest_folders():
    data    = request.json or {}
    api_key = get_credential('api_key')
    if not api_key:
        return jsonify({'error': 'API key not set'}), 400
    try:
        suggestion = suggest_folder_reorganisation(api_key, data.get('folders', []))
        return jsonify(suggestion)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/folders/apply', methods=['POST'])
def apply_folder_changes():
    data        = request.json or {}
    changes     = data.get('changes', [])
    new_folders = data.get('new_folders', [])
    ea = get_credential('email')
    ap = get_credential('app_password')
    results = []
    try:
        conn = get_imap_conn(ea, ap)
        for nf in new_folders:
            try:
                conn.create(f'"{nf["path"]}"')
                results.append({'op': 'create', 'path': nf['path'], 'ok': True})
            except Exception as e:
                results.append({'op': 'create', 'path': nf['path'],
                                'ok': False, 'error': str(e)})
        for c in changes:
            if c['op'] == 'rename':
                try:
                    ok = rename_folder(conn, c['from'], c['to'])
                    results.append({'op': 'rename', 'from': c['from'], 'to': c['to'], 'ok': ok})
                except Exception as e:
                    results.append({'op': 'rename', 'from': c['from'], 'to': c['to'],
                                    'ok': False, 'error': str(e)})
        conn.logout()
        return jsonify({'results': results})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ─────────────────────────────────────────────────────────────────────────────
# Routes — Folder jobs
# ─────────────────────────────────────────────────────────────────────────────

_JOB_WRITE_FIELDS = [
    'name', 'enabled', 'batch_size', 'rate_limit_per_hour', 'oldest_first',
    'start_from_days_ago', 'max_emails', 'custom_prompt',
    'delete_marketing_unread', 'skip_flagged', 'aggressive_trash',
]


@app.route('/api/folder-jobs', methods=['GET'])
def list_folder_jobs():
    db   = get_db()
    rows = db.execute('SELECT * FROM folder_jobs ORDER BY created_at DESC').fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/folder-jobs', methods=['POST'])
def create_folder_job():
    data = request.json or {}
    db   = get_db()
    cursor = db.execute(
        'INSERT INTO folder_jobs '
        '(name, folder, enabled, batch_size, rate_limit_per_hour, oldest_first, '
        ' start_from_days_ago, max_emails, custom_prompt, '
        ' delete_marketing_unread, skip_flagged, aggressive_trash, created_at) '
        'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
        (data.get('name', 'Folder cleanup'),
         data.get('folder', 'INBOX'),
         1,
         data.get('batch_size', 20),
         data.get('rate_limit_per_hour', 60),
         1 if data.get('oldest_first', True) else 0,
         data.get('start_from_days_ago'),
         data.get('max_emails'),
         sanitise_custom_prompt(data.get('custom_prompt', '')),
         int(data.get('delete_marketing_unread', 0)),
         int(data.get('skip_flagged', 1)),
         int(data.get('aggressive_trash', 0)),
         datetime.utcnow().isoformat()),
    )
    job_id = cursor.lastrowid
    db.commit()
    row = db.execute('SELECT * FROM folder_jobs WHERE id = ?', (job_id,)).fetchone()
    db.close()
    return jsonify(dict(row))


@app.route('/api/folder-jobs/<int:jid>', methods=['PATCH'])
def update_folder_job(jid):
    data = request.json or {}
    db   = get_db()
    for k in _JOB_WRITE_FIELDS:
        if k in data:
            v = sanitise_custom_prompt(data[k]) if k == 'custom_prompt' else data[k]
            db.execute(f'UPDATE folder_jobs SET {k}=? WHERE id=?', (v, jid))
    db.commit()
    row = db.execute('SELECT * FROM folder_jobs WHERE id=?', (jid,)).fetchone()
    db.close()
    return jsonify(dict(row) if row else {})


@app.route('/api/folder-jobs/<int:jid>', methods=['DELETE'])
def delete_folder_job(jid):
    db = get_db()
    db.execute('UPDATE folder_jobs SET enabled=0 WHERE id=?', (jid,))
    db.execute('DELETE FROM folder_jobs WHERE id=?', (jid,))
    db.commit()
    db.close()
    return jsonify({'ok': True})


@app.route('/api/folder-jobs/<int:jid>/start', methods=['POST'])
def start_folder_job(jid):
    log.info(f'[job={jid}] START requested')
    db = get_db()
    db.execute('UPDATE folder_jobs SET enabled=1 WHERE id=?', (jid,))
    db.commit()
    db.close()
    session_id = start_job_thread(jid)
    log.info(f'[job={jid}] START → session_id={session_id}')
    return jsonify({'ok': True, 'session_id': session_id})


@app.route('/api/folder-jobs/<int:jid>/stop', methods=['POST'])
def stop_folder_job(jid):
    log.info(f'[job={jid}] STOP requested')
    db = get_db()
    db.execute('UPDATE folder_jobs SET enabled=0, status="paused" WHERE id=?', (jid,))
    db.commit()
    db.close()
    return jsonify({'ok': True})


@app.route('/api/folder-jobs/<int:jid>/resume', methods=['POST'])
def resume_folder_job(jid):
    log.info(f'[job={jid}] RESUME requested')
    db = get_db()
    db.execute('UPDATE folder_jobs SET enabled=1, status="idle" WHERE id=?', (jid,))
    db.commit()
    db.close()
    session_id = start_job_thread(jid)
    log.info(f'[job={jid}] RESUME → session_id={session_id}')
    return jsonify({'ok': True, 'session_id': session_id})


@app.route('/api/folder-jobs/<int:jid>/runs')
def folder_job_runs(jid):
    db   = get_db()
    rows = db.execute(
        'SELECT * FROM runs WHERE job_id=? ORDER BY started_at DESC LIMIT 100', (jid,)
    ).fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/folder-jobs/<int:jid>/actions')
def folder_job_actions(jid):
    db   = get_db()
    rows = db.execute(
        '''SELECT a.*, r.started_at as run_started
           FROM actions a
           JOIN runs r ON a.run_id = r.id
           WHERE r.job_id=?
           ORDER BY a.created_at DESC LIMIT 300''',
        (jid,),
    ).fetchall()
    db.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/folder-jobs/<int:jid>/progress')
def folder_job_progress(jid):
    db  = get_db()
    row = db.execute('SELECT * FROM folder_jobs WHERE id=?', (jid,)).fetchone()
    db.close()
    if not row:
        return jsonify({'error': 'Not found'}), 404
    sid = row['session_id'] if 'session_id' in row.keys() else None
    return jsonify({
        'session_id':      sid,
        'status':          row['status'],
        'total_processed': row['total_processed'],
        'total_remaining': row['total_remaining'],
    })


# ─────────────────────────────────────────────────────────────────────────────
# Routes — Containers & Cache
# ─────────────────────────────────────────────────────────────────────────────

@app.route('/api/containers')
def list_containers():
    workers, error = list_worker_containers()
    return jsonify({
        'docker_available': docker_available(),
        'workers':          workers,
        'error':            error,
    })


@app.route('/api/containers/<name>/logs')
def container_logs(name):
    """Return recent stdout/stderr from a named worker container."""
    if not re.match(r'^[a-zA-Z0-9_\-]+$', name):
        return jsonify({'error': 'Invalid container name'}), 400
    try:
        result = subprocess.run(
            ['docker', 'logs', '--tail', '300', '--timestamps', name],
            capture_output=True, text=True, timeout=10,
        )
        combined = (result.stdout + result.stderr).strip()
        return jsonify({'logs': combined or '(no output)', 'name': name})
    except subprocess.TimeoutExpired:
        return jsonify({'error': 'Timed out fetching logs'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ─────────────────────────────────────────────────────────────────────────────
# Routes — Accounts
# ─────────────────────────────────────────────────────────────────────────────

@app.route('/api/accounts', methods=['GET'])
def list_accounts():
    db   = get_db()
    rows = db.execute(
        'SELECT id, label, email, imap_host, imap_port, created_at FROM accounts ORDER BY created_at DESC'
    ).fetchall()
    db.close()
    result = []
    for r in rows:
        d = dict(r)
        d['has_password'] = get_credential_meta(f'app_password_{r["id"]}').get('set', False)
        result.append(d)
    return jsonify(result)


@app.route('/api/accounts', methods=['POST'])
def create_account():
    data  = request.json or {}
    email = data.get('email', '').strip()
    label = data.get('label', '').strip() or email
    if not email:
        return jsonify({'error': 'email required'}), 400
    db = get_db()
    cursor = db.execute(
        'INSERT INTO accounts (label, email, imap_host, imap_port, created_at) VALUES (?,?,?,?,?)',
        (label, email,
         data.get('imap_host', 'imap.mail.me.com'),
         int(data.get('imap_port', 993)),
         datetime.utcnow().isoformat()),
    )
    account_id = cursor.lastrowid
    db.commit()
    if data.get('app_password'):
        save_credential(f'app_password_{account_id}', data['app_password'])
    row = db.execute('SELECT * FROM accounts WHERE id=?', (account_id,)).fetchone()
    db.close()
    return jsonify(dict(row))


@app.route('/api/accounts/<int:aid>', methods=['PATCH'])
def update_account(aid):
    data = request.json or {}
    db   = get_db()
    for k in ('label', 'email', 'imap_host', 'imap_port'):
        if k in data:
            db.execute(f'UPDATE accounts SET {k}=? WHERE id=?', (data[k], aid))
    db.commit()
    if data.get('app_password'):
        save_credential(f'app_password_{aid}', data['app_password'])
    row = db.execute('SELECT * FROM accounts WHERE id=?', (aid,)).fetchone()
    db.close()
    return jsonify(dict(row) if row else {})


@app.route('/api/accounts/<int:aid>', methods=['DELETE'])
def delete_account(aid):
    db = get_db()
    db.execute('DELETE FROM accounts WHERE id=?', (aid,))
    db.commit()
    db.close()
    return jsonify({'ok': True})


@app.route('/api/cache/stats')
def cache_stats():
    return jsonify(get_cache_stats())


@app.route('/api/cache/clear', methods=['POST'])
def clear_cache_route():
    clear_cache()
    return jsonify({'ok': True})


@app.route('/api/data/clear', methods=['POST'])
def clear_all_data():
    db = get_db()
    for tbl in ('credentials', 'runs', 'actions', 'email_cache', 'schedules', 'folder_jobs'):
        db.execute(f'DELETE FROM {tbl}')
    db.commit()
    db.close()
    return jsonify({'ok': True})


# ─────────────────────────────────────────────────────────────────────────────
# Entrypoint
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    logging.basicConfig(
        level  = logging.INFO,
        format = '[%(asctime)s] %(levelname)-8s %(name)s  %(message)s',
        datefmt= '%Y-%m-%d %H:%M:%S',
        force  = True,
    )
    init_db()
    start_scheduler()
    recover_running_jobs()
    app.run(host='0.0.0.0', port=5000, debug=False)
