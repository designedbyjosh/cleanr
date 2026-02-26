"""
core.orchestrator — folder-job orchestration.

Each folder job runs as a persistent loop: launch a worker container,
wait for it to finish, then launch the next batch.  A per-job threading
lock ensures only one orchestrator runs at a time even if Flask restarts
race with manual start requests.
"""

import logging
import secrets
import threading
import time
from datetime import datetime

from .db import get_db, get_credential, get_setting, DB_PATH
from .docker_runner import (
    get_docker_client, launch_worker_container,
    poll_container_exit,
)
from .manifest import JobManifest

log = logging.getLogger('inbox')

_folder_job_threads: dict[int, threading.Thread] = {}
_folder_job_locks:   dict[int, threading.Lock]   = {}


# ── Internal helpers ──────────────────────────────────────────────────────────

def _emit_job_event(db, job_id, run_id, session_id, event, data) -> None:
    import json
    db.execute(
        'INSERT INTO job_events '
        '(job_id, run_id, session_id, event, data, created_at) VALUES (?,?,?,?,?,?)',
        (job_id, run_id, session_id, event,
         json.dumps(data), datetime.utcnow().isoformat()),
    )
    db.commit()


# ── Orchestrator ──────────────────────────────────────────────────────────────

def run_folder_job(job_id: int) -> None:
    """
    Background orchestrator thread for a single folder job.

    Loops indefinitely, launching a worker container per batch until the
    folder is empty, the job is paused, or an error occurs.
    """
    # Per-job mutex — prevents two concurrent orchestrators
    if job_id not in _folder_job_locks:
        _folder_job_locks[job_id] = threading.Lock()
    if not _folder_job_locks[job_id].acquire(blocking=False):
        log.warning(f'[job={job_id}] orchestrator lock busy — duplicate start ignored')
        return
    log.info(f'[job={job_id}] orchestrator thread started')

    db = get_db()

    def update_job(**kwargs):
        sets = ', '.join(f'{k}=?' for k in kwargs)
        db.execute(f'UPDATE folder_jobs SET {sets} WHERE id=?', (*kwargs.values(), job_id))
        db.commit()

    job = db.execute('SELECT * FROM folder_jobs WHERE id=?', (job_id,)).fetchone()
    if not job:
        log.error(f'[job={job_id}] job not found in DB — aborting')
        db.close()
        _folder_job_locks[job_id].release()
        return

    if not all([get_credential('email'), get_credential('app_password'),
                get_credential('api_key')]):
        log.error(f'[job={job_id}] missing credentials — aborting')
        update_job(status='error')
        db.close()
        _folder_job_locks[job_id].release()
        return

    session_id = f'folderjob_{job_id}_{secrets.token_hex(6)}'
    db.execute('UPDATE folder_jobs SET session_id=?, status="running" WHERE id=?',
               (session_id, job_id))
    db.commit()
    log.info(
        f'[job={job_id}] session={session_id}  '
        f'folder={job["folder"]!r}  batch_size={job["batch_size"]}'
    )

    parallel = int(get_setting('parallel_batches', 3) or 3)

    try:
        while True:
            # ── Wait for orphaned containers from a dead orchestrator ─────────
            poll_client = get_docker_client()
            if poll_client:
                try:
                    orphans = poll_client.containers.list(
                        filters={'name': f'inbox-worker-{job_id}-'}
                    )
                    for oc in orphans:
                        log.warning(
                            f'[job={job_id}] orphan container {oc.name!r} '
                            f'(status={oc.status}) — waiting'
                        )
                        _emit_job_event(db, job_id, None, session_id, 'status',
                                        {'msg': f'Waiting for existing batch ({oc.name})…'})
                        while True:
                            try:
                                oc.reload()
                                if oc.status in ('exited', 'dead'):
                                    log.info(f'[job={job_id}] orphan {oc.name!r} done — removing')
                                    try:
                                        oc.remove()
                                    except Exception:
                                        pass
                                    break
                            except Exception:
                                log.info(f'[job={job_id}] orphan {oc.name!r} gone')
                                break
                            time.sleep(3)
                except Exception:
                    pass

            # ── Pause / stop check ────────────────────────────────────────────
            fresh = db.execute('SELECT * FROM folder_jobs WHERE id=?', (job_id,)).fetchone()
            if not fresh or not fresh['enabled']:
                log.info(f'[job={job_id}] job disabled — pausing orchestrator')
                _emit_job_event(db, job_id, None, session_id, 'status',
                                {'msg': 'Job paused.'})
                update_job(status='paused')
                break

            # ── Create run record ─────────────────────────────────────────────
            cursor = db.execute(
                'INSERT INTO runs (started_at, status, run_type, source_folder, job_id) '
                'VALUES (?, "running", "folder_job", ?, ?)',
                (datetime.utcnow().isoformat(), job['folder'], job_id),
            )
            run_id = cursor.lastrowid
            db.commit()
            log.info(f'[job={job_id}] created run_id={run_id}  folder={job["folder"]!r}')

            # ── Build manifest ────────────────────────────────────────────────
            manifest = JobManifest.from_folder_job(
                dict(fresh), run_id, session_id,
                parallel_batches=parallel, db_path=DB_PATH,
            )

            # ── Record container start ────────────────────────────────────────
            container_name = f'inbox-worker-{job_id}-{run_id}'
            db.execute(
                'INSERT INTO worker_containers '
                '(job_id, run_id, container_name, status, created_at) VALUES (?,?,?,?,?)',
                (job_id, run_id, container_name, 'starting',
                 datetime.utcnow().isoformat()),
            )
            db.commit()

            # ── Launch ────────────────────────────────────────────────────────
            try:
                container = launch_worker_container(manifest, container_name)
                db.execute(
                    'UPDATE worker_containers SET container_id=?, status="running" '
                    'WHERE job_id=? AND run_id=?',
                    (container.id, job_id, run_id),
                )
                db.commit()
            except Exception as launch_err:
                log.error(f'[job={job_id}] [run={run_id}] launch failed: {launch_err}')
                db.execute('UPDATE runs SET status="error", finished_at=? WHERE id=?',
                           (datetime.utcnow().isoformat(), run_id))
                db.execute(
                    'UPDATE worker_containers SET status="error", finished_at=? '
                    'WHERE job_id=? AND run_id=?',
                    (datetime.utcnow().isoformat(), job_id, run_id),
                )
                db.commit()
                _emit_job_event(db, job_id, run_id, session_id, 'error', {
                    'code':        'LAUNCH_FAILED',
                    'message':     str(launch_err),
                    'remediation': 'Docker may be unavailable or the image may need rebuilding.',
                })
                update_job(status='error')
                break

            # ── Poll until container exits ────────────────────────────────────
            exit_code = poll_container_exit(
                container, job_id, run_id, session_id, db,
                update_job, _emit_job_event,
            )
            if exit_code is None:
                log.info(f'[job={job_id}] [run={run_id}] paused while container still running')
                return

            container_status = 'done' if exit_code == 0 else 'error'
            db.execute(
                'UPDATE worker_containers SET status=?, finished_at=? '
                'WHERE job_id=? AND run_id=?',
                (container_status, datetime.utcnow().isoformat(), job_id, run_id),
            )
            db.commit()

            # ── Remove container ──────────────────────────────────────────────
            try:
                container.remove()
                log.info(f'[job={job_id}] [run={run_id}] container removed')
            except Exception as rm_err:
                log.warning(f'[job={job_id}] [run={run_id}] container remove failed: {rm_err}')

            # ── Read results ──────────────────────────────────────────────────
            run = db.execute('SELECT * FROM runs WHERE id=?', (run_id,)).fetchone()

            if exit_code != 0:
                log.error(
                    f'[job={job_id}] [run={run_id}] worker exited code={exit_code} — error'
                )
                update_job(status='error')
                break

            if run:
                log.info(
                    f'[job={job_id}] [run={run_id}] batch done  '
                    f'total={run["total"] or 0}  kept={run["kept"] or 0}  '
                    f'filed={run["filed"] or 0}  trashed={run["trashed"] or 0}  '
                    f'errors={run["errors"] or 0}'
                )

            if run and (run['total'] or 0) == 0:
                log.info(f'[job={job_id}] folder {job["folder"]!r} fully processed — complete')
                update_job(
                    status='completed',
                    completed_at=datetime.utcnow().isoformat(),
                    total_remaining=0,
                )
                _emit_job_event(db, job_id, run_id, session_id, 'done', {
                    'msg':             f'✓ {job["folder"]} fully processed!',
                    'total_processed': (fresh or job)['total_processed'],
                })
                break

            # ── Inter-batch delay ─────────────────────────────────────────────
            batch_delay = int(get_setting('batch_delay_seconds', 5) or 5)
            log.info(f'[job={job_id}] inter-batch delay {batch_delay}s')
            for _ in range(batch_delay):
                fr = db.execute(
                    'SELECT enabled FROM folder_jobs WHERE id=?', (job_id,)
                ).fetchone()
                if not fr or not fr['enabled']:
                    break
                time.sleep(1)

    except Exception as e:
        log.exception(f'[job={job_id}] unexpected error in orchestrator: {e}')
        _emit_job_event(db, job_id, None, session_id, 'error', {
            'code':        'FATAL',
            'message':     str(e),
            'remediation': 'Check credentials and folder name.',
        })
        update_job(status='error')
    finally:
        log.info(f'[job={job_id}] orchestrator thread exiting')
        db.close()
        _folder_job_threads.pop(job_id, None)
        try:
            _folder_job_locks[job_id].release()
        except Exception:
            pass


# ── Thread management ─────────────────────────────────────────────────────────

def start_job_thread(jid: int) -> str | None:
    """
    Start the orchestrator thread for *jid* if it is not already running.
    Waits up to ~3 s for the orchestrator to write session_id then returns it.
    """
    if jid not in _folder_job_threads or not _folder_job_threads[jid].is_alive():
        log.info(f'[job={jid}] starting orchestrator thread')
        t = threading.Thread(target=run_folder_job, args=(jid,), daemon=True)
        _folder_job_threads[jid] = t
        t.start()
        for _ in range(30):   # up to ~3 s
            time.sleep(0.1)
            db  = get_db()
            row = db.execute(
                'SELECT session_id, status FROM folder_jobs WHERE id=?', (jid,)
            ).fetchone()
            db.close()
            if row and row['session_id'] and row['status'] == 'running':
                log.info(f'[job={jid}] orchestrator ready  session_id={row["session_id"]}')
                return row['session_id']
        log.warning(f'[job={jid}] orchestrator did not reach running state within 3 s')
    else:
        log.info(f'[job={jid}] orchestrator already alive — returning existing session_id')

    db  = get_db()
    row = db.execute('SELECT session_id FROM folder_jobs WHERE id=?', (jid,)).fetchone()
    db.close()
    return row['session_id'] if row else None


# ── Recovery on Flask boot ────────────────────────────────────────────────────

def recover_running_jobs() -> None:
    """
    Called at startup.  Any folder job that was 'running' when Flask last died
    is picked back up in a background thread.  Live containers from the previous
    Flask instance are polled until they finish before a new orchestrator starts.
    """
    def _do_recover():
        time.sleep(3)
        log.info('Recovery scan starting…')
        client = get_docker_client()
        db     = get_db()
        try:
            running = db.execute(
                "SELECT id FROM folder_jobs WHERE status='running' AND enabled=1"
            ).fetchall()
            log.info(f'Recovery: {len(running)} job(s) in running state')
            for row in running:
                jid = row['id']
                if client:
                    try:
                        active = client.containers.list(
                            filters={'name': f'inbox-worker-{jid}-'}
                        )
                        log.info(f'Recovery [job={jid}]: {len(active)} live container(s)')
                        for c in active:
                            log.info(
                                f'Recovery [job={jid}]: waiting on '
                                f'{c.name!r} (status={c.status!r})'
                            )
                            while True:
                                try:
                                    c.reload()
                                    if c.status in ('exited', 'dead'):
                                        log.info(f'Recovery [job={jid}]: {c.name!r} done — removing')
                                        try:
                                            c.remove()
                                        except Exception:
                                            pass
                                        break
                                except Exception:
                                    log.info(f'Recovery [job={jid}]: container gone')
                                    break
                                time.sleep(3)
                    except Exception as e:
                        log.warning(f'Recovery [job={jid}]: container list error: {e}')

                if jid not in _folder_job_threads or not _folder_job_threads[jid].is_alive():
                    log.info(f'Recovery [job={jid}]: restarting orchestrator')
                    t = threading.Thread(target=run_folder_job, args=(jid,), daemon=True)
                    _folder_job_threads[jid] = t
                    t.start()
                else:
                    log.info(f'Recovery [job={jid}]: orchestrator already alive')
            log.info('Recovery complete')
        finally:
            db.close()

    threading.Thread(target=_do_recover, daemon=True).start()
