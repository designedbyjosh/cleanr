"""
core.scheduler — time-based job scheduler.

Schedules are stored in the `schedules` table.  When a schedule fires,
a worker container is launched via Docker (same mechanism as folder jobs)
so the work runs outside the Flask process and survives restarts.
"""

import logging
import secrets
import threading
import time
from datetime import datetime, timedelta

from .db import get_db, get_credential, get_setting, DB_PATH
from .docker_runner import launch_worker_container
from .manifest import JobManifest

log = logging.getLogger('inbox')

_scheduler_thread: threading.Thread | None = None


def _fire_schedule(db, sched: dict, now: datetime) -> None:
    """Create a run record and launch a worker container for one schedule firing."""
    ea = get_credential('email')
    ap = get_credential('app_password')
    ak = get_credential('api_key')
    if not all([ea, ap, ak]):
        log.warning(f'Scheduler: skipping schedule id={sched["id"]} — credentials missing')
        return

    cursor = db.execute(
        'INSERT INTO runs (started_at, status, run_type, source_folder) '
        'VALUES (?, "running", "scheduled", ?)',
        (now.isoformat(), sched['folder'] if sched['folder'] else 'INBOX'),
    )
    run_id = cursor.lastrowid
    db.commit()

    session_id = f'sched_{run_id}_{secrets.token_hex(6)}'
    parallel   = int(get_setting('parallel_batches', 3) or 3)

    manifest = JobManifest.from_schedule(
        dict(sched), run_id, session_id,
        limit            = sched['limit_per_run'],
        parallel_batches = parallel,
        db_path          = DB_PATH,
    )

    container_name = f'inbox-sched-{sched["id"]}-{run_id}'
    log.info(
        f'Scheduler: firing schedule id={sched["id"]} name={sched["name"]!r}  '
        f'run_id={run_id}  folder={manifest.folder!r}'
    )

    try:
        launch_worker_container(manifest, container_name)
    except Exception as e:
        log.error(f'Scheduler: failed to launch container for schedule {sched["id"]}: {e}')
        db.execute(
            'UPDATE runs SET status="error", finished_at=? WHERE id=?',
            (datetime.utcnow().isoformat(), run_id),
        )
        db.commit()


def _sched_delta(sched) -> timedelta:
    """Return the interval timedelta for a schedule, preferring interval_minutes if set."""
    try:
        mins = sched['interval_minutes']
    except (KeyError, IndexError):
        mins = None
    if mins:
        return timedelta(minutes=int(mins))
    return timedelta(hours=int(sched['interval_hours']))


def scheduler_loop() -> None:
    log.info('Scheduler started')
    while True:
        try:
            db  = get_db()
            now = datetime.utcnow()
            for sched in db.execute('SELECT * FROM schedules WHERE enabled = 1').fetchall():
                next_run = sched['next_run']
                if not next_run:
                    nr = (now + _sched_delta(sched)).isoformat()
                    db.execute('UPDATE schedules SET next_run=? WHERE id=?', (nr, sched['id']))
                    db.commit()
                    continue

                if datetime.fromisoformat(next_run) <= now:
                    _fire_schedule(db, sched, now)
                    nr = (now + _sched_delta(sched)).isoformat()
                    db.execute(
                        'UPDATE schedules SET next_run=?, last_run=? WHERE id=?',
                        (nr, now.isoformat(), sched['id']),
                    )
                    db.commit()

            db.close()
        except Exception as e:
            log.warning(f'Scheduler loop error: {e}')
        time.sleep(60)


def start_scheduler() -> None:
    global _scheduler_thread
    if _scheduler_thread is None or not _scheduler_thread.is_alive():
        _scheduler_thread = threading.Thread(target=scheduler_loop, daemon=True)
        _scheduler_thread.start()
