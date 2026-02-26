#!/usr/bin/env python3
"""
Application entrypoint.

Initialises the database, starts the background scheduler and job-recovery
thread, then hands off to Flask.  The Dockerfile CMD points here so that
the container starts cleanly without an inline python -c command.
"""

import logging

logging.basicConfig(
    level   = logging.INFO,
    format  = '[%(asctime)s] %(levelname)-8s %(name)s  %(message)s',
    datefmt = '%Y-%m-%d %H:%M:%S',
    force   = True,
)

from core.db import init_db
from core.scheduler import start_scheduler
from core.orchestrator import recover_running_jobs
from app import app

if __name__ == '__main__':
    init_db()
    start_scheduler()
    recover_running_jobs()
    app.run(host='0.0.0.0', port=5000, debug=False)
