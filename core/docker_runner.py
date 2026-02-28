"""
core.docker_runner — sibling-container launch and lifecycle management.

Uses Docker-outside-Docker (DooD) by mounting /var/run/docker.sock.
Self-inspects the Flask container once to discover the correct image,
/data volume and network so worker containers inherit the same config
without any hardcoding.
"""

import logging
import os
import time
from typing import Optional

log = logging.getLogger('inbox')

try:
    import docker as _docker
    _DOCKER_AVAILABLE = True
except ImportError:
    _DOCKER_AVAILABLE = False

_docker_client = None   # cached Docker client
_worker_cfg    = None   # cached self-inspect result


# ── Client ────────────────────────────────────────────────────────────────────

def get_docker_client():
    """Return a cached Docker SDK client, or None if unavailable."""
    global _docker_client
    if not _DOCKER_AVAILABLE:
        return None
    if _docker_client is None:
        try:
            _docker_client = _docker.from_env()
        except Exception as e:
            log.warning(f'Docker client unavailable: {e}')
    return _docker_client


def docker_available() -> bool:
    return _DOCKER_AVAILABLE and get_docker_client() is not None


# ── Self-inspect ──────────────────────────────────────────────────────────────

def _find_self_container(client):
    """
    Try to find the running Flask container object via multiple strategies:
      1. Direct lookup by HOSTNAME (the short container ID Docker sets by default).
      2. Scan all running containers for one whose ID starts with HOSTNAME.
      3. Scan all running containers for one that has /var/run/docker.sock mounted
         AND /data mounted (i.e. looks like us) as a last resort.
    Returns the container object or None.
    """
    hostname = os.environ.get('HOSTNAME', '')

    # Strategy 1 — direct get by ID/name
    if hostname:
        try:
            return client.containers.get(hostname)
        except Exception:
            pass

    # Strategy 2 — scan for ID prefix match
    if hostname:
        try:
            for c in client.containers.list():
                if c.id.startswith(hostname) or c.short_id == hostname:
                    log.info(f'Docker self-inspect: found self via ID-prefix scan ({c.name!r})')
                    return c
        except Exception:
            pass

    # Strategy 3 — find the container that has /var/run/docker.sock AND /data mounted
    try:
        for c in client.containers.list():
            mounts = c.attrs.get('Mounts', [])
            has_sock  = any(m.get('Source') == '/var/run/docker.sock' for m in mounts)
            has_data  = any(m.get('Destination') == '/data' for m in mounts)
            if has_sock and has_data:
                log.info(f'Docker self-inspect: found self via mount-signature scan ({c.name!r})')
                return c
    except Exception:
        pass

    return None


def get_worker_cfg() -> dict:
    """
    Inspect the running Flask container once to discover:
      - Docker image name   (same image is used for worker containers)
      - Named volume for /data (so workers share the SQLite DB)
      - Network name        (so workers can reach the same bridge)
    Result is cached for the lifetime of the process.
    """
    global _worker_cfg
    if _worker_cfg is not None:
        return _worker_cfg

    client = get_docker_client()
    if client is None:
        _worker_cfg = {}
        return _worker_cfg

    self_c = _find_self_container(client)
    if self_c is None:
        log.warning(
            f'Docker self-inspect failed: could not locate self container '
            f'(HOSTNAME={os.environ.get("HOSTNAME", "")!r})'
        )
        _worker_cfg = {}
        return _worker_cfg

    try:
        image   = self_c.attrs['Config']['Image']

        volumes = {}
        for m in self_c.attrs['Mounts']:
            if m['Destination'] == '/data':
                if m['Type'] == 'volume':
                    volumes[m['Name']] = {'bind': '/data', 'mode': 'rw'}
                else:
                    volumes[m['Source']] = {'bind': '/data', 'mode': 'rw'}

        nets    = list(self_c.attrs['NetworkSettings']['Networks'].keys())
        network = nets[0] if nets else 'bridge'

        _worker_cfg = {'image': image, 'volumes': volumes, 'network': network}
        log.info(
            f'Docker self-inspect: image={image!r}  '
            f'network={network!r}  volumes={list(volumes.keys())}'
        )
    except Exception as e:
        log.warning(f'Docker self-inspect failed: {e}')
        _worker_cfg = {}

    return _worker_cfg


# ── Launch ────────────────────────────────────────────────────────────────────

def launch_worker_container(manifest, container_name: str):
    """
    Spin up a short-lived sibling container to execute one manifest.

    The entire manifest is passed as the MANIFEST env var (JSON) so that
    no credential data and no magic positional arguments flow through env.

    Returns the Docker container object.
    Raises RuntimeError if Docker is unavailable.
    """
    client = get_docker_client()
    if client is None:
        raise RuntimeError('Docker is not available — cannot launch worker container.')

    cfg     = get_worker_cfg()
    # WORKER_IMAGE env var is the explicit override (set in production docker-compose).
    # Falls back to the self-inspected image, then to the local-dev compose name.
    image   = (os.environ.get('WORKER_IMAGE')
               or cfg.get('image', 'inbox-cleaner-v2-inbox-cleaner:latest'))
    volumes = cfg.get('volumes', {})
    network = cfg.get('network', 'bridge')

    log.info(
        f'Launching container {container_name!r}  '
        f'image={image!r}  job_type={manifest.job_type!r}  '
        f'folder={manifest.folder!r}  batch={manifest.batch_size}'
    )

    # Remove stale container with the same name
    try:
        old = client.containers.get(container_name)
        old.remove(force=True)
        log.warning(f'Removed stale container {container_name!r} before launch')
    except Exception:
        pass

    container = client.containers.run(
        image       = image,
        command     = ['python', '/app/worker.py'],
        name        = container_name,
        environment = {
            'MANIFEST': manifest.to_json(),
            'DB_PATH':  manifest.db_path,
        },
        volumes     = volumes,
        network     = network,
        detach      = True,
        remove      = False,    # orchestrator inspects exit code before removing
    )
    log.info(f'Container {container_name!r} started  id={container.short_id}')
    return container


# ── Lifecycle polling ─────────────────────────────────────────────────────────

def poll_container_exit(
    container,
    job_id:     int,
    run_id:     int,
    session_id: str,
    db,
    update_job_fn,
    emit_event_fn,
) -> Optional[int]:
    """
    Poll a worker container every 3 s until it exits.

    Returns the integer exit code, or None if the job was paused while waiting.
    Uses polling instead of container.wait() which fails on long-lived
    unix-socket HTTP connections (connection resets / idle timeouts).
    """
    client  = get_docker_client()
    poll_n  = 0

    while True:
        poll_n += 1
        try:
            c = client.containers.get(container.id)
            if c.status in ('exited', 'dead'):
                exit_code = c.attrs['State']['ExitCode']
                log.info(
                    f'[job={job_id}] [run={run_id}] container exited  '
                    f'code={exit_code}  polls={poll_n}'
                )
                return exit_code

            if poll_n == 1 or poll_n % 10 == 0:
                log.info(
                    f'[job={job_id}] [run={run_id}] container running  '
                    f'status={c.status!r}  poll={poll_n}'
                )
        except Exception as e:
            if 'NotFound' in type(e).__name__:
                log.info(
                    f'[job={job_id}] [run={run_id}] '
                    f'container gone (self-removed) — treating as exit 0'
                )
                return 0
            log.warning(f'[job={job_id}] [run={run_id}] Docker API error (retrying): {e}')

        # Check for pause signal
        fr = db.execute(
            'SELECT enabled FROM folder_jobs WHERE id=?', (job_id,)
        ).fetchone()
        if not fr or not fr['enabled']:
            log.info(f'[job={job_id}] [run={run_id}] pause signal detected — stopping poll')
            emit_event_fn(db, job_id, run_id, session_id,
                          'status', {'msg': 'Job paused — waiting for batch to finish.'})
            update_job_fn(status='paused')
            return None

        time.sleep(3)


# ── Status query ──────────────────────────────────────────────────────────────

def list_worker_containers() -> tuple[list[dict], Optional[str]]:
    """Return (workers, error_or_None) for the /api/containers endpoint."""
    client = get_docker_client()
    if client is None:
        err = 'Docker SDK not available' if not _DOCKER_AVAILABLE else 'Docker daemon unreachable'
        return [], err

    workers = []
    try:
        live = client.containers.list(all=False, filters={'name': 'inbox-worker-'})
        for c in live:
            parts      = c.name.split('-')           # inbox-worker-{job_id}-{run_id}
            job_id_val = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else None
            run_id_val = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else None
            workers.append({
                'id':      c.short_id,
                'name':    c.name,
                'status':  c.status,
                'job_id':  job_id_val,
                'run_id':  run_id_val,
                'started': c.attrs.get('State', {}).get('StartedAt', ''),
            })
        return workers, None
    except Exception as e:
        return [], str(e)
