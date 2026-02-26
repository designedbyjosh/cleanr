# 📬 Inbox Cleaner

**AI-powered iCloud Mail organiser that runs entirely on your own machine.**

Inbox Cleaner connects to your iCloud mailbox over IMAP, classifies every email with Claude, and automatically files, archives or deletes the noise — while leaving anything important (and anything you've flagged ⭐) exactly where it is.

---

## Features

- **One-click manual clean** — process your inbox on demand and watch results stream in real time
- **Scheduled automation** — set up recurring runs (hourly, daily, etc.) that fire in the background
- **Folder jobs** — bulk-process any IMAP folder, batch by batch, resuming where it left off
- **Flagged-email protection** — starred / flagged emails are never touched, regardless of settings
- **Unread marketing deletion** — optionally delete unread promotional mail (off by default)
- **Custom prompt** — add your own guidance to every classification prompt (sanitised against injection)
- **Deduplication cache** — emails with the same sender + subject are classified once and cached
- **Folder AI suggestion** — ask Claude to suggest a cleaner folder hierarchy for your mailbox
- **Full audit trail** — every action is recorded in SQLite; the UI shows history, stats and charts
- **Isolated workers** — each job runs as an ephemeral Docker sibling container; crashes never affect the UI

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Docker host                              │
│                                                                 │
│  ┌──────────────────────────────┐                               │
│  │  inbox-cleaner  (Flask)      │  :5050                        │
│  │                              │                               │
│  │  app.py          ← routes    │                               │
│  │  core/                       │                               │
│  │    db.py         ← SQLite    │                               │
│  │    imap.py       ← IMAP      │    imap.mail.me.com:993       │
│  │    classifier.py ← Claude ───┼──────────────────────────►   │
│  │    apply.py      ← IMAP ops  │                               │
│  │    manifest.py   ← config    │                               │
│  │    orchestrator.py           │    Docker socket              │
│  │    scheduler.py  ────────────┼──────────┐                    │
│  └──────────────────────────────┘          │                    │
│                                            ▼                    │
│              ┌──────────────────────────────────┐               │
│              │  worker  (ephemeral container)   │               │
│              │  worker.py  ← JobManifest (env)  │               │
│              │    fetch → classify → apply       │               │
│              └──────────────────────────────────┘               │
│                                                                 │
│  Named volume: inbox_data  →  /data/inbox_cleaner.db            │
└─────────────────────────────────────────────────────────────────┘
```

**Key design decisions:**

| Concern | Approach |
|---|---|
| Worker isolation | Each job runs in a short-lived sibling container; the Flask process never blocks |
| Configuration | `JobManifest` dataclass serialised to JSON, passed as a single `MANIFEST` env var |
| Credentials | Stored in SQLite on the Docker volume; never passed as individual env vars to workers |
| Prompt safety | `sanitise_custom_prompt()` strips injection patterns and caps length at 500 chars |
| Email safety | Flagged emails are filtered at fetch time *and* again during apply |
| Deduplication | SHA-256 hash of `sender + normalised subject` keyed in `email_cache` table |

---

## Quick Start

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) with Compose
- An [iCloud app-specific password](https://support.apple.com/en-us/102654) (not your Apple ID password)
- An [Anthropic API key](https://console.anthropic.com/)

### Run

```bash
git clone https://github.com/yourusername/inbox-cleaner.git
cd inbox-cleaner
docker compose up -d
```

Open **http://localhost:5050**, go to **Settings**, enter your credentials, then hit **Run Cleanup**.

---

## Project Structure

```
inbox-cleaner/
├── app.py               # Flask routes (thin layer — no business logic)
├── worker.py            # Generic batch worker (runs inside sibling containers)
├── main.py              # Entrypoint: init DB → scheduler → Flask
├── core/
│   ├── db.py            # SQLite schema, migrations, credentials, settings
│   ├── imap.py          # IMAP connection, fetch, move, delete, folder management
│   ├── classifier.py    # Claude integration, batch classification, prompts
│   ├── apply.py         # Apply classifications to IMAP (move / delete / keep)
│   ├── cache.py         # Email deduplication cache
│   ├── manifest.py      # JobManifest dataclass + prompt-injection sanitiser
│   ├── docker_runner.py # Launch / poll worker containers via Docker SDK
│   ├── orchestrator.py  # Folder-job thread management and container polling
│   └── scheduler.py     # Time-based job scheduler (fires containerised workers)
├── templates/
│   └── index.html       # Single-page UI
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

---

## Configuration

All settings are managed through the web UI at **Settings**.

### Credentials

| Field | Description |
|---|---|
| iCloud email | Your `@icloud.com` or custom domain address |
| App-specific password | Generated at [appleid.apple.com](https://appleid.apple.com) — **not** your Apple ID password |
| Anthropic API key | From [console.anthropic.com](https://console.anthropic.com) |

> Only email metadata (sender, subject, date) is sent to Anthropic — never email bodies.

### Global Settings

| Key | Default | Description |
|---|---|---|
| `rate_limit_per_hour` | 200 | Maximum IMAP operations per hour |
| `batch_delay_seconds` | 5 | Pause between classification batches |
| `inbox_zero_mode` | on | Process oldest emails first |
| `default_limit` | 50 | Default number of emails per manual run |
| `parallel_batches` | 3 | Concurrent Claude classification batches |
| `cache_ttl_days` | 30 | How long to cache email classifications |

---

## Job Parameters

Every job type (manual run, folder job, schedule) accepts these parameters:

| Parameter | Type | Default | Description |
|---|---|---|---|
| `custom_prompt` | string | — | Extra guidance appended to the classification prompt |
| `skip_flagged` | bool | `true` | Never process flagged / starred emails |
| `delete_marketing_unread` | bool | `false` | Delete marketing emails even when unread |
| `aggressive_trash` | bool | `false` | Lean towards deleting borderline emails |
| `start_from_days_ago` | int | — | Only process emails from the last N days |

### Folder Jobs (additional)

| Parameter | Type | Default | Description |
|---|---|---|---|
| `batch_size` | int | 20 | Emails per container batch |
| `oldest_first` | bool | `true` | Process oldest emails first |
| `max_emails` | int | — | Hard cap on total emails processed per run |

---

## How Classification Works

Each email is sent to Claude with its sender, subject and date. Claude returns one of:

| Action | Meaning |
|---|---|
| `keep` | Leave in place — personal, important or ambiguous |
| `archive` | Move to `Archive` — read, low value |
| `file` | Move to a named folder — newsletters, receipts, etc. |
| `trash` | Delete — spam, marketing, automated noise |

**Safety gates applied regardless of AI output:**
- Flagged / starred emails → always skipped (double-checked at fetch *and* apply)
- Unread emails → only deleted if `delete_marketing_unread = true` and action is `trash`
- Cache hit → skip re-classification within TTL window

---

## API Reference

<details>
<summary>Credentials & Settings</summary>

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/credentials` | Check which credentials are configured |
| `POST` | `/api/credentials` | Save credentials |
| `GET` | `/api/settings` | Get all settings |
| `POST` | `/api/settings` | Update settings |
</details>

<details>
<summary>Manual Runs</summary>

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/run` | Start a manual inbox cleanup |
| `GET` | `/api/progress/:session_id` | SSE stream of run events |
| `GET` | `/api/runs` | Recent run history |
| `GET` | `/api/runs/:id/actions` | Actions taken in a specific run |
| `GET` | `/api/stats` | Aggregate stats + 14-day chart data |

**`POST /api/run` body:**
```json
{
  "limit": 50,
  "source_folder": "INBOX",
  "custom_prompt": "Be extra strict with newsletters.",
  "skip_flagged": true,
  "delete_marketing_unread": false,
  "aggressive_trash": false,
  "start_from_days_ago": 7
}
```
</details>

<details>
<summary>Folder Jobs</summary>

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/folder-jobs` | List all folder jobs |
| `POST` | `/api/folder-jobs` | Create a folder job |
| `PATCH` | `/api/folder-jobs/:id` | Update a folder job |
| `DELETE` | `/api/folder-jobs/:id` | Delete a folder job |
| `POST` | `/api/folder-jobs/:id/start` | Start / resume processing |
| `POST` | `/api/folder-jobs/:id/stop` | Pause processing |
| `GET` | `/api/folder-jobs/:id/runs` | Run history for this job |
| `GET` | `/api/folder-jobs/:id/actions` | Actions taken by this job |
| `GET` | `/api/folder-jobs/:id/progress` | Current progress and session ID |
</details>

<details>
<summary>Schedules</summary>

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/schedules` | List all schedules |
| `POST` | `/api/schedules` | Create a schedule |
| `PATCH` | `/api/schedules/:id` | Update a schedule |
| `DELETE` | `/api/schedules/:id` | Delete a schedule |

**`POST /api/schedules` body:**
```json
{
  "name": "Nightly cleanup",
  "enabled": 1,
  "interval_hours": 24,
  "limit_per_run": 100,
  "folder": "INBOX",
  "custom_prompt": "",
  "delete_marketing_unread": 0,
  "skip_flagged": 1
}
```
</details>

<details>
<summary>Folders</summary>

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/folders` | List IMAP folders (`?counts=true` for email counts) |
| `POST` | `/api/folders/create` | Create a new folder |
| `POST` | `/api/folders/rename` | Rename a folder |
| `POST` | `/api/folders/suggest` | Ask Claude to suggest folder reorganisation |
| `POST` | `/api/folders/apply` | Apply a set of folder create / rename operations |
</details>

<details>
<summary>Cache & Data</summary>

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/cache/stats` | Active cache entry count |
| `POST` | `/api/cache/clear` | Clear the classification cache |
| `POST` | `/api/data/clear` | Wipe all data (credentials, runs, cache) |
| `GET` | `/api/containers` | List active worker containers |
</details>

---

## Development

```bash
# Build and start
docker compose up --build

# Tail logs (structured, timestamped)
docker compose logs -f

# Shell into the running container
docker compose exec inbox-cleaner bash

# Full rebuild from scratch
docker compose down && docker compose up --build
```

The SQLite database lives in a named Docker volume (`inbox_data`) and persists across container rebuilds.

---

## Security Notes

- Credentials are stored in SQLite inside the Docker volume — keep that volume and port 5050 private
- The Docker socket (`/var/run/docker.sock`) is mounted so the app can launch worker containers
- Custom prompts are sanitised to strip known prompt-injection patterns before being embedded in AI prompts
- Flagged emails are checked at both fetch time and apply time to ensure they are never modified

---

## License

MIT
