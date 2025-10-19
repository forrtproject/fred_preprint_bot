# 🧠 OSF Preprints → Postgres → PDFs → GROBID (TEI)

End-to-end pipeline to **ingest OSF preprints**, store metadata in **PostgreSQL**, **download primary PDFs**, and extract structured metadata using **GROBID** to produce TEI XML — all running inside **Docker Compose** with **Celery workers**.

---

## 📦 Prerequisites

- Docker Desktop (allocate **≥ 4 GB RAM** under Settings → Resources)
- Git + Bash (or PowerShell)
- A valid **OSF API Token** — store this in your `.env`

---

## 🧩 Project Structure

```
.
├─ osf_sync/
│  ├─ celery_app.py           # Celery configuration & beat schedule
│  ├─ tasks.py                # Celery tasks (sync, PDF, GROBID)
│  ├─ db.py                   # Postgres schema & connection
│  ├─ upsert.py               # Incremental upsert logic
│  ├─ iter_preprints.py       # Paged OSF API iterator
│  ├─ cli.py                  # Command-line entrypoints
│  └─ ...
├─ scripts/
│  └─ wait_for_grobid.py      # Helper to wait until GROBID is ready
├─ data/                      # PDFs and TEI XML (mounted as /data)
├─ docker-compose.yml
├─ .env                       # Environment configuration
└─ README.md
```

---

## ⚙️ Environment Setup

Create a `.env` file next to `docker-compose.yml`:

```dotenv
# PostgreSQL
POSTGRES_DB=osfdb
POSTGRES_USER=osf
POSTGRES_PASSWORD=osfpass

# SQLAlchemy connection
DATABASE_URL=postgresql+psycopg://osf:osfpass@postgres:5432/osfdb

# Celery broker & backend
CELERY_BROKER_URL=redis://redis:6379/0
CELERY_RESULT_BACKEND=redis://redis:6379/0

# OSF API Token (required)
OSF_API_TOKEN=your_osf_token_here

# Data mount path
PDF_DEST_ROOT=/data/preprints

# GROBID service URL (inside docker network)
GROBID_URL=http://grobid:8070

TZ=Europe/Berlin
```

---

## 🚀 Build & Start Containers

### Build images

```bash
docker compose build
```

### Start everything

```bash
docker compose up -d
```

✅ Starts:

- Postgres + Redis
- App container
- Celery workers (`celery-worker`, `celery-pdf`, `celery-grobid`)
- Celery beat scheduler
- Flower dashboard (optional)
- GROBID service

---

## 🔄 Fetch Preprints from OSF

Fetch and upsert preprints from a start date **to now**:

```bash
docker compose run --rm app python -m osf_sync.cli sync-from-date --start 2025-07-01
```

Optionally filter by subject:

```bash
docker compose run --rm app python -m osf_sync.cli sync-from-date --start 2025-07-01 --subject Psychology
```

🧩 This:

- Pulls preprints from the OSF API
- Inserts new ones
- Updates changed ones (no duplicates)

---

## 📥 Download PDFs

Queue a batch of missing PDFs (downloads one at a time):

```bash
docker compose run --rm app python -m osf_sync.cli enqueue-pdf --limit 50
```

Check progress:

```bash
docker compose logs -f celery-pdf
```

📂 Saved files:

```
./data/preprints/<osf_id>/file.pdf
```

---

## 🧾 Run GROBID on PDFs → TEI XML

Queue GROBID parsing for all downloaded PDFs that lack XML:

```bash
docker compose run --rm app python -m osf_sync.cli enqueue-grobid --limit 25
```

Monitor:

```bash
docker compose logs -f celery-grobid
```

📂 Output:

```
./data/preprints/<osf_id>/tei.xml
```

---

## 🧰 Common Commands

| Action                      | Command                                             |
| --------------------------- | --------------------------------------------------- |
| **Build containers**        | `docker compose build`                              |
| **Start stack**             | `docker compose up -d`                              |
| **Enter app shell**         | `docker compose run --rm app bash`                  |
| **View logs**               | `docker compose logs -f celery-worker`              |
| **View PDF worker logs**    | `docker compose logs -f celery-pdf`                 |
| **View GROBID worker logs** | `docker compose logs -f celery-grobid`              |
| **View Flower UI**          | Open [http://localhost:5555](http://localhost:5555) |

---

## 🧠 Database Quick Checks

Connect:

```bash
docker compose exec postgres psql -U osf -d osfdb
```

Inside psql:

```sql
-- Count pending and completed records
SELECT
  COUNT(*) FILTER (WHERE COALESCE(pdf_downloaded,false)=false) AS pdf_pending,
  COUNT(*) FILTER (WHERE COALESCE(pdf_downloaded,false)=true AND COALESCE(tei_generated,false)=false) AS tei_pending,
  COUNT(*) FILTER (WHERE COALESCE(tei_generated,false)=true) AS tei_done
FROM preprints;

-- Recently downloaded PDFs
SELECT osf_id, pdf_path, pdf_downloaded_at
FROM preprints WHERE pdf_downloaded ORDER BY pdf_downloaded_at DESC LIMIT 10;

-- Recently generated TEI XML
SELECT osf_id, tei_path, tei_generated_at
FROM preprints WHERE tei_generated ORDER BY tei_generated_at DESC LIMIT 10;
```

Exit: `\q`

---

## 📊 Task Monitoring

Inspect running and registered tasks:

```bash
docker compose run --rm app sh -lc "celery -A osf_sync.celery_app.app inspect active"
docker compose run --rm app sh -lc "celery -A osf_sync.celery_app.app inspect registered"
```

---

## 🕒 Scheduled Jobs (Celery Beat)

`celery-beat` handles automatic scheduling for:

- Daily OSF sync
- Periodic PDF & GROBID enqueuing

Adjust schedules in `osf_sync/celery_app.py` under `app.conf.beat_schedule`.

---

### Sequential processing

PDF & GROBID workers run with `--concurrency=1` and chained tasks for strict order.

---

## 🔍 One-off Manual Tasks

**Download a specific PDF:**

```bash
docker compose run --rm app sh -lc "celery -A osf_sync.celery_app.app call osf_sync.tasks.download_single_pdf --args '[\"<OSF_ID>\"]'"
```

**Run GROBID on a specific preprint:**

```bash
docker compose run --rm app sh -lc "celery -A osf_sync.celery_app.app call osf_sync.tasks.grobid_single --args '[\"<OSF_ID>\"]'"
```

---

## 🧪 Quickstart Summary

```bash
# 1. Build and start services
docker compose build
docker compose up -d

# 2. Sync preprints from OSF
docker compose run --rm app python -m osf_sync.cli sync-from-date --start 2025-07-01

# 4. Download PDFs
docker compose run --rm app python -m osf_sync.cli enqueue-pdf --limit 50

# 5. Run GROBID on PDFs
docker compose run --rm app python -m osf_sync.cli enqueue-grobid --limit 25
```

---
