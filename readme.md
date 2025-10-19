## Docker Build

docker compose build

## Docker Start Containers

docker compose up -d

## Log Workers

docker compose logs -f worker-name

## Download PDFs

docker compose run --rm app sh -lc 'celery -A osf_sync.celery_app.app call osf_sync.tasks.enqueue_pdf_downloads --args "[]"'

## Fetch Preprints from OSF into DB (Start Date)

docker compose run --rm app python -m osf_sync.cli sync-from-date --start 2025-07-01

## Enter Container

docker compose run --rm app bash
