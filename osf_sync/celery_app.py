import os
from celery import Celery
from celery.schedules import crontab

app = Celery(
    "osf_sync",
    broker=os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0"),
    backend=os.getenv("CELERY_RESULT_BACKEND", "redis://redis:6379/0"),
    include=["osf_sync.tasks"],            
)

app.conf.update(
    timezone="Europe/Berlin",
    enable_utc=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
)

app.conf.task_routes = {
    "osf_sync.tasks.download_single_pdf": {"queue": "pdf"},
}   

app.conf.beat_schedule.update({
    "queue-pdf-downloads-daily": {
        "task": "osf_sync.tasks.enqueue_pdf_downloads",
        "schedule": crontab(minute=45, hour=3),  # 03:45 Europe/Berlin
        "args": [200],  # enqueue up to 200 PDF downloads daily
    },
})

app.conf.beat_schedule = {
    "osf-live-sync-daily": {
        "task": "osf_sync.tasks.sync_from_osf",
        "schedule": crontab(minute=15, hour=2),
        "args": [],
    },
}

app.conf.task_routes.update({
    "osf_sync.tasks.grobid_single": {"queue": "grobid"},
})

app.conf.beat_schedule.update({
    "queue-grobid-daily": {
        "task": "osf_sync.tasks.enqueue_grobid",
        "schedule": crontab(minute=30, hour=4),
        "args": [200],   # process up to 200 per day; adjust as you like
    },
})