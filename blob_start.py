import logging

from celery import Celery
from celery.schedules import crontab


from awsl_blob.config import settings
from awsl_blob.awsl_blob import migration

logging.basicConfig(
    format="%(asctime)s: %(levelname)s: %(name)s: %(message)s",
    level=logging.INFO
)

app = Celery('blob-tasks', broker=settings.broker)


@app.task
def start_blob():
    migration()


app.conf.beat_schedule = {
    "blob-tasks": {
        "task": "blob_start.start_blob",
        "schedule": crontab(hour="*", minute=1)
    }
}
