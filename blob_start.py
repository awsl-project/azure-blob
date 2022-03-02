import logging

from celery import Celery
from celery.schedules import crontab


from awsl_blob.config import settings, migration

logging.basicConfig(
    format="%(asctime)s: %(levelname)s: %(name)s: %(message)s",
    level=logging.INFO
)

app = Celery('awsl-tasks', broker=settings.broker)


@app.task
def start_blob():
    migration()


app.conf.beat_schedule = {
    "awsl-tasks": {
        "task": "blob_start.start_blob",
        "schedule": crontab(hour="*", minute=1)
    }
}
