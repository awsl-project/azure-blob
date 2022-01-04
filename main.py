import logging
import threading

from celery import Celery
from celery.schedules import crontab

from config import settings
from tools import start_upload

logging.basicConfig(
    format="%(asctime)s: %(levelname)s: %(name)s: %(message)s",
    level=logging.INFO
)

app = Celery('awsl-tasks', broker=settings.broker)


lock = threading.Lock()


@app.task
def start():
    lock.acquire()
    try:
        start_upload()
    finally:
        lock.release()


app.conf.beat_schedule = {
    "awsl-tasks": {
        "task": "main.start",
        "schedule": crontab(hour="*", minute=1)
    }
}
