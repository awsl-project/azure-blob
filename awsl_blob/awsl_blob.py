from concurrent.futures import ThreadPoolExecutor
import pika
import json
import logging

from retry import retry
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError

from awsl_blob.tools import copy_from_url

from .config import settings
from .tools import delete_pic, get_all_pic_to_upload, get_pic_to_upload, update_db_status

_logger = logging.getLogger(__name__)


@retry(Exception, tries=10, delay=5, jitter=(1, 3), max_delay=50, logger=_logger)
def send_photos(ch, method, properties, body) -> None:
    pic_ids = json.loads(body)
    blob_service_client = BlobServiceClient.from_connection_string(
        settings.connection_string)
    blob_groups = get_pic_to_upload(pic_ids)
    for blob_group in blob_groups:
        for pic_size, blob in blob_group.blobs.blobs.items():
            copy_from_url(blob_service_client, pic_size, blob)
    update_db_status(blob_groups)
    _logger.info("upload to azure blob %s", pic_ids)


@retry(Exception, delay=5, jitter=(1, 3), max_delay=50, logger=_logger)
def start_consuming():
    _logger.info('[*] Waiting for messages. To exit press CTRL+C')
    connection = pika.BlockingConnection(pika.URLParameters(settings.pika_url))
    channel = connection.channel()
    channel.queue_declare(queue=settings.queue, durable=True)
    channel.basic_consume(
        on_message_callback=send_photos,
        queue=settings.queue,
        auto_ack=True
    )
    try:
        channel.start_consuming()
    finally:
        connection.close()


def migration():
    blob_service_client = BlobServiceClient.from_connection_string(
        settings.connection_string)
    blob_groups = get_all_pic_to_upload()
    with ThreadPoolExecutor(max_workers=100) as executor:
        for blob_group in blob_groups:
            executor.submit(upload, blob_service_client, blob_group)


def upload(blob_service_client, blob_group):
    try:
        for pic_size, blob in blob_group.blobs.blobs.items():
            copy_from_url(blob_service_client, pic_size, blob)
        update_db_status([blob_group])
        _logger.info("upload to azure blob %s", blob_group)
    except ResourceNotFoundError as e:
        delete_pic(blob_group)
        _logger.exception(e)
