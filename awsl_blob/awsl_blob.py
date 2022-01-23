import pika
import time
import json
import logging

from retry import retry
from azure.storage.blob import BlobServiceClient

from awsl_blob.tools import copy_from_url
from .LRU import LRU

from .config import settings
from .tools import get_pic_to_upload, update_db_status

_logger = logging.getLogger(__name__)
failed_key = LRU()


def send_photos(ch, method, properties, body) -> None:
    try:
        hash_body = hash(body)
        pic_ids = json.loads(body)
        blob_service_client = BlobServiceClient.from_connection_string(
            settings.connection_string)
        blob_groups = get_pic_to_upload(pic_ids)
        for blob_group in blob_groups:
            for pic_size, blob in blob_group.blobs.blobs.items():
                copy_from_url(blob_service_client, pic_size, blob)
        update_db_status(blob_groups)
        _logger.info("upload to azure blob %s", pic_ids)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        count = failed_key.get(hash_body, 0)
        failed_key.put(hash_body, count+1)
        if count >= 5:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            _logger.info("send body to %s", settings.queue+"_failed")
            ch.basic_publish(
                exchange='',
                routing_key=settings.queue+"_failed",
                body=body,
                properties=pika.BasicProperties(delivery_mode=2)
            )
        raise e
    finally:
        time.sleep(10)


@retry(Exception, delay=5, jitter=(1, 3), max_delay=50, logger=_logger)
def start_consuming():
    _logger.info('[*] Waiting for messages. To exit press CTRL+C')
    connection = pika.BlockingConnection(pika.URLParameters(settings.pika_url))
    channel = connection.channel()
    channel.queue_declare(queue=settings.queue, durable=True)
    channel.queue_declare(queue=settings.queue+"_failed", durable=True)
    channel.basic_consume(
        on_message_callback=send_photos,
        queue=settings.queue,
        auto_ack=False
    )
    try:
        channel.start_consuming()
    finally:
        connection.close()
