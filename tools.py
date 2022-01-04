import logging
import time
from azure.storage.blob import BlobServiceClient
from sqlalchemy.sql.sqltypes import Boolean

from config import settings
from models import Blob, get_pic_to_upload, update_db_status

_logger = logging.getLogger(__name__)


def start_upload():
    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(
        settings.connection_string)
    blobs_map = get_pic_to_upload()

    for id_of_pic, blobs in blobs_map.items():
        status_list = [
            copy_from_url(blob_service_client, blob)
            for blob in blobs
        ]
        if all(status_list):
            update_db_status(id_of_pic)
        time.sleep(10)


def copy_from_url(blob_service_client: BlobServiceClient, blob: Blob) -> Boolean:
    # Create the container
    # container_client = blob_service_client.create_container("mw2000")
    blob_client = blob_service_client.get_blob_client(
        container=blob.container, blob=blob.url.split("/")[-1]
    )
    blob_client.start_copy_from_url(blob.url)
    time.sleep(10)

    for _ in range(5):
        props = blob_client.get_blob_properties()
        status = props.copy.status
        if status == "success":
            # Copy finished
            _logger.info("copy success: pic_id = %s, container = %s",
                         blob.pic_id, blob.container)
            return True
        time.sleep(10)

    if status != "success":
        # if not finished after 50s, cancel the operation
        props = blob_client.get_blob_properties()
        copy_id = props.copy.id
        blob_client.abort_copy(copy_id)
        props = blob_client.get_blob_properties()
        _logger.info("abort_copy: pic_id = %s, container = %s",
                     blob.pic_id, blob.container)

    return False
