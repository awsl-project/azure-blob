from concurrent.futures import ThreadPoolExecutor
import logging

from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError

from awsl_blob.tools import copy_from_url

from .config import settings
from .tools import delete_azure_blob, delete_pic, get_all_pic_to_delete, get_all_pic_to_upload, update_db_status

_logger = logging.getLogger(__name__)


def migration():
    blob_service_client = BlobServiceClient.from_connection_string(
        settings.connection_string)
    blob_groups = get_all_pic_to_upload()
    with ThreadPoolExecutor(max_workers=settings.max_workers) as executor:
        for blob_group in blob_groups:
            executor.submit(upload, blob_service_client, blob_group)


def cleanup():
    blob_service_client = BlobServiceClient.from_connection_string(
        settings.connection_string)
    blobs_list = get_all_pic_to_delete()
    for blobs in blobs_list:
        for pic_type, blob in blobs.blobs.items():
            delete_azure_blob(blob_service_client, pic_type, blob)
    _logger.info("delete_azure_blobs")


def upload(blob_service_client, blob_group):
    try:
        for pic_size, blob in blob_group.blobs.blobs.items():
            copy_from_url(blob_service_client, pic_size, blob)
        update_db_status([blob_group])
        _logger.info("upload to azure blob %s", blob_group)
    except ResourceNotFoundError as e:
        delete_pic(blob_group)
        _logger.exception(e)
