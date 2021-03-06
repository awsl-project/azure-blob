from datetime import datetime, timedelta
import json
import logging
import time
from typing import List
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .models.models import AwslBlob, Pic
from .config import settings
from .models.pydantic_models import Blob, BlobGroup, Blobs

_logger = logging.getLogger(__name__)
engine = create_engine(settings.db_url, pool_size=100)
DBSession = sessionmaker(bind=engine)
PIC_TYPES = ["large", "original"]


def copy_from_url(blob_service_client: BlobServiceClient, pic_size: str, blob: Blob) -> bool:
    blob_client = blob_service_client.get_blob_client(
        container=settings.blob_container, blob="/".join(
            [pic_size, blob.url.split("/")[-1]])
    )
    blob_client.start_copy_from_url(blob.url)
    blob.url = blob_client.url
    time.sleep(1)

    for _ in range(50):
        props = blob_client.get_blob_properties()
        status = props.copy.status
        if status == "success":
            # Copy finished
            _logger.info("copy success: blob = %s", blob)
            return True
        time.sleep(10)

    if status != "success":
        # if not finished after 50s, cancel the operation
        props = blob_client.get_blob_properties()
        copy_id = props.copy.id
        blob_client.abort_copy(copy_id)
        props = blob_client.get_blob_properties()
        _logger.info("abort_copy: blob = %s", blob)

    raise Exception("abort_copy: blob = %s" % blob)


def get_all_pic_to_upload() -> List[BlobGroup]:
    session = DBSession()
    res = []
    try:
        pics = session.query(Pic).join(
            AwslBlob, Pic.pic_id == AwslBlob.pic_id, isouter=True
        ).filter(
            AwslBlob.pic_id.is_(None)
        ).filter(
            Pic.deleted.isnot(True)
        ).order_by(Pic.awsl_id.desc()).limit(settings.migration_limit).all()
        for pic in pics:
            pic_info = json.loads(pic.pic_info)
            res.append(
                BlobGroup(
                    id=pic.pic_id,
                    awsl_id=pic.awsl_id,
                    blobs=Blobs(blobs={
                        pic_type: Blob(
                            pic_id=pic.pic_id,
                            url=pic_data["url"],
                            width=pic_data["width"],
                            height=pic_data["height"]
                        )
                        for pic_type, pic_data in pic_info.items()
                        if pic_type in PIC_TYPES and isinstance(pic_data, dict) and "url" in pic_data
                    })
                )
            )
        _logger.info("get_pic_to_upload: count = %s", len(res))
    finally:
        session.close()
    return res


def update_db_status(blob_groups: List[BlobGroup]):
    session = DBSession()
    try:
        for blob_group in blob_groups:
            session.add(AwslBlob(
                awsl_id=blob_group.awsl_id,
                pic_id=blob_group.id,
                pic_info=blob_group.blobs.json(),
            ))
        session.commit()
    finally:
        session.close()


def delete_pic(blob_group: BlobGroup):
    session = DBSession()
    try:
        for picobj in session.query(Pic).filter(
            Pic.pic_id == blob_group.id
        ).all():
            picobj.deleted = True
            picobj.cleaned = True
        session.commit()
    finally:
        session.close()


def get_all_pic_to_delete() -> List[Blobs]:
    session = DBSession()
    res = []
    try:
        awsl_blobs = session.query(AwslBlob).filter(
            AwslBlob.create_date < (
                datetime.now() - timedelta(days=settings.delete_after_days))
        ).limit(settings.delete_limit).all()
        pic_ids = [awsl_blob.pic_id for awsl_blob in awsl_blobs]
        res = [
            Blobs.parse_raw(awsl_blob.pic_info)
            for awsl_blob in awsl_blobs
        ]
        for picobj in session.query(Pic).filter(
            Pic.pic_id.in_(pic_ids)
        ).all():
            picobj.deleted = True
        for blob in awsl_blobs:
            session.delete(blob)
        session.commit()
        _logger.info("get_all_pic_to_delete: count = %s", len(res))
    finally:
        session.close()
    return res


def delete_azure_blob(blob_service_client: BlobServiceClient, pic_size: str, blob: Blob) -> bool:
    if pic_size not in PIC_TYPES:
        return True
    blob_client = blob_service_client.get_blob_client(
        container=settings.blob_container, blob="/".join(
            [pic_size, blob.url.split("/")[-1]])
    )
    try:
        blob_client.delete_blob()
    except ResourceNotFoundError:
        return True
