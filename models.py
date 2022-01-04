import json
import logging
from typing import Dict, List

from pydantic.main import BaseModel
from sqlalchemy import Column, String, INT, TEXT, Boolean, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from config import settings

engine = create_engine(settings.db_url, pool_size=100)
DBSession = sessionmaker(bind=engine)
Base = declarative_base(engine)

_logger = logging.getLogger(__name__)
CONTAINERS = ["thumbnail", "bmiddle", "large", "original", "largest", "mw2000"]


class Blob(BaseModel):
    pic_id: str
    container: str
    url: dict


class Pic(Base):
    __tablename__ = 'awsl_pic'

    id = Column(INT, primary_key=True, autoincrement=True)
    pic_id = Column(String(255))
    pic_info = Column(TEXT)
    is_azure_blob = Column(Boolean)


def get_pic_to_upload() -> Dict[int, List[Blob]]:
    session = DBSession()
    res = {}
    try:
        pics = session.query(Pic).filter(
            not Pic.is_azure_blob
        ).all()
        for pic in pics:
            pic_info = json.loads(pic.pic_info)
            res[pic.id] = [
                Blob(pic_id=pic.pic_id, container=container, url=pic_info[container]["url"])
                for container in CONTAINERS
                if isinstance(pic_info.get(container), dict) and
                isinstance(pic_info[container].get("url"), str)
            ]
        _logger.info("get_pic_to_upload: count = %s", len(res))
    finally:
        session.close()
    return res


def update_db_status(id_of_pic: int):
    pass
