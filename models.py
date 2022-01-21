import json
import logging
from typing import List

from pydantic.main import BaseModel
from sqlalchemy import Column, String, INT, TEXT, Boolean, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.schema import ForeignKey

from config import settings

engine = create_engine(settings.db_url, pool_size=100)
DBSession = sessionmaker(bind=engine)
Base = declarative_base(engine)

_logger = logging.getLogger(__name__)
CONTAINERS = ["thumbnail", "bmiddle", "large", "original", "largest", "mw2000"]
