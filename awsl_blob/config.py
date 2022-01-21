import os

from pydantic import BaseSettings


class Settings(BaseSettings):
    connection_string: str
    blob_url: str
    blob_container: str
    db_url: str
    pika_url: str
    queue: str

    class Config:
        env_file = os.environ.get("ENV_FILE", ".env")


settings = Settings()
