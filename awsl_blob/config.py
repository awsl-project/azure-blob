import os

from pydantic import BaseSettings


class Settings(BaseSettings):
    connection_string: str
    blob_container: str
    db_url: str
    max_workers: int
    migration_limit: int
    delete_after_days: int
    delete_limit: int

    class Config:
        env_file = os.environ.get("ENV_FILE", ".env")


settings = Settings()
