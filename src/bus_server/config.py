from pathlib import Path

import pydantic
from pydantic_settings import BaseSettings


class AppConfig(BaseSettings):
    S3_BUCKET: str = "your-s3-bucket-name"
    REF_SERVER_URL: pydantic.HttpUrl = pydantic.HttpUrl("http://localhost:8080")
    RUN_LOCALLY: bool = True
    LOCAL_STORAGE_PATH: Path = Path.cwd() / "local_storage"

    class Config:
        env_prefix = "BUS_APP_"
        env_file = ".env"


CONFIG = AppConfig()
