from pathlib import Path

import boto3
import pydantic
from mypy_boto3_s3 import S3Client
from pydantic_settings import BaseSettings


class AppConfig(BaseSettings):
    S3_BUCKET: str = "your-s3-bucket-name"
    """S3 bucket name."""

    REF_SERVER_URL: pydantic.HttpUrl = pydantic.HttpUrl("http://localhost:8080")
    """URL of the reference server."""

    RUN_LOCALLY: bool = True
    """Whether the application is using local storage."""

    LOCAL_STORAGE_PATH: Path = Path.cwd() / "local_storage"
    """Path to the local storage directory."""

    class Config:
        env_prefix = "BUS_APP_"
        env_file = ".env"


CONFIG = AppConfig()

if not CONFIG.RUN_LOCALLY:
    # Initialize S3 client
    S3_CLIENT: S3Client = boto3.client("s3", region_name="us-east-1")
else:
    S3_CLIENT = None
