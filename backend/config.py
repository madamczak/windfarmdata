"""
Application configuration using pydantic-settings.
Values can be overridden via environment variables.
"""

import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Global application settings."""

    storage_backend: str = "local"  # "local" | "s3" | "azure" | "gcs"

    # Base path where per-farm parquet directories live (local mode)
    parquet_base_path: str = os.path.join(
        os.path.dirname(__file__), "..", "data"
    )

    events_db_path: str = os.path.join(
        os.path.dirname(__file__), "..", "data", "events.db"
    )

    aws_region: str = "eu-west-1"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# Module-level singleton
settings = Settings()

