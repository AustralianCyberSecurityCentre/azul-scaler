"""Scaler configuration options."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Scaler environment variables parsed into settings object."""

    model_config = SettingsConfigDict(env_prefix="scaler_", extra="allow")

    port: int = 8090

    # URL used to contact Burrow
    burrow_url: str = ""

    # Redis connection details
    redis_url: str = "localhost:6379"
    redis_username: str = ""
    redis_password: str = ""

    # If GRPC channelz should be enabled
    debug: bool = False

    # Topic prefix used as a pre-filter for consumer groups
    topic_prefix: str = ""


settings = Settings()
