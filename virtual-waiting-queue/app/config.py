from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    redis_url: str = "redis://localhost:6379/0"
    redis_max_connections: int = 128
    queue_poll_seconds: float = 1.0
    heartbeat_seconds: float = 15.0
    default_admission_ttl_seconds: int = 600
    default_queue_enabled: bool = False


settings = Settings()
