from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    aws_region: str = "us-east-1"
    dynamodb_endpoint_url: str | None = "http://localhost:8000"
    dynamodb_jobs_table: str = "job_scheduler_jobs"
    dynamodb_executions_table: str = "job_scheduler_executions"
    redis_url: str = "redis://localhost:6379/0"
    redis_due_queue_key: str = "job_scheduler:due"

    scheduler_window_seconds: int = 300
    scheduler_poll_seconds: float = 5.0
    worker_poll_seconds: float = 0.5
    max_attempts: int = 3


settings = Settings()
