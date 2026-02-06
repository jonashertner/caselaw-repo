from __future__ import annotations

from functools import lru_cache
from typing import List

from pydantic import AnyHttpUrl, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Core
    app_env: str = Field(default="dev", alias="APP_ENV")
    app_name: str = Field(default="Swiss Case Law AI", alias="APP_NAME")
    public_base_url: str = Field(default="http://localhost:8000", alias="PUBLIC_BASE_URL")

    # DB
    database_url: str = Field(
        default="postgresql+psycopg://postgres:postgres@localhost:5432/swisslaw",
        alias="DATABASE_URL",
    )
    db_echo: bool = Field(default=False, alias="DB_ECHO")

    # CORS
    cors_origins: str = Field(default="http://localhost:3000", alias="CORS_ORIGINS")

    # Ingestion
    ingest_user_agent: str = Field(default="swiss-caselaw-ai/0.1", alias="INGEST_USER_AGENT")
    ingest_max_pages_per_source: int = Field(default=2000, alias="INGEST_MAX_PAGES_PER_SOURCE")
    ingest_max_depth: int = Field(default=3, alias="INGEST_MAX_DEPTH")
    ingest_respect_robots: bool = Field(default=True, alias="INGEST_RESPECT_ROBOTS")
    ingest_request_timeout_s: int = Field(default=30, alias="INGEST_REQUEST_TIMEOUT_S")
    ingest_concurrency: int = Field(default=10, alias="INGEST_CONCURRENCY")

    # AI
    llm_provider: str = Field(default="openai", alias="LLM_PROVIDER")
    embeddings_provider: str = Field(default="openai", alias="EMBEDDINGS_PROVIDER")
    embeddings_dim: int = Field(default=3072, alias="EMBEDDINGS_DIM")

    openai_api_key: str | None = Field(default=None, alias="OPENAI_API_KEY")
    openai_base_url: str = Field(default="https://api.openai.com/v1", alias="OPENAI_BASE_URL")
    openai_model: str = Field(default="gpt-5", alias="OPENAI_MODEL")
    openai_embeddings_model: str = Field(default="text-embedding-3-large", alias="OPENAI_EMBEDDINGS_MODEL")

    local_embeddings_model: str = Field(default="intfloat/multilingual-e5-base", alias="LOCAL_EMBEDDINGS_MODEL")

    # Auth / security (reserved; UI uses local storage only by default)
    jwt_secret: str = Field(default="dev-secret-change-me", alias="JWT_SECRET")

    def cors_origin_list(self) -> List[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
