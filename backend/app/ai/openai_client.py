from __future__ import annotations

from functools import lru_cache
from typing import Optional

from openai import OpenAI

from app.core.config import get_settings


@lru_cache
def get_openai_client() -> OpenAI:
    """Get OpenAI client with server-configured API key."""
    settings = get_settings()
    return OpenAI(
        api_key=settings.openai_api_key,
        base_url=settings.openai_base_url,
    )


def get_openai_client_with_key(api_key: Optional[str] = None) -> OpenAI:
    """Get OpenAI client with user-provided or server API key."""
    settings = get_settings()
    return OpenAI(
        api_key=api_key or settings.openai_api_key,
        base_url=settings.openai_base_url,
    )
