from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional, Protocol

from app.ai.openai_client import get_openai_client, get_openai_client_with_key
from app.core.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class LLMProvider(Protocol):
    def generate(self, *, system: str, user: str) -> str: ...


@dataclass
class OpenAILLM(LLMProvider):
    model: str = field(default_factory=lambda: settings.openai_model)
    api_key: Optional[str] = None

    def generate(self, *, system: str, user: str) -> str:
        if self.api_key:
            client = get_openai_client_with_key(self.api_key)
        else:
            client = get_openai_client()

        # Prefer Responses API (current). Fall back to Chat Completions if needed.
        if hasattr(client, "responses"):
            resp = client.responses.create(
                model=self.model,
                input=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
            )
            # SDK returns output_text convenience on response objects.
            if hasattr(resp, "output_text") and resp.output_text:
                return resp.output_text
            # Fallback: attempt to stitch content
            try:
                parts = []
                for item in resp.output:
                    for c in getattr(item, "content", []) or []:
                        if getattr(c, "type", None) == "output_text":
                            parts.append(getattr(c, "text", ""))
                return "".join(parts).strip()
            except Exception:
                return ""
        else:
            resp = client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
            )
            return (resp.choices[0].message.content or "").strip()


class DisabledLLM(LLMProvider):
    def generate(self, *, system: str, user: str) -> str:
        return "LLM is not configured. Set LLM_PROVIDER and API keys."


def get_llm_provider(api_key: Optional[str] = None) -> LLMProvider:
    if settings.llm_provider.lower() == "openai":
        if not api_key and not settings.openai_api_key:
            logger.warning("OPENAI_API_KEY not set; LLM disabled.")
            return DisabledLLM()
        return OpenAILLM(api_key=api_key)
    return DisabledLLM()
