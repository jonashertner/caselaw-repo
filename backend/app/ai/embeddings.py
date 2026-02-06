from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List, Optional, Protocol

from app.core.config import get_settings
from app.ai.openai_client import get_openai_client, get_openai_client_with_key

logger = logging.getLogger(__name__)
settings = get_settings()


class EmbeddingsProvider(Protocol):
    def embed(self, texts: List[str]) -> List[List[float]]: ...


@dataclass
class OpenAIEmbeddings(EmbeddingsProvider):
    model: str = field(default_factory=lambda: settings.openai_embeddings_model)
    api_key: Optional[str] = None

    def embed(self, texts: List[str]) -> List[List[float]]:
        if self.api_key:
            client = get_openai_client_with_key(self.api_key)
        else:
            client = get_openai_client()
        # dimensions is supported by text-embedding-3-*; harmless if ignored by server.
        kwargs = {"model": self.model, "input": texts}
        if "text-embedding-3" in (self.model or ""):
            kwargs["dimensions"] = settings.embeddings_dim
        resp = client.embeddings.create(**kwargs)
        return [d.embedding for d in resp.data]


@dataclass
class LocalEmbeddings(EmbeddingsProvider):
    model_name: str = settings.local_embeddings_model

    def __post_init__(self) -> None:
        from sentence_transformers import SentenceTransformer

        self._model = SentenceTransformer(self.model_name)

    def embed(self, texts: List[str]) -> List[List[float]]:
        # normalize embeddings for cosine similarity
        import numpy as np

        v = self._model.encode(texts, normalize_embeddings=True)
        if hasattr(v, "tolist"):
            return v.tolist()
        return np.asarray(v).tolist()


def get_embeddings_provider(api_key: Optional[str] = None) -> EmbeddingsProvider:
    if settings.embeddings_provider.lower() == "local":
        return LocalEmbeddings()
    return OpenAIEmbeddings(api_key=api_key)