from __future__ import annotations

import datetime as dt
import logging
import uuid
from typing import Any, Optional

from sqlmodel import Session, select, delete

from app.ai.embeddings import EmbeddingsProvider, get_embeddings_provider
from app.models.chunk import Chunk
from app.models.decision import Decision
from app.utils.text import ChunkSpec, chunk_text, extract_docket_like, guess_language, normalize_text, sha256_text

logger = logging.getLogger(__name__)


def stable_uuid_url(url: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, url))


def stable_uuid_chunk(decision_id: str, chunk_index: int) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{decision_id}::chunk::{chunk_index}"))


class Indexer:
    def __init__(self, embeddings: Optional[EmbeddingsProvider] = None):
        self.embeddings = embeddings or get_embeddings_provider()

    def upsert_decision(
        self,
        session: Session,
        *,
        source_id: str,
        source_name: str,
        level: str,
        canton: Optional[str],
        url: str,
        pdf_url: Optional[str],
        title: Optional[str],
        decision_date: Optional[dt.date],
        published_date: Optional[dt.date],
        court: Optional[str],
        chamber: Optional[str],
        docket: Optional[str],
        language: Optional[str],
        text: str,
        meta: dict[str, Any] | None = None,
    ) -> tuple[Decision, bool]:
        """Upsert a decision. Returns (decision, is_new) tuple."""
        text = normalize_text(text)
        content_hash = sha256_text(text)
        decision_id = stable_uuid_url(url)

        existing = session.exec(select(Decision).where(Decision.id == decision_id)).first()
        if existing and existing.content_hash == content_hash:
            return existing, False

        if not docket:
            docket = extract_docket_like(text)

        if not language:
            language = guess_language(text)

        payload = Decision(
            id=decision_id,
            source_id=source_id,
            source_name=source_name,
            level=level,
            canton=canton,
            court=court,
            chamber=chamber,
            docket=docket,
            decision_date=decision_date,
            published_date=published_date,
            title=title,
            language=language,
            url=url,
            pdf_url=pdf_url,
            content_text=text,
            content_hash=content_hash,
            meta=meta or {},
        )

        session.merge(payload)
        session.commit()
        session.refresh(payload)

        # Re-index chunks on change.
        session.exec(delete(Chunk).where(Chunk.decision_id == payload.id))
        session.commit()

        self._index_chunks(session, payload.id, text)
        return payload, True

    def _index_chunks(self, session: Session, decision_id: str, text: str) -> None:
        spec = ChunkSpec()
        chunks = chunk_text(text, spec=spec)
        if not chunks:
            return

        # Embed in batches
        batch_size = 64
        embeddings: list[Optional[list[float]]] = []
        for i in range(0, len(chunks), batch_size):
            batch = chunks[i : i + batch_size]
            try:
                embeddings.extend(self.embeddings.embed(batch))
            except Exception as e:
                logger.warning("Embedding batch failed (storing null embeddings): %s", e)
                embeddings.extend([None] * len(batch))  # type: ignore[list-item]

        for idx, ch in enumerate(chunks):
            emb = embeddings[idx]
            c = Chunk(
                id=stable_uuid_chunk(decision_id, idx),
                decision_id=decision_id,
                chunk_index=idx,
                text=ch,
                embedding=emb,
            )
            session.add(c)
        session.commit()
