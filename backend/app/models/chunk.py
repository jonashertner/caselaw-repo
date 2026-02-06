from __future__ import annotations

from typing import Optional

from pgvector.sqlalchemy import Vector
from sqlalchemy import Column, ForeignKey, TEXT
from sqlmodel import Field, SQLModel

from app.core.config import get_settings


settings = get_settings()


class Chunk(SQLModel, table=True):
    __tablename__ = "chunks"

    id: str = Field(primary_key=True, default=None)  # assigned in service

    decision_id: str = Field(foreign_key="decisions.id", index=True)
    chunk_index: int = Field(index=True)

    text: str = Field(sa_column=Column(TEXT, nullable=False))

    # Fixed dimension enables vector indexes; adjust via EMBEDDINGS_DIM.
    embedding: Optional[list[float]] = Field(
        default=None,
        sa_column=Column(Vector(settings.embeddings_dim), nullable=True),
    )
