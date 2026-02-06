from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Iterable, List

from langdetect import detect, LangDetectException


def normalize_text(text: str) -> str:
    # Collapse whitespace, normalize hyphenation artifacts, keep paragraphs.
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t\f\v]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


def guess_language(text: str) -> str | None:
    sample = text[:4000]
    try:
        return detect(sample)
    except LangDetectException:
        return None


@dataclass(frozen=True)
class ChunkSpec:
    max_chars: int = 1800
    overlap_chars: int = 250


def chunk_text(text: str, spec: ChunkSpec = ChunkSpec()) -> List[str]:
    """Chunk by paragraphs with overlap. Chunks are in characters to be model-agnostic."""
    paras = [p.strip() for p in text.split("\n\n") if p.strip()]
    chunks: list[str] = []
    buf: list[str] = []
    buf_len = 0

    def flush() -> None:
        nonlocal buf, buf_len
        if not buf:
            return
        chunk = "\n\n".join(buf).strip()
        if chunk:
            chunks.append(chunk)
        # overlap: keep tail
        tail = chunk[-spec.overlap_chars :] if spec.overlap_chars > 0 else ""
        buf = [tail] if tail else []
        buf_len = len(tail)

    for p in paras:
        if buf_len + len(p) + 2 > spec.max_chars:
            flush()
        buf.append(p)
        buf_len += len(p) + 2
    flush()
    return chunks


def extract_docket_like(text: str) -> str | None:
    # Very rough. Examples: 1C_123/2024, 5A_100/2021, ST.2022.111-SK3, PS250322, etc.
    patterns = [
        r"\b\d+[A-Z]_[0-9]{1,4}/[0-9]{4}\b",
        r"\b[A-Z]{1,3}\.?\d{4}\.\d{1,4}(?:-[A-Z]{1,4}\d?)?\b",
        r"\b[A-Z]{2}\d{5,6}\b",
        r"\b[A-Z]{1,3}\d{2}\d{4}\b",
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            return m.group(0)
    return None
