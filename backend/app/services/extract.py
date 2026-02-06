from __future__ import annotations

import io
import logging
import re
from dataclasses import dataclass
from typing import Optional

from bs4 import BeautifulSoup
from pdfminer.high_level import extract_text as pdfminer_extract_text
from pypdf import PdfReader
import trafilatura

from app.utils.text import normalize_text

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Extracted:
    text: str
    title: Optional[str] = None


def extract_html(html_bytes: bytes, url: str | None = None) -> Extracted:
    html = html_bytes.decode("utf-8", errors="ignore")
    title = None
    try:
        soup = BeautifulSoup(html, "lxml")
        t = soup.find("title")
        title = t.get_text(strip=True) if t else None
    except Exception:
        pass

    # Trafilatura does the heavy lifting; fallback to soup text if needed.
    text = None
    try:
        text = trafilatura.extract(
            html,
            url=url,
            include_comments=False,
            include_tables=False,
            include_formatting=False,
            favor_precision=True,
        )
    except Exception:
        text = None

    if not text:
        try:
            soup = BeautifulSoup(html, "lxml")
            text = soup.get_text("\n")
        except Exception:
            text = html

    return Extracted(text=normalize_text(text), title=title)


def extract_pdf(pdf_bytes: bytes) -> Extracted:
    # Try pypdf first (fast, good enough for most court PDFs), then pdfminer.
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        parts: list[str] = []
        for page in reader.pages:
            parts.append(page.extract_text() or "")
        text = "\n\n".join(parts)
        text = normalize_text(text)
        if len(text) > 200:
            return Extracted(text=text)
    except Exception:
        pass

    try:
        text = pdfminer_extract_text(io.BytesIO(pdf_bytes)) or ""
        return Extracted(text=normalize_text(text))
    except Exception as e:
        logger.warning("PDF extraction failed: %s", e)
        return Extracted(text="")


def is_pdf_content_type(content_type: str | None) -> bool:
    if not content_type:
        return False
    return "application/pdf" in content_type.lower()


def is_probably_pdf_url(url: str) -> bool:
    return url.lower().endswith(".pdf")


def maybe_extract(bytes_: bytes, content_type: str | None, url: str) -> Extracted:
    if is_pdf_content_type(content_type) or is_probably_pdf_url(url):
        return extract_pdf(bytes_)
    return extract_html(bytes_, url=url)
