"""Common utilities for Swiss caselaw scrapers.

This module provides shared functionality for all scrapers:
- Retry decorator with exponential backoff
- Rate limiting
- Checkpointing for resume capability
- Hash computation
- PDF text extraction
- Date parsing utilities
- Upsert logic for database insertion (ON CONFLICT DO UPDATE)
"""
from __future__ import annotations

import functools
import hashlib
import io
import json
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, TypeVar, TYPE_CHECKING

import httpx
from sqlalchemy.dialects.postgresql import insert as pg_insert

if TYPE_CHECKING:
    from sqlmodel import Session
    from app.models.decision import Decision

logger = logging.getLogger(__name__)

# Type variable for retry decorator
T = TypeVar("T")

# Default headers for all scrapers
DEFAULT_HEADERS = {
    "User-Agent": "swiss-caselaw-ai/0.1 (+https://github.com/jonashertner/swiss-caselaw)"
}


# =============================================================================
# Retry Decorator
# =============================================================================

def retry(
    max_attempts: int = 3,
    backoff_base: float = 2.0,
    max_backoff: float = 60.0,
    exceptions: tuple = (httpx.HTTPError, httpx.TimeoutException, ConnectionError),
    no_retry_status_codes: tuple[int, ...] = (400, 401, 403, 404, 410, 422),
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator for retrying functions with exponential backoff.

    Args:
        max_attempts: Maximum number of retry attempts
        backoff_base: Base for exponential backoff (seconds)
        max_backoff: Maximum backoff time (seconds)
        exceptions: Tuple of exceptions to catch and retry
        no_retry_status_codes: HTTP status codes that should not be retried
            (e.g., 404 Not Found is not a transient error)

    Example:
        @retry(max_attempts=3, backoff_base=2.0)
        def fetch_page(url: str) -> str:
            return httpx.get(url).text
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            last_exception = None
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    # Don't retry certain HTTP status codes (not transient errors)
                    if isinstance(e, httpx.HTTPStatusError):
                        if e.response.status_code in no_retry_status_codes:
                            raise  # Re-raise immediately, don't retry

                    last_exception = e
                    if attempt < max_attempts - 1:
                        wait_time = min(backoff_base ** attempt, max_backoff)
                        logger.warning(
                            f"Attempt {attempt + 1}/{max_attempts} failed: {e}. "
                            f"Retrying in {wait_time:.1f}s..."
                        )
                        time.sleep(wait_time)
                    else:
                        logger.error(f"All {max_attempts} attempts failed: {e}")
            raise last_exception  # type: ignore
        return wrapper
    return decorator


# =============================================================================
# Rate Limiter
# =============================================================================

class RateLimiter:
    """Rate limiter using token bucket algorithm.

    Example:
        limiter = RateLimiter(requests_per_second=2.0)
        for url in urls:
            limiter.wait()
            response = httpx.get(url)
    """

    def __init__(self, requests_per_second: float = 2.0):
        self.min_interval = 1.0 / requests_per_second
        self.last_request_time = 0.0

    def wait(self) -> None:
        """Wait if necessary to respect rate limit."""
        now = time.time()
        elapsed = now - self.last_request_time
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_request_time = time.time()


# =============================================================================
# Checkpoint Manager
# =============================================================================

@dataclass
class CheckpointManager:
    """Manages checkpoints for scraper resume capability.

    Stores progress in JSON files under backend/data/checkpoints/.

    Example:
        checkpoint = CheckpointManager()

        # Load previous state
        state = checkpoint.load("bger")
        start_page = state.get("last_page", 1) if state else 1

        for page in range(start_page, max_pages):
            process_page(page)
            checkpoint.save("bger", {"last_page": page, "last_id": last_id})

        # Clear on success
        checkpoint.clear("bger")
    """

    checkpoint_dir: Path = field(default_factory=lambda: Path(__file__).parent.parent / "data" / "checkpoints")

    def __post_init__(self) -> None:
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

    def _get_path(self, scraper_name: str) -> Path:
        return self.checkpoint_dir / f"{scraper_name}.json"

    def save(self, scraper_name: str, state: dict[str, Any]) -> None:
        """Save checkpoint state for a scraper."""
        state["_timestamp"] = datetime.now().isoformat()
        path = self._get_path(scraper_name)
        path.write_text(json.dumps(state, default=str, indent=2))
        logger.debug(f"Checkpoint saved for {scraper_name}: {state}")

    def load(self, scraper_name: str) -> dict[str, Any] | None:
        """Load checkpoint state for a scraper. Returns None if no checkpoint exists."""
        path = self._get_path(scraper_name)
        if not path.exists():
            return None
        try:
            state = json.loads(path.read_text())
            logger.info(f"Resuming {scraper_name} from checkpoint: {state}")
            return state
        except Exception as e:
            logger.warning(f"Failed to load checkpoint for {scraper_name}: {e}")
            return None

    def clear(self, scraper_name: str) -> None:
        """Clear checkpoint for a scraper (call on successful completion)."""
        path = self._get_path(scraper_name)
        if path.exists():
            path.unlink()
            logger.debug(f"Checkpoint cleared for {scraper_name}")


# =============================================================================
# Hash Computation
# =============================================================================

def compute_hash(text: str) -> str:
    """Compute SHA-256 hash of text content (truncated to 32 chars).

    Used for deduplication - same content produces same hash.
    """
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:32]


# =============================================================================
# PDF Text Extraction
# =============================================================================

def extract_pdf_text(pdf_content: bytes) -> str | None:
    """Extract text from PDF content.

    Args:
        pdf_content: Raw PDF bytes

    Returns:
        Extracted text or None if extraction fails
    """
    try:
        from pdfminer.high_level import extract_text
        return extract_text(io.BytesIO(pdf_content))
    except ImportError:
        logger.error("pdfminer.six not installed. Run: pip install pdfminer.six")
        return None
    except Exception as e:
        logger.warning(f"PDF extraction failed: {e}")
        return None


# =============================================================================
# Date Parsing
# =============================================================================

# Common date patterns in Swiss legal documents
DATE_PATTERNS = [
    (r"(\d{4})-(\d{2})-(\d{2})", "%Y-%m-%d"),  # ISO: 2024-01-15
    (r"(\d{2})\.(\d{2})\.(\d{4})", "%d.%m.%Y"),  # Swiss: 15.01.2024
    (r"(\d{2})/(\d{2})/(\d{4})", "%d/%m/%Y"),  # Alt: 15/01/2024
    (r"(\d{1,2})\.\s*(\w+)\s+(\d{4})", None),  # German: 15. Januar 2024
]

MONTH_NAMES_DE = {
    "januar": 1, "februar": 2, "märz": 3, "april": 4,
    "mai": 5, "juni": 6, "juli": 7, "august": 8,
    "september": 9, "oktober": 10, "november": 11, "dezember": 12,
}

MONTH_NAMES_FR = {
    "janvier": 1, "février": 2, "mars": 3, "avril": 4,
    "mai": 5, "juin": 6, "juillet": 7, "août": 8,
    "septembre": 9, "octobre": 10, "novembre": 11, "décembre": 12,
}


def parse_date_flexible(date_str: str) -> date | None:
    """Parse date from various formats common in Swiss legal documents.

    Handles:
    - ISO format: 2024-01-15
    - Swiss format: 15.01.2024
    - German text: 15. Januar 2024
    - French text: 15 janvier 2024

    Returns:
        date object or None if parsing fails
    """
    if not date_str:
        return None

    date_str = date_str.strip()

    # Try standard patterns
    for pattern, fmt in DATE_PATTERNS[:3]:  # Skip German month pattern
        match = re.search(pattern, date_str)
        if match:
            try:
                return datetime.strptime(match.group(0), fmt).date()
            except ValueError:
                continue

    # Try German month names
    match = re.search(r"(\d{1,2})\.?\s*(\w+)\s+(\d{4})", date_str, re.I)
    if match:
        day, month_name, year = match.groups()
        month = MONTH_NAMES_DE.get(month_name.lower()) or MONTH_NAMES_FR.get(month_name.lower())
        if month:
            try:
                return date(int(year), month, int(day))
            except ValueError:
                pass

    return None


# =============================================================================
# HTTP Utilities
# =============================================================================

@retry(max_attempts=3, backoff_base=2.0)
def fetch_with_retry(
    url: str,
    *,
    headers: dict | None = None,
    timeout: int = 60,
    follow_redirects: bool = True,
) -> httpx.Response:
    """Fetch URL with automatic retry on failure.

    Args:
        url: URL to fetch
        headers: Optional headers (defaults to DEFAULT_HEADERS)
        timeout: Request timeout in seconds
        follow_redirects: Whether to follow redirects

    Returns:
        httpx.Response object

    Raises:
        httpx.HTTPError: If all retries fail
    """
    response = httpx.get(
        url,
        headers=headers or DEFAULT_HEADERS,
        timeout=timeout,
        follow_redirects=follow_redirects,
    )
    response.raise_for_status()
    return response


def create_http_client(
    *,
    timeout: int = 60,
    headers: dict | None = None,
) -> httpx.Client:
    """Create a configured httpx client for scraping.

    Example:
        with create_http_client() as client:
            response = client.get(url)
    """
    return httpx.Client(
        headers=headers or DEFAULT_HEADERS,
        timeout=timeout,
        follow_redirects=True,
    )


# =============================================================================
# Scraper Result Tracking
# =============================================================================

@dataclass
class ScraperStats:
    """Track scraper statistics."""

    imported: int = 0
    skipped: int = 0
    errors: int = 0
    start_time: float = field(default_factory=time.time)

    def add_imported(self, count: int = 1) -> None:
        self.imported += count

    def add_skipped(self, count: int = 1) -> None:
        self.skipped += count

    def add_error(self, count: int = 1) -> None:
        self.errors += count

    @property
    def elapsed(self) -> float:
        return time.time() - self.start_time

    def summary(self, scraper_name: str) -> str:
        return (
            f"\n{scraper_name} completed in {self.elapsed:.1f}s:\n"
            f"  Imported: {self.imported}\n"
            f"  Skipped:  {self.skipped}\n"
            f"  Errors:   {self.errors}"
        )


# =============================================================================
# Metadata Extraction Utilities
# =============================================================================

# Legal area patterns (German)
LEGAL_AREA_PATTERNS_DE = {
    r"steuer": "Steuerrecht",
    r"bau|planungs": "Baurecht",
    r"straf": "Strafrecht",
    r"zivil|vertrags|schuld": "Zivilrecht",
    r"sozialversicherung|iv|ahv|uv|alv": "Sozialversicherungsrecht",
    r"verwaltung": "Verwaltungsrecht",
    r"ausländer|migration": "Migrationsrecht",
    r"arbeits": "Arbeitsrecht",
    r"familie|ehe|kind": "Familienrecht",
    r"wettbewerb|kartell": "Wettbewerbsrecht",
    r"datenschutz": "Datenschutzrecht",
    r"umwelt": "Umweltrecht",
    r"patent|marke|urheber": "Immaterialgüterrecht",
}


def extract_legal_area(text: str) -> str | None:
    """Extract legal area from text content.

    Args:
        text: Document text or title

    Returns:
        Legal area category or None
    """
    text_lower = text.lower()
    for pattern, area in LEGAL_AREA_PATTERNS_DE.items():
        if re.search(pattern, text_lower):
            return area
    return None


# Case number patterns for Swiss courts
CASE_NUMBER_PATTERNS = [
    # Federal courts: 1C_123/2024, 2C_456/2024
    r"\b([124568][A-Z]_\d+/\d{4})\b",
    # BGE reference: BGE 147 II 1
    r"\b(BGE\s+\d+\s+[IVX]+\s+\d+)\b",
    # Cantonal patterns: ST.2024.123, BKREKGGB/2024/1
    r"\b([A-Z]{2,}\.\d{4}\.\d+)\b",
    r"\b([A-Z]{2,}/\d{4}/\d+)\b",
    # Generic: 2024-123, 123/2024
    r"\b(\d{4}[-/]\d+)\b",
    r"\b(\d+/\d{4})\b",
]


def extract_case_numbers(text: str) -> list[str]:
    """Extract all case numbers from text.

    Args:
        text: Document text

    Returns:
        List of unique case numbers found
    """
    numbers = []
    seen = set()

    for pattern in CASE_NUMBER_PATTERNS:
        for match in re.finditer(pattern, text):
            num = match.group(1)
            if num not in seen:
                seen.add(num)
                numbers.append(num)

    return numbers


# Legal norm patterns
NORM_PATTERNS = [
    # Swiss constitution: Art. 8 BV
    r"Art\.?\s*\d+(?:\s*(?:Abs|al|cpv)\.?\s*\d+)?(?:\s*(?:lit|let)\.?\s*[a-z])?\s+(?:BV|Cst|Cost)",
    # Swiss codes: Art. 41 OR, Art. 28 ZGB
    r"Art\.?\s*\d+(?:\s*(?:Abs|al|cpv)\.?\s*\d+)?(?:\s*(?:lit|let)\.?\s*[a-z])?\s+(?:OR|CO|ZGB|CC|StGB|CP|SchKG|LP)",
    # ECHR: Art. 6 EMRK
    r"Art\.?\s*\d+(?:\s*(?:Abs|al|cpv)\.?\s*\d+)?\s+(?:EMRK|CEDH|CEDU)",
    # Procedural codes
    r"Art\.?\s*\d+(?:\s*(?:Abs|al|cpv)\.?\s*\d+)?\s+(?:ZPO|CPC|StPO|CPP|BGG|LTF|VwVG|PA)",
    # Generic § reference
    r"§\s*\d+(?:\s*(?:Abs|al|cpv)\.?\s*\d+)?",
]


def extract_legal_norms(text: str) -> list[str]:
    """Extract legal norm citations from text.

    Args:
        text: Document text

    Returns:
        List of unique legal norm citations
    """
    norms = []
    seen = set()

    for pattern in NORM_PATTERNS:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            norm = match.group(0).strip()
            norm_key = re.sub(r"\s+", " ", norm.lower())
            if norm_key not in seen:
                seen.add(norm_key)
                norms.append(norm)

    return norms


def extract_metadata_from_text(text: str) -> dict[str, Any]:
    """Extract all available metadata from document text.

    Args:
        text: Full document text

    Returns:
        Dictionary with extracted metadata fields
    """
    # Use first 10000 chars for efficiency
    sample = text[:10000]

    meta = {}

    # Extract legal area
    area = extract_legal_area(sample)
    if area:
        meta["legal_area"] = area

    # Extract case numbers (besides primary)
    case_nums = extract_case_numbers(sample)
    if len(case_nums) > 1:
        meta["related_cases"] = case_nums[1:5]  # Limit to 4 related

    # Extract legal norms
    norms = extract_legal_norms(sample)
    if norms:
        meta["norms"] = norms[:20]  # Limit to 20 norms

    return meta


# =============================================================================
# Database Upsert Helper
# =============================================================================

def upsert_decision(session: "Session", decision: "Decision") -> tuple[bool, bool]:
    """Upsert a decision into the database using ON CONFLICT DO UPDATE.

    This handles duplicate URLs gracefully by updating the existing record
    instead of failing with a constraint violation.

    Args:
        session: SQLModel/SQLAlchemy session
        decision: Decision object to insert or update

    Returns:
        Tuple of (was_inserted, was_updated):
        - (True, False) if new record was inserted
        - (False, True) if existing record was updated
        - (False, False) if no change was needed (same content_hash)
    """
    from app.models.decision import Decision

    # Build the insert statement
    stmt = pg_insert(Decision).values(
        id=decision.id,
        source_id=decision.source_id,
        source_name=decision.source_name,
        level=decision.level,
        canton=decision.canton,
        court=decision.court,
        chamber=decision.chamber,
        docket=decision.docket,
        decision_date=decision.decision_date,
        published_date=decision.published_date,
        title=decision.title,
        language=decision.language,
        url=decision.url,
        pdf_url=decision.pdf_url,
        content_text=decision.content_text,
        content_hash=decision.content_hash,
        meta=decision.meta,
    )

    # On conflict (url), update all fields except id and indexed_at
    # Only update if content has changed (different content_hash)
    stmt = stmt.on_conflict_do_update(
        index_elements=["url"],
        set_={
            "source_id": stmt.excluded.source_id,
            "source_name": stmt.excluded.source_name,
            "level": stmt.excluded.level,
            "canton": stmt.excluded.canton,
            "court": stmt.excluded.court,
            "chamber": stmt.excluded.chamber,
            "docket": stmt.excluded.docket,
            "decision_date": stmt.excluded.decision_date,
            "published_date": stmt.excluded.published_date,
            "title": stmt.excluded.title,
            "language": stmt.excluded.language,
            "pdf_url": stmt.excluded.pdf_url,
            "content_text": stmt.excluded.content_text,
            "content_hash": stmt.excluded.content_hash,
            "meta": stmt.excluded.meta,
            "updated_at": datetime.now(),
        },
        where=(Decision.content_hash != stmt.excluded.content_hash),
    )

    result = session.execute(stmt)

    # Check if a row was inserted or updated
    # rowcount will be 1 if inserted or updated, 0 if no change
    if result.rowcount > 0:
        # We can't easily distinguish insert vs update with ON CONFLICT
        # For now, return (True, False) assuming new insert
        # The actual tracking is done via the updated_at field
        return (True, False)
    return (False, False)


def upsert_decisions_batch(
    session: "Session",
    decisions: list["Decision"],
    batch_size: int = 100,
) -> tuple[int, int]:
    """Upsert multiple decisions in batches.

    Args:
        session: SQLModel/SQLAlchemy session
        decisions: List of Decision objects
        batch_size: Number of decisions per batch (default 100)

    Returns:
        Tuple of (inserted_count, updated_count)
    """
    from app.models.decision import Decision

    total_affected = 0

    for i in range(0, len(decisions), batch_size):
        batch = decisions[i : i + batch_size]

        values_list = [
            {
                "id": d.id,
                "source_id": d.source_id,
                "source_name": d.source_name,
                "level": d.level,
                "canton": d.canton,
                "court": d.court,
                "chamber": d.chamber,
                "docket": d.docket,
                "decision_date": d.decision_date,
                "published_date": d.published_date,
                "title": d.title,
                "language": d.language,
                "url": d.url,
                "pdf_url": d.pdf_url,
                "content_text": d.content_text,
                "content_hash": d.content_hash,
                "meta": d.meta,
            }
            for d in batch
        ]

        stmt = pg_insert(Decision).values(values_list)
        stmt = stmt.on_conflict_do_update(
            index_elements=["url"],
            set_={
                "source_id": stmt.excluded.source_id,
                "source_name": stmt.excluded.source_name,
                "level": stmt.excluded.level,
                "canton": stmt.excluded.canton,
                "court": stmt.excluded.court,
                "chamber": stmt.excluded.chamber,
                "docket": stmt.excluded.docket,
                "decision_date": stmt.excluded.decision_date,
                "published_date": stmt.excluded.published_date,
                "title": stmt.excluded.title,
                "language": stmt.excluded.language,
                "pdf_url": stmt.excluded.pdf_url,
                "content_text": stmt.excluded.content_text,
                "content_hash": stmt.excluded.content_hash,
                "meta": stmt.excluded.meta,
                "updated_at": datetime.now(),
            },
            where=(Decision.content_hash != stmt.excluded.content_hash),
        )

        result = session.execute(stmt)
        total_affected += result.rowcount

    session.commit()
    return (total_affected, 0)  # Can't distinguish insert vs update in batch
