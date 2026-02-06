#!/usr/bin/env python3
"""Scrape decisions from BVGer (Federal Administrative Court).

This scraper fetches BVGer decisions from entscheidsuche.ch API,
which provides comprehensive coverage of administrative court decisions.

The decisions have hierarchy codes like CH_BVGE_001 in entscheidsuche.ch.

Usage:
    python scripts/scrape_bvger.py [--limit N] [--from-date YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import httpx

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlmodel import select, func
from app.db.session import get_session
from app.models.decision import Decision
from app.services.indexer import stable_uuid_url

from scripts.scraper_common import (
    DEFAULT_HEADERS,
    RateLimiter,
    ScraperStats,
    compute_hash,
    retry,
    upsert_decision,
)

API_URL = "https://entscheidsuche.ch/_search.php"
BATCH_SIZE = 100

# Rate limiter: 2 requests per second
rate_limiter = RateLimiter(requests_per_second=2.0)


@retry(max_attempts=3, backoff_base=2.0)
def fetch_bvger_decisions(
    search_after: list | None = None,
    from_date: date | None = None,
    size: int = BATCH_SIZE
) -> dict:
    """Fetch BVGer decisions from entscheidsuche.ch API."""
    rate_limiter.wait()

    # Query for BVGer decisions (hierarchy starts with CH_BVGE)
    query = {
        "bool": {
            "must": [
                {"prefix": {"hierarchy": "CH_BVGE"}}
            ]
        }
    }

    if from_date:
        query["bool"]["must"].append({
            "range": {"date": {"gte": from_date.isoformat()}}
        })

    body = {
        "query": query,
        "size": size,
        "sort": [{"date": "desc"}, {"_id": "asc"}],
        "_source": ["id", "date", "canton", "title", "abstract", "attachment", "hierarchy", "reference"]
    }

    if search_after:
        body["search_after"] = search_after

    resp = httpx.post(API_URL, json=body, timeout=60, headers=DEFAULT_HEADERS)
    resp.raise_for_status()
    return resp.json()


def extract_chamber_from_hierarchy(hierarchy: list[str]) -> str | None:
    """Extract chamber info from hierarchy codes."""
    # CH_BVGE_001 = Abteilung I, CH_BVGE_002 = Abteilung II, etc.
    for h in hierarchy:
        if h.startswith("CH_BVGE_"):
            code = h.replace("CH_BVGE_", "")
            chamber_map = {
                "001": "Abteilung I",
                "002": "Abteilung II",
                "003": "Abteilung III",
                "004": "Abteilung IV",
                "005": "Abteilung V",
                "006": "Abteilung VI",
            }
            return chamber_map.get(code)
    return None


def extract_case_number(doc_id: str) -> str:
    """Extract case number from document ID."""
    # Format: CH_BVGE_001_A-1234-2023_2024-01-15
    parts = doc_id.split("_")
    if len(parts) >= 4:
        return parts[3]  # A-1234-2023
    return doc_id


def get_string_value(value, lang_priority: list[str] = ["de", "fr", "it", "en"]) -> str:
    """Extract string from potentially multilingual dict value."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        # Try preferred languages in order
        for lang in lang_priority:
            if lang in value and value[lang]:
                return str(value[lang])
        # Return any available value
        for v in value.values():
            if v:
                return str(v)
    return str(value)


def detect_language(text: str) -> str:
    """Detect language from text content."""
    try:
        from langdetect import detect
        lang = detect(text[:1000] if len(text) > 1000 else text)
        return {"de": "de", "fr": "fr", "it": "it", "en": "en"}.get(lang, "de")
    except Exception:
        return "de"


def scrape_bvger(
    limit: int | None = None,
    from_date: date | None = None,
) -> int:
    """Scrape BVGer decisions from entscheidsuche.ch.

    Returns:
        Number of decisions imported
    """
    print("Scraping BVGer decisions from entscheidsuche.ch...")
    if from_date:
        print(f"  Date filter: from {from_date}")

    stats = ScraperStats()

    with get_session() as session:
        existing_count = session.exec(
            select(func.count(Decision.id)).where(Decision.source_name == "Bundesverwaltungsgericht")
        ).one()
        print(f"Existing BVGer decisions in DB: {existing_count}")

        search_after = None

        while True:
            try:
                result = fetch_bvger_decisions(search_after, from_date)
            except Exception as e:
                print(f"Error fetching (giving up after retries): {e}")
                stats.add_error()
                break

            hits = result.get("hits", {}).get("hits", [])
            if not hits:
                break

            for hit in hits:
                doc = hit.get("_source", {})
                doc_id = doc.get("id", "")

                # Generate stable ID
                stable_id = stable_uuid_url(f"bvger:{doc_id}")

                # Check if exists
                existing = session.get(Decision, stable_id)
                if existing:
                    stats.add_skipped()
                    search_after = hit.get("sort")
                    continue

                # Extract content (handle multilingual dict values)
                abstract = get_string_value(doc.get("abstract", ""))
                attachment = get_string_value(doc.get("attachment", ""))
                content = f"{abstract}\n\n{attachment}" if abstract and attachment else (abstract or attachment)

                if not content or len(content) < 50:
                    stats.add_skipped()
                    search_after = hit.get("sort")
                    continue

                # Parse date
                decision_date = None
                date_str = doc.get("date")
                if date_str:
                    try:
                        decision_date = date.fromisoformat(date_str[:10])
                    except ValueError:
                        pass

                # Extract metadata
                hierarchy = doc.get("hierarchy", [])
                if isinstance(hierarchy, str):
                    hierarchy = [hierarchy]

                case_number = extract_case_number(doc_id)
                chamber = extract_chamber_from_hierarchy(hierarchy)
                language = detect_language(content)

                # Build URL
                content_url = f"https://entscheidsuche.ch/docs/{doc_id}"

                # Extract title (handle multilingual dict)
                title = get_string_value(doc.get("title")) or f"BVGer {case_number}"
                reference = get_string_value(doc.get("reference"))

                # Create decision
                try:
                    dec = Decision(
                        id=stable_id,
                        source_id="bvger",
                        source_name="Bundesverwaltungsgericht",
                        level="federal",
                        canton=None,
                        court="Bundesverwaltungsgericht",
                        chamber=chamber,
                        docket=case_number,
                        decision_date=decision_date,
                        published_date=None,
                        title=title,
                        language=language,
                        url=content_url,
                        pdf_url=f"{content_url}.pdf" if doc_id else None,
                        content_text=content,
                        content_hash=compute_hash(content),
                        meta={
                            "source": "entscheidsuche.ch",
                            "hierarchy": hierarchy,
                            "reference": reference,
                        },
                    )
                    session.add(dec)
                    stats.add_imported()

                    if stats.imported % 100 == 0:
                        print(f"  Imported {stats.imported} (skipped {stats.skipped})...")
                        session.commit()

                    if limit and stats.imported >= limit:
                        break

                except Exception as e:
                    print(f"  Error saving {doc_id}: {e}")
                    stats.add_error()

                search_after = hit.get("sort")

            if limit and stats.imported >= limit:
                break

        session.commit()
        print(stats.summary("BVGer"))
        return stats.imported


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape BVGer decisions")
    parser.add_argument("--limit", type=int, help="Max decisions to import")
    parser.add_argument("--from-date", help="Start date (YYYY-MM-DD)")
    args = parser.parse_args()

    from_dt = date.fromisoformat(args.from_date) if args.from_date else None
    scrape_bvger(limit=args.limit, from_date=from_dt)
