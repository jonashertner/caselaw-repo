#!/usr/bin/env python3
"""DEPRECATED: Use scrape_cantons.py scrape_ge_crawler() instead.

This file uses entscheidsuche.ch as an intermediary. The new direct scraper
in scrape_cantons.py accesses justice.ge.ch directly.

---
Original description:
Scrape decisions from Geneva (GE) courts.

Geneva courts publish decisions through justice.ge.ch. This scraper uses
entscheidsuche.ch mirrors for reliable access to decision content.

The Geneva judicial system includes:
- Cour de Justice (Court of Justice) - highest cantonal court
- Tribunal civil (Civil Tribunal)
- Tribunal pénal (Criminal Tribunal)
- Chambre administrative (Administrative Chamber)
- Tribunal des baux et loyers (Rental Tribunal)

Usage:
    python scripts/scrape_ge_direct.py [--limit N] [--from-date YYYY-MM-DD] [--to-date YYYY-MM-DD]
"""
from __future__ import annotations

import warnings
warnings.warn(
    "scrape_ge_direct.py is deprecated. Use scrape_cantons.py scrape_ge_crawler() instead.",
    DeprecationWarning,
    stacklevel=2
)

import argparse
import re
import sys
from datetime import date
from pathlib import Path
from typing import Any

import httpx

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlmodel import select, func
from app.db.session import get_session
from app.models.decision import Decision
from app.services.indexer import stable_uuid_url

from scripts.scraper_common import (
    RateLimiter,
    ScraperStats,
    compute_hash,
    extract_pdf_text,
    parse_date_flexible,
    upsert_decision,
)

# API endpoint
API_URL = "https://entscheidsuche.ch/_search.php"
BATCH_SIZE = 100

# Rate limiter: 2 requests per second
rate_limiter = RateLimiter(requests_per_second=2.0)

# Geneva court mappings based on document ID patterns
GE_COURTS = {
    "GE_CJ": "Cour de Justice",
    "GE_TC": "Tribunal civil",
    "GE_TP": "Tribunal pénal",
    "GE_CA": "Chambre administrative",
    "GE_TBL": "Tribunal des baux et loyers",
    "GE_TAPI": "Tribunal administratif de première instance",
    "GE_CPP": "Commission de police",
}

# Chamber codes in document IDs
GE_CHAMBERS = {
    "001": "1ère Chambre",
    "002": "2ème Chambre",
    "003": "3ème Chambre",
    "004": "4ème Chambre",
    "005": "5ème Chambre",
    "006": "6ème Chambre",
    "007": "7ème Chambre",
    "008": "8ème Chambre",
    "009": "9ème Chambre",
    "010": "10ème Chambre",
    "011": "11ème Chambre",
    "012": "12ème Chambre",
    "013": "13ème Chambre",
}


def get_court_from_doc_id(doc_id: str) -> str | None:
    """Extract court name from document ID."""
    for prefix, court in GE_COURTS.items():
        if doc_id.startswith(prefix):
            return court
    return "Tribunal cantonal"


def get_chamber_from_doc_id(doc_id: str) -> str | None:
    """Extract chamber from document ID."""
    # Pattern: GE_CJ_013_A-1793-2025
    match = re.search(r"GE_[A-Z]+_(\d{3})_", doc_id)
    if match:
        return GE_CHAMBERS.get(match.group(1))
    return None


def scrape_ge_direct(
    limit: int | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> int:
    """Scrape decisions from Geneva courts via entscheidsuche.ch mirrors.

    Args:
        limit: Maximum number of decisions to import
        from_date: Only import decisions on or after this date
        to_date: Only import decisions on or before this date

    Returns:
        Number of decisions imported
    """
    print("Scraping Geneva (GE) courts...")

    if to_date is None:
        to_date = date.today()
    if from_date is None:
        # Geneva decisions available from ~1998
        from_date = date(1998, 1, 1)

    print(f"  Date range: {from_date} to {to_date}")

    stats = ScraperStats()

    with get_session() as session:
        existing_count = session.exec(select(func.count(Decision.id)).where(
            Decision.source_id == "ge"
        )).one()
        print(f"  Existing GE decisions in DB: {existing_count}")

        search_after = None

        while True:
            rate_limiter.wait()

            # Query for Geneva decisions
            query = {
                "bool": {
                    "must": [
                        {"term": {"canton": "GE"}},
                    ],
                    "filter": [
                        {"range": {"date": {"gte": from_date.isoformat(), "lte": to_date.isoformat()}}}
                    ]
                }
            }

            body: dict[str, Any] = {
                "query": query,
                "size": BATCH_SIZE,
                "sort": [{"date": "desc"}, {"_id": "asc"}],
                "_source": ["id", "date", "canton", "title", "abstract", "attachment", "hierarchy", "reference"]
            }

            if search_after:
                body["search_after"] = search_after

            try:
                resp = httpx.post(API_URL, json=body, timeout=60)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                print(f"  Error fetching: {e}")
                stats.add_error()
                break

            hits = data.get("hits", {}).get("hits", [])
            if not hits:
                break

            search_after = hits[-1].get("sort")

            for hit in hits:
                if limit and stats.imported >= limit:
                    break

                src = hit.get("_source", {})
                doc_id = src.get("id") or hit.get("_id")

                # Extract attachment info first (needed for URL check)
                attachment = src.get("attachment", {})
                content_url = attachment.get("content_url", "")
                url = content_url or f"https://justice.ge.ch/apps/decis/{doc_id}"

                # Generate stable ID
                stable_id = stable_uuid_url(f"ge:{doc_id}")

                # Check if exists by ID
                existing = session.get(Decision, stable_id)
                if existing:
                    stats.add_skipped()
                    continue

                # Also check by URL (for records imported via entscheidsuche.ch importer)
                existing_by_url = session.exec(
                    select(Decision).where(Decision.url == url)
                ).first()
                if existing_by_url:
                    stats.add_skipped()
                    continue

                # Download PDF if available
                content = None
                if content_url and content_url.endswith(".pdf"):
                    try:
                        rate_limiter.wait()
                        pdf_resp = httpx.get(content_url, timeout=120, follow_redirects=True)
                        pdf_resp.raise_for_status()
                        content = extract_pdf_text(pdf_resp.content)
                    except Exception as e:
                        print(f"    Error downloading PDF: {e}")
                else:
                    # Use pre-extracted content
                    content = attachment.get("content", "")

                if not content or len(content) < 100:
                    stats.add_skipped()
                    continue

                # Parse date
                date_str = src.get("date")
                decision_date = None
                if date_str:
                    try:
                        decision_date = date.fromisoformat(date_str)
                    except ValueError:
                        decision_date = parse_date_flexible(date_str)

                # Extract case number: GE_CJ_013_A-1793-2025
                case_number = None
                case_match = re.search(r"_([A-Z]+-?\d+-\d{4})(?:_|$)", doc_id)
                if case_match:
                    case_number = case_match.group(1)

                # Get court and chamber
                court = get_court_from_doc_id(doc_id)
                chamber = get_chamber_from_doc_id(doc_id)

                # Get title
                title_obj = src.get("title", {})
                if isinstance(title_obj, dict):
                    title = title_obj.get("fr") or title_obj.get("de") or title_obj.get("it") or doc_id
                else:
                    title = str(title_obj) if title_obj else doc_id

                # Get language (Geneva is primarily French)
                language = attachment.get("language", "fr")

                try:
                    dec = Decision(
                        id=stable_id,
                        source_id="ge",
                        source_name="Genève Pouvoir judiciaire",
                        level="cantonal",
                        canton="GE",
                        court=court,
                        chamber=chamber,
                        docket=case_number,
                        decision_date=decision_date,
                        published_date=None,
                        title=f"GE {case_number}" if case_number else title[:500],
                        language=language,
                        url=url,
                        pdf_url=content_url if content_url.endswith(".pdf") else None,
                        content_text=content,
                        content_hash=compute_hash(content),
                        meta={
                            "source": "justice.ge.ch (via entscheidsuche.ch)",
                            "doc_id": doc_id,
                            "hierarchy": src.get("hierarchy"),
                        },
                    )
                    session.merge(dec)  # Use merge to handle existing records
                    stats.add_imported()

                    if stats.imported % 100 == 0:
                        print(f"  Imported {stats.imported} (skipped {stats.skipped})...")
                        session.commit()

                except Exception as e:
                    print(f"  Error saving: {e}")
                    stats.add_error()
                    continue

            if limit and stats.imported >= limit:
                break

        session.commit()
        print(stats.summary("Geneva (GE)"))
        return stats.imported


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape decisions from Geneva courts")
    parser.add_argument("--limit", type=int, help="Max decisions to import")
    parser.add_argument("--from-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to-date", help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    from_dt = date.fromisoformat(args.from_date) if args.from_date else None
    to_dt = date.fromisoformat(args.to_date) if args.to_date else None

    scrape_ge_direct(
        limit=args.limit,
        from_date=from_dt,
        to_date=to_dt,
    )
