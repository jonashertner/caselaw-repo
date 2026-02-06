#!/usr/bin/env python3
"""Scrape decisions from BStGer (Federal Criminal Court).

The Federal Criminal Court (Bundesstrafgericht/BStGer) publishes decisions
through bstger.weblaw.ch. This scraper uses entscheidsuche.ch mirrors for
reliable access to decision content.

Usage:
    python scripts/scrape_bstger_direct.py [--limit N] [--from-date YYYY-MM-DD] [--to-date YYYY-MM-DD]
"""
from __future__ import annotations

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

# BStGer chamber mappings based on case number prefix
BSTGER_CHAMBERS = {
    "SK": "Strafkammer",
    "CA": "Berufungskammer",
    "BB": "Beschwerdekammer",
    "BH": "Beschwerdekammer (Haft)",
    "BP": "Beschwerdekammer (Personalia)",
    "RR": "Beschwerdekammer (Rechtshilfe)",
    "RP": "Beschwerdekammer (Rechtshilfe Personalia)",
    "RH": "Beschwerdekammer (Rechtshilfe Haft)",
    "CR": "Beschwerdekammer (Korruption)",
    "SN": "Strafkammer (Nebenklagen)",
}


def get_chamber_from_case_number(case_number: str | None) -> str | None:
    """Extract chamber from case number prefix."""
    if not case_number:
        return None
    prefix = case_number.split("-")[0] if "-" in case_number else case_number[:2]
    return BSTGER_CHAMBERS.get(prefix.upper())


def scrape_bstger_direct(
    limit: int | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> int:
    """Scrape decisions from BStGer via entscheidsuche.ch mirrors.

    Args:
        limit: Maximum number of decisions to import
        from_date: Only import decisions on or after this date
        to_date: Only import decisions on or before this date

    Returns:
        Number of decisions imported
    """
    print("Scraping Bundesstrafgericht (Federal Criminal Court)...")

    if to_date is None:
        to_date = date.today()
    if from_date is None:
        # BStGer established in 2004
        from_date = date(2004, 1, 1)

    print(f"  Date range: {from_date} to {to_date}")

    stats = ScraperStats()

    with get_session() as session:
        existing_count = session.exec(select(func.count(Decision.id)).where(
            Decision.source_id == "bstger"
        )).one()
        print(f"  Existing BStGer decisions in DB: {existing_count}")

        search_after = None

        while True:
            rate_limiter.wait()

            # Query for BStGer decisions - identified by ID pattern CH_BSTG_*
            query = {
                "bool": {
                    "must": [
                        {"term": {"canton": "CH"}},
                        {"prefix": {"id": "CH_BSTG_"}},
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
                url = content_url or f"https://bstger.weblaw.ch/cache/{doc_id}"

                # Generate stable ID
                stable_id = stable_uuid_url(f"bstger:{doc_id}")

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

                # Extract case number: CH_BSTG_001_RR-2023-127_2026-01-20
                case_number = None
                case_match = re.search(r"([A-Z]{2}-\d{4}-\d+)", doc_id)
                if case_match:
                    case_number = case_match.group(1)

                # Get chamber from case number
                chamber = get_chamber_from_case_number(case_number)

                # Get title
                title_obj = src.get("title", {})
                title = title_obj.get("de") or title_obj.get("fr") or title_obj.get("it") or doc_id

                # Get language
                language = attachment.get("language", "de")

                try:
                    dec = Decision(
                        id=stable_id,
                        source_id="bstger",
                        source_name="Bundesstrafgericht",
                        level="federal",
                        canton=None,
                        court="Bundesstrafgericht",
                        chamber=chamber,
                        docket=case_number,
                        decision_date=decision_date,
                        published_date=None,
                        title=f"BStGer {case_number}" if case_number else title[:500],
                        language=language,
                        url=url,
                        pdf_url=content_url if content_url.endswith(".pdf") else None,
                        content_text=content,
                        content_hash=compute_hash(content),
                        meta={
                            "source": "bstger.weblaw.ch (via entscheidsuche.ch)",
                            "doc_id": doc_id,
                            "hierarchy": src.get("hierarchy"),
                        },
                    )
                    session.add(dec)
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
        print(stats.summary("BStGer"))
        return stats.imported


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape decisions from BStGer")
    parser.add_argument("--limit", type=int, help="Max decisions to import")
    parser.add_argument("--from-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to-date", help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    from_dt = date.fromisoformat(args.from_date) if args.from_date else None
    to_dt = date.fromisoformat(args.to_date) if args.to_date else None

    scrape_bstger_direct(
        limit=args.limit,
        from_date=from_dt,
        to_date=to_dt,
    )
