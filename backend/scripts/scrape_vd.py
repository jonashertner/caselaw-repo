#!/usr/bin/env python3
"""DEPRECATED: Use scrape_cantons.py scrape_vd_crawler() instead.

This file uses entscheidsuche.ch as an intermediary. The new direct scraper
in scrape_cantons.py accesses vd.ch directly.

---
Original description:
Scrape decisions from Vaud (VD) courts.

Vaud courts publish decisions through the Tribunal cantonal system.
This scraper uses entscheidsuche.ch mirrors for reliable access.

Usage:
    python scripts/scrape_vd.py [--limit N] [--from-date YYYY-MM-DD] [--to-date YYYY-MM-DD]
"""
from __future__ import annotations

import warnings
warnings.warn(
    "scrape_vd.py is deprecated. Use scrape_cantons.py scrape_vd_crawler() instead.",
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

# Rate limiter
rate_limiter = RateLimiter(requests_per_second=2.0)

# Vaud court mappings
VD_COURTS = {
    "VD_TC": "Tribunal cantonal",
    "VD_CA": "Cour d'appel civile",
    "VD_CP": "Cour d'appel pénale",
    "VD_CDP": "Cour de droit public",
}


def get_court_from_doc_id(doc_id: str) -> str:
    """Extract court name from document ID."""
    for prefix, court in VD_COURTS.items():
        if doc_id.startswith(prefix):
            return court
    return "Tribunal cantonal"


def scrape_vd(
    limit: int | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> int:
    """Scrape decisions from Vaud courts."""
    print("Scraping Vaud (VD) courts...")

    if to_date is None:
        to_date = date.today()
    if from_date is None:
        from_date = date(1990, 1, 1)

    print(f"  Date range: {from_date} to {to_date}")

    stats = ScraperStats()

    with get_session() as session:
        existing_count = session.exec(select(func.count(Decision.id)).where(
            Decision.source_id == "vd"
        )).one()
        print(f"  Existing VD decisions in DB: {existing_count}")

        search_after = None

        while True:
            rate_limiter.wait()

            query = {
                "bool": {
                    "must": [{"term": {"canton": "VD"}}],
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

                # Extract attachment info
                attachment = src.get("attachment", {})
                content_url = attachment.get("content_url", "")
                url = content_url or f"https://vd.ch/decisions/{doc_id}"

                # Generate stable ID
                stable_id = stable_uuid_url(f"vd:{doc_id}")

                # Check if exists
                existing = session.get(Decision, stable_id)
                if existing:
                    stats.add_skipped()
                    continue

                # Check by URL
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

                # Extract case number
                case_number = None
                case_match = re.search(r"_([A-Z]+-\d{4}-\d+)", doc_id)
                if case_match:
                    case_number = case_match.group(1)

                court = get_court_from_doc_id(doc_id)

                title_obj = src.get("title", {})
                if isinstance(title_obj, dict):
                    title = title_obj.get("fr") or title_obj.get("de") or doc_id
                else:
                    title = str(title_obj) if title_obj else doc_id

                language = attachment.get("language", "fr")

                try:
                    dec = Decision(
                        id=stable_id,
                        source_id="vd",
                        source_name="Vaud Tribunal cantonal",
                        level="cantonal",
                        canton="VD",
                        court=court,
                        chamber=None,
                        docket=case_number,
                        decision_date=decision_date,
                        published_date=None,
                        title=f"VD {case_number}" if case_number else title[:500],
                        language=language,
                        url=url,
                        pdf_url=content_url if content_url.endswith(".pdf") else None,
                        content_text=content,
                        content_hash=compute_hash(content),
                        meta={
                            "source": "vd.ch (via entscheidsuche.ch)",
                            "doc_id": doc_id,
                        },
                    )
                    session.merge(dec)
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
        print(stats.summary("Vaud (VD)"))
        return stats.imported


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape decisions from Vaud courts")
    parser.add_argument("--limit", type=int, help="Max decisions to import")
    parser.add_argument("--from-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to-date", help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    from_dt = date.fromisoformat(args.from_date) if args.from_date else None
    to_dt = date.fromisoformat(args.to_date) if args.to_date else None

    scrape_vd(limit=args.limit, from_date=from_dt, to_date=to_dt)
