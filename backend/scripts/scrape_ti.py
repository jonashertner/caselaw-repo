#!/usr/bin/env python3
"""DEPRECATED: Use scrape_cantons.py scrape_ti_crawler() instead.

This file uses entscheidsuche.ch as an intermediary. The new direct scraper
in scrape_cantons.py accesses sentenze.ti.ch directly.

---
Original description:
Scrape decisions from Ticino (TI) courts.

Ticino courts publish decisions through sentenze.ti.ch.
This scraper uses entscheidsuche.ch mirrors for reliable access.

Usage:
    python scripts/scrape_ti.py [--limit N] [--from-date YYYY-MM-DD] [--to-date YYYY-MM-DD]
"""
from __future__ import annotations

import warnings
warnings.warn(
    "scrape_ti.py is deprecated. Use scrape_cantons.py scrape_ti_crawler() instead.",
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


def scrape_ti(
    limit: int | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> int:
    """Scrape decisions from Ticino courts."""
    print("Scraping Ticino (TI) courts...")

    if to_date is None:
        to_date = date.today()
    if from_date is None:
        from_date = date(1990, 1, 1)

    print(f"  Date range: {from_date} to {to_date}")

    stats = ScraperStats()

    with get_session() as session:
        existing_count = session.exec(select(func.count(Decision.id)).where(
            Decision.source_id == "ti"
        )).one()
        print(f"  Existing TI decisions in DB: {existing_count}")

        search_after = None

        while True:
            rate_limiter.wait()

            query = {
                "bool": {
                    "must": [{"term": {"canton": "TI"}}],
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
                url = content_url or f"https://sentenze.ti.ch/{doc_id}"

                # Generate stable ID
                stable_id = stable_uuid_url(f"ti:{doc_id}")

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
                case_match = re.search(r"_(\d+[._]\d+)", doc_id)
                if case_match:
                    case_number = case_match.group(1)

                title_obj = src.get("title", {})
                if isinstance(title_obj, dict):
                    title = title_obj.get("it") or title_obj.get("de") or title_obj.get("fr") or doc_id
                else:
                    title = str(title_obj) if title_obj else doc_id

                # Ticino is Italian
                language = attachment.get("language", "it")

                try:
                    dec = Decision(
                        id=stable_id,
                        source_id="ti",
                        source_name="Ticino Tribunali",
                        level="cantonal",
                        canton="TI",
                        court="Tribunale cantonale",
                        chamber=None,
                        docket=case_number,
                        decision_date=decision_date,
                        published_date=None,
                        title=f"TI {case_number}" if case_number else title[:500],
                        language=language,
                        url=url,
                        pdf_url=content_url if content_url.endswith(".pdf") else None,
                        content_text=content,
                        content_hash=compute_hash(content),
                        meta={
                            "source": "sentenze.ti.ch (via entscheidsuche.ch)",
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
        print(stats.summary("Ticino (TI)"))
        return stats.imported


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape decisions from Ticino courts")
    parser.add_argument("--limit", type=int, help="Max decisions to import")
    parser.add_argument("--from-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to-date", help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    from_dt = date.fromisoformat(args.from_date) if args.from_date else None
    to_dt = date.fromisoformat(args.to_date) if args.to_date else None

    scrape_ti(limit=args.limit, from_date=from_dt, to_date=to_dt)
