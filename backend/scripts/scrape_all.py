#!/usr/bin/env python3
"""Master scraper that runs all court scrapers.

This script coordinates scraping from multiple sources:
- bger.ch (Federal Supreme Court)
- gerichte-zh.ch (Zürich courts)
- Additional cantonal courts (to be added)

Usage:
    python scripts/scrape_all.py [--sources SOURCE1,SOURCE2,...] [--limit N]
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlmodel import select, func
from app.db.session import get_session
from app.models.decision import Decision


def get_current_stats():
    """Get current database statistics."""
    with get_session() as session:
        total = session.exec(select(func.count(Decision.id))).one()

        by_source = session.exec(
            select(Decision.source_id, func.count(Decision.id))
            .group_by(Decision.source_id)
            .order_by(func.count(Decision.id).desc())
        ).all()

        by_canton = session.exec(
            select(Decision.canton, func.count(Decision.id))
            .group_by(Decision.canton)
            .order_by(func.count(Decision.id).desc())
        ).all()

    return total, by_source, by_canton


def scrape_all(sources: list[str] | None = None, limit: int | None = None):
    """Run all specified scrapers."""
    all_sources = ["bger", "zh"]

    if sources:
        sources = [s.strip().lower() for s in sources]
    else:
        sources = all_sources

    print("=" * 60)
    print("Swiss Court Decision Scraper")
    print("=" * 60)

    # Show current stats
    total, by_source, by_canton = get_current_stats()
    print(f"\nCurrent database: {total:,} decisions")
    print("\nTop sources:")
    for source_id, count in by_source[:5]:
        print(f"  {source_id}: {count:,}")

    print(f"\nScraping sources: {', '.join(sources)}")
    print("-" * 60)

    for source in sources:
        if source == "bger":
            print(f"\n>>> Scraping bger.ch (Federal Supreme Court)...")
            from scrape_bger import scrape_bger
            # Scrape last 30 days by default
            to_date = date.today()
            from_date = to_date - timedelta(days=30)
            scrape_bger(from_date=from_date, to_date=to_date, limit=limit)

        elif source == "zh":
            print(f"\n>>> Scraping gerichte-zh.ch (Zürich courts)...")
            from scrape_zh_courts import scrape_zh_courts
            scrape_zh_courts(limit=limit)

        else:
            print(f"\n>>> Unknown source: {source}")

    # Show final stats
    print("\n" + "=" * 60)
    new_total, _, _ = get_current_stats()
    print(f"Final database: {new_total:,} decisions (+{new_total - total:,})")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run all court scrapers")
    parser.add_argument(
        "--sources",
        help="Comma-separated list of sources to scrape (default: all)",
    )
    parser.add_argument("--limit", type=int, help="Max decisions per source")
    args = parser.parse_args()

    sources = args.sources.split(",") if args.sources else None
    scrape_all(sources=sources, limit=args.limit)
