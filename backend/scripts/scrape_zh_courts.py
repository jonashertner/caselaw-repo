#!/usr/bin/env python3
"""Scrape decisions from Zürich cantonal courts (gerichte-zh.ch).

This scraper fetches decisions directly from the Zürich court website,
which may have decisions not indexed by entscheidsuche.ch.

Usage:
    python scripts/scrape_zh_courts.py [--limit N] [--from-date YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import re
import sys
from datetime import date, timedelta
from pathlib import Path

import httpx
from bs4 import BeautifulSoup

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
    extract_pdf_text,
    parse_date_flexible,
    retry,
    upsert_decision,
)

BASE_URL = "https://www.gerichte-zh.ch"
DECISIONS_URL = f"{BASE_URL}/entscheide/entscheide-anzeigen.html"

# Rate limiter: 2 requests per second
rate_limiter = RateLimiter(requests_per_second=2.0)


@retry(max_attempts=3, backoff_base=2.0)
def fetch_page(url: str, timeout: int = 60) -> httpx.Response:
    """Fetch a page with retry logic."""
    rate_limiter.wait()
    resp = httpx.get(url, headers=DEFAULT_HEADERS, timeout=timeout, follow_redirects=True)
    resp.raise_for_status()
    return resp


def fetch_decision_list_page(page: int = 1) -> list[dict]:
    """Fetch list of decisions from a specific page of the ZH court website."""
    if page == 1:
        url = DECISIONS_URL
    else:
        url = f"{DECISIONS_URL}?tx_frpentscheidsammlungextended_pi1%5Bseite%5D={page}"

    resp = fetch_page(url)

    soup = BeautifulSoup(resp.text, "html.parser")

    decisions = []
    for link in soup.find_all("a", href=True):
        href = link.get("href", "")
        if href.endswith(".pdf") and "/entscheide/" in href:
            # Extract case info from filename
            filename = href.split("/")[-1].replace(".pdf", "")

            # Get link text for additional info
            text = link.get_text(strip=True)

            # Build full URL
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"

            # Parse date from parent elements
            parent = link.find_parent("tr") or link.find_parent("div")
            date_str = None
            if parent:
                date_match = re.search(r"(\d{2}\.\d{2}\.\d{4})", parent.get_text())
                if date_match:
                    date_str = date_match.group(1)

            # Parse court from filename
            court = "Obergericht"
            if "SB" in filename or "PS" in filename:
                court = "Strafkammer"
            elif "ZK" in filename or "PA" in filename or "PQ" in filename:
                court = "Zivilkammer"
            elif "ZMP" in filename:
                court = "Mietgericht"

            decisions.append({
                "filename": filename,
                "title": text or filename,
                "url": full_url,
                "date_str": date_str,
                "court": court,
            })

    return decisions


def fetch_all_decisions(
    max_pages: int = 100,
    from_date: date | None = None,
    to_date: date | None = None,
) -> list[dict]:
    """Fetch decisions from all pages.

    Args:
        max_pages: Maximum number of pages to fetch
        from_date: Only include decisions on or after this date
        to_date: Only include decisions on or before this date

    Returns:
        List of decision dictionaries
    """
    all_decisions = []
    seen_filenames = set()
    stop_early = False

    for page in range(1, max_pages + 1):
        print(f"  Fetching page {page}...")
        try:
            decisions = fetch_decision_list_page(page)
        except Exception as e:
            print(f"  Error fetching page {page}: {e}")
            break

        if not decisions:
            print(f"  No more decisions found")
            break

        # Check for duplicates (means we've looped)
        new_decisions = []
        for d in decisions:
            if d["filename"] not in seen_filenames:
                seen_filenames.add(d["filename"])

                # Parse and apply date filter
                decision_date = None
                if d["date_str"]:
                    decision_date = parse_date_flexible(d["date_str"])
                    d["decision_date"] = decision_date

                # Apply date filters
                if from_date and decision_date and decision_date < from_date:
                    # Decisions are typically sorted newest first
                    # If we find one older than from_date, we can stop
                    stop_early = True
                    continue

                if to_date and decision_date and decision_date > to_date:
                    continue

                new_decisions.append(d)

        if not new_decisions and stop_early:
            print(f"  Reached decisions before {from_date}, stopping")
            break

        if not new_decisions:
            print(f"  All decisions already seen, stopping")
            break

        all_decisions.extend(new_decisions)

    return all_decisions


def scrape_zh_courts(
    limit: int | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> int:
    """Scrape decisions from Zürich courts.

    Args:
        limit: Maximum number of decisions to import
        from_date: Only import decisions on or after this date
        to_date: Only import decisions on or before this date

    Returns:
        Number of decisions imported
    """
    print("Scraping gerichte-zh.ch...")

    # Default to_date to today
    if to_date is None:
        to_date = date.today()

    # If from_date is specified, print the filter
    if from_date:
        print(f"  Date filter: {from_date} to {to_date}")
    else:
        print("  Full historical import (no date filter)")

    stats = ScraperStats()

    with get_session() as session:
        existing_count = session.exec(select(func.count(Decision.id))).one()
        print(f"Existing decisions in DB: {existing_count}")

        print("Fetching decision list from all pages...")
        decisions = fetch_all_decisions(from_date=from_date, to_date=to_date)
        print(f"Found {len(decisions)} unique decisions in date range")

        for dec_info in decisions:
            # Generate stable ID
            stable_id = stable_uuid_url(f"gerichte-zh:{dec_info['filename']}")

            # Check if exists
            existing = session.get(Decision, stable_id)
            if existing:
                stats.add_skipped()
                continue

            print(f"  Fetching {dec_info['filename']}...")

            # Download PDF with retry
            try:
                resp = fetch_page(dec_info["url"], timeout=120)
                pdf_content = resp.content
            except Exception as e:
                print(f"    Error downloading: {e}")
                stats.add_error()
                continue

            # Extract text
            content = extract_pdf_text(pdf_content)
            if not content or len(content) < 100:
                print(f"    No text content, skipping")
                stats.add_skipped()
                continue

            # Use pre-parsed date from fetch_all_decisions
            decision_date = dec_info.get("decision_date")

            # Create decision
            try:
                dec = Decision(
                    id=stable_id,
                    source_id="zh",
                    source_name="Zürich",
                    level="cantonal",
                    canton="ZH",
                    court=dec_info["court"],
                    chamber=None,
                    docket=dec_info["filename"],
                    decision_date=decision_date,
                    published_date=None,
                    title=dec_info["title"][:500],
                    language="de",
                    url=dec_info["url"],
                    pdf_url=dec_info["url"],
                    content_text=content,
                    content_hash=compute_hash(content),
                    meta={
                        "source": "gerichte-zh.ch",
                        "filename": dec_info["filename"],
                    },
                )
                session.add(dec)
                stats.add_imported()

                if stats.imported % 10 == 0:
                    print(f"  Imported {stats.imported} (skipped {stats.skipped})...")
                    session.commit()

                if limit and stats.imported >= limit:
                    break

            except Exception as e:
                print(f"    Error saving: {e}")
                stats.add_error()
                continue

        session.commit()
        print(stats.summary("Zürich Courts"))
        return stats.imported


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape decisions from gerichte-zh.ch")
    parser.add_argument("--limit", type=int, help="Max decisions to import")
    parser.add_argument("--from-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to-date", help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    from_dt = date.fromisoformat(args.from_date) if args.from_date else None
    to_dt = date.fromisoformat(args.to_date) if args.to_date else None

    scrape_zh_courts(limit=args.limit, from_date=from_dt, to_date=to_dt)
