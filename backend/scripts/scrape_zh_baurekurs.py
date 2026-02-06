#!/usr/bin/env python3
"""Scrape decisions from Zürich Baurekursgericht (Construction Appeals Court).

The ZH Construction Appeals Court publishes decisions with headnotes and
appealability information.

Usage:
    python scripts/scrape_zh_baurekurs.py [--limit N] [--from-date YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import re
import sys
from datetime import date
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

BASE_URL = "https://www.baurekursgericht-zh.ch"
SEARCH_URL = f"{BASE_URL}/rechtsprechung/entscheiddatenbank/volltextsuche/"
RESULTS_PER_PAGE = 10

# Rate limiter: 2 requests per second
rate_limiter = RateLimiter(requests_per_second=2.0)


@retry(max_attempts=3, backoff_base=2.0)
def fetch_page(url: str, timeout: int = 60, data: dict | None = None) -> httpx.Response:
    """Fetch a page with retry logic."""
    rate_limiter.wait()
    if data:
        resp = httpx.post(url, headers=DEFAULT_HEADERS, data=data, timeout=timeout, follow_redirects=True)
    else:
        resp = httpx.get(url, headers=DEFAULT_HEADERS, timeout=timeout, follow_redirects=True)
    resp.raise_for_status()
    return resp


def fetch_decisions_page(page: int = 0, from_date_str: str = "") -> tuple[list[dict], int]:
    """Fetch a page of search results.

    Args:
        page: Page number (0-indexed)
        from_date_str: Optional start date filter (DD.MM.YYYY format)

    Returns:
        Tuple of (decisions list, total count)
    """
    form_data = {
        "keywords": "",
        "source": "2",
        "datefrom": from_date_str,
        "dateto": "",
        "search_type": "2",
    }

    if page > 0:
        form_data["start"] = str(RESULTS_PER_PAGE * page)

    try:
        resp = fetch_page(SEARCH_URL, data=form_data)
    except Exception as e:
        print(f"Error fetching page {page}: {e}")
        return [], 0

    soup = BeautifulSoup(resp.text, "html.parser")

    # Get total count
    total = 0
    count_div = soup.select_one("div.search-listing-head div.col-6")
    if count_div:
        match = re.search(r"(\d+)\s+Entscheide", count_div.get_text())
        if match:
            total = int(match.group(1))

    # Parse decisions
    decisions = []
    for item in soup.select("div.search-listing-item"):
        try:
            # Get case number and date
            meta_elem = item.select_one("div.search-listing-item-number")
            if not meta_elem:
                continue

            meta_text = meta_elem.get_text(strip=True)
            parts = meta_text.split(" vom ")

            if len(parts) != 2:
                continue

            docket = parts[0].strip()
            decision_date = parse_date_flexible(parts[1].strip())

            # Get title
            title_elem = item.select_one("h4")
            title = title_elem.get_text(strip=True) if title_elem else docket

            # Get headnote/summary
            headnote = None
            summary_elem = item.select_one("div.search-listing-item-summary")
            if summary_elem:
                headnote = " ".join(p.get_text(strip=True) for p in summary_elem.select("p"))

            # Get appealability
            appealability = None
            legal_elem = item.select_one("div.search-listing-item-legal")
            if legal_elem:
                appealability = legal_elem.get_text(strip=True)

            # Get PDF URL
            pdf_link = item.select_one("div.search-listing-item-download a")
            if not pdf_link:
                continue

            pdf_url = pdf_link.get("href", "")
            if not pdf_url:
                continue
            if not pdf_url.startswith("http"):
                pdf_url = f"{BASE_URL}{pdf_url}"

            decisions.append({
                "docket": docket,
                "decision_date": decision_date,
                "title": title[:500],
                "headnote": headnote,
                "appealability": appealability,
                "pdf_url": pdf_url,
            })

        except Exception as e:
            print(f"    Error parsing item: {e}")
            continue

    return decisions, total


def scrape_zh_baurekurs(
    limit: int | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> int:
    """Scrape decisions from ZH Baurekursgericht.

    Args:
        limit: Maximum number of decisions to import
        from_date: Only import decisions on or after this date
        to_date: Only import decisions on or before this date

    Returns:
        Number of decisions imported
    """
    print("Scraping baurekursgericht-zh.ch (Zürich Construction Appeals Court)...")

    if to_date is None:
        to_date = date.today()

    if from_date:
        print(f"  Date filter: {from_date} to {to_date}")
        from_date_str = from_date.strftime("%d.%m.%Y")
    else:
        print("  Full historical import (no date filter)")
        from_date_str = ""

    stats = ScraperStats()

    with get_session() as session:
        existing_count = session.exec(select(func.count(Decision.id))).one()
        print(f"Existing decisions in DB: {existing_count}")

        # Fetch first page to get total count
        print("Fetching decision list...")
        first_page_decisions, total_count = fetch_decisions_page(0, from_date_str)
        print(f"Found {total_count} total decisions")

        if total_count == 0:
            print("No decisions found")
            return 0

        # Calculate pages
        total_pages = (total_count + RESULTS_PER_PAGE - 1) // RESULTS_PER_PAGE

        # Process all pages
        all_decisions = first_page_decisions
        for page in range(1, total_pages):
            print(f"  Fetching page {page + 1}/{total_pages}...")
            page_decisions, _ = fetch_decisions_page(page, from_date_str)
            all_decisions.extend(page_decisions)

            if limit and len(all_decisions) >= limit * 2:
                break

        print(f"Collected {len(all_decisions)} decision references")

        for dec_info in all_decisions:
            # Apply date filter (in case of server-side filtering issues)
            if dec_info["decision_date"]:
                if from_date and dec_info["decision_date"] < from_date:
                    stats.add_skipped()
                    continue
                if to_date and dec_info["decision_date"] > to_date:
                    stats.add_skipped()
                    continue

            # Generate stable ID
            stable_id = stable_uuid_url(f"zh-baurekurs:{dec_info['docket']}")

            # Check if exists
            existing = session.get(Decision, stable_id)
            if existing:
                stats.add_skipped()
                continue

            print(f"  Fetching {dec_info['docket']}...")

            # Download PDF
            try:
                resp = fetch_page(dec_info["pdf_url"], timeout=120)
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

            # Build metadata
            meta = {
                "source": "baurekursgericht-zh.ch",
                "legal_area": "Baurecht",
            }
            if dec_info["headnote"]:
                meta["headnote"] = dec_info["headnote"]
            if dec_info["appealability"]:
                meta["appealability"] = dec_info["appealability"]

            # Create decision
            try:
                dec = Decision(
                    id=stable_id,
                    source_id="zh_baurekurs",
                    source_name="Zürich Baurekursgericht",
                    level="cantonal",
                    canton="ZH",
                    court="Baurekursgericht",
                    chamber=None,
                    docket=dec_info["docket"],
                    decision_date=dec_info["decision_date"],
                    published_date=None,
                    title=dec_info["title"],
                    language="de",
                    url=dec_info["pdf_url"],
                    pdf_url=dec_info["pdf_url"],
                    content_text=content,
                    content_hash=compute_hash(content),
                    meta=meta,
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
        print(stats.summary("ZH Baurekurs"))
        return stats.imported


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape decisions from baurekursgericht-zh.ch")
    parser.add_argument("--limit", type=int, help="Max decisions to import")
    parser.add_argument("--from-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to-date", help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    from_dt = date.fromisoformat(args.from_date) if args.from_date else None
    to_dt = date.fromisoformat(args.to_date) if args.to_date else None

    scrape_zh_baurekurs(limit=args.limit, from_date=from_dt, to_date=to_dt)
