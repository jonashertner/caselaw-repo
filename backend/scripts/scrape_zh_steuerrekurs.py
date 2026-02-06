#!/usr/bin/env python3
"""Scrape decisions from Zürich Steuerrekursgericht (Tax Appeals Court).

The ZH Tax Appeals Court (strgzh.ch) publishes decisions with rich metadata
including legal norms cited, headnotes, and appealability information.

Usage:
    python scripts/scrape_zh_steuerrekurs.py [--limit N] [--from-date YYYY-MM-DD]
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
    MONTH_NAMES_DE,
    upsert_decision,
)

BASE_URL = "https://www.strgzh.ch"
SEARCH_URL = f"{BASE_URL}/entscheide/datenbank/verfahrensnummersuche"
RESULTS_PER_PAGE = 10

# Rate limiter: 2 requests per second
rate_limiter = RateLimiter(requests_per_second=2.0)

# Regex for parsing case metadata
# Format: "ST.2024.123, ST.2024.124 / 15. Januar 2024"
MONTH_PATTERN = "|".join(MONTH_NAMES_DE.keys())
RE_META = re.compile(
    rf"^(?P<Num>[A-Z]+\.\d{{4}}\.\d+)"
    rf"(?:,\s*(?P<Num2>[A-Z]+\.\d{{4}}\.\d+))?"
    rf"(?:,\s*(?P<Num3>[A-Z]+\.\d{{4}}\.\d+))?"
    rf"\s*/\s*(?P<Datum>\d{{1,2}}\.?\s*(?:{MONTH_PATTERN})\s+\d{{4}})$",
    re.IGNORECASE
)


@retry(max_attempts=3, backoff_base=2.0)
def fetch_page(url: str, timeout: int = 60) -> httpx.Response:
    """Fetch a page with retry logic."""
    rate_limiter.wait()
    resp = httpx.get(url, headers=DEFAULT_HEADERS, timeout=timeout, follow_redirects=True)
    resp.raise_for_status()
    return resp


def parse_decision_from_element(element, soup_text: str) -> dict | None:
    """Parse a decision from a search result element."""
    try:
        # Get citation title (case number and date)
        cit_title = element.select_one("p.cit-title")
        if not cit_title:
            return None

        meta_text = cit_title.get_text(strip=True)
        meta_match = RE_META.search(meta_text)

        if not meta_match:
            # Try simpler pattern
            simple_match = re.search(r"([A-Z]+\.\d{4}\.\d+)", meta_text)
            date_match = re.search(r"(\d{1,2}\.?\s*\w+\s+\d{4})", meta_text)

            docket = simple_match.group(1) if simple_match else None
            decision_date = parse_date_flexible(date_match.group(1)) if date_match else None
            additional_dockets = []
        else:
            docket = meta_match.group("Num")
            decision_date = parse_date_flexible(meta_match.group("Datum"))
            additional_dockets = [
                meta_match.group(f"Num{i}")
                for i in [2, 3]
                if meta_match.group(f"Num{i}")
            ]

        # Get PDF URL
        title_link = element.select_one("h2.ruling__title a")
        if not title_link:
            return None

        pdf_url = title_link.get("href", "")
        if not pdf_url:
            return None
        if not pdf_url.startswith("http"):
            pdf_url = f"{BASE_URL}{pdf_url}"

        title = title_link.get_text(strip=True)

        # Get legal norms cited
        norms_elem = element.select_one("p.legal_foundation")
        norms = norms_elem.get_text(strip=True) if norms_elem else None

        # Get headnote/summary
        headnote = None
        summary_elem = element.select_one("p.legal_foundation + p:not([class])")
        if summary_elem:
            headnote = summary_elem.get_text(strip=True)

        # Get appealability note
        appealability = None
        note_elem = element.select_one("p.note")
        if note_elem:
            appealability = note_elem.get_text(strip=True)

        return {
            "docket": docket,
            "additional_dockets": additional_dockets,
            "decision_date": decision_date,
            "title": title[:500] if title else docket,
            "pdf_url": pdf_url,
            "norms": norms,
            "headnote": headnote,
            "appealability": appealability,
        }

    except Exception as e:
        print(f"    Error parsing element: {e}")
        return None


def fetch_decisions_page(page: int = 1) -> tuple[list[dict], int]:
    """Fetch a page of search results.

    Returns:
        Tuple of (decisions list, total count)
    """
    url = f"{SEARCH_URL}?subject=&year=&number=&submit=Suchen&page={page}"

    try:
        resp = fetch_page(url)
    except Exception as e:
        print(f"Error fetching page {page}: {e}")
        return [], 0

    soup = BeautifulSoup(resp.text, "html.parser")

    # Get total count
    total_text = soup.find("p", string=re.compile(r"\d+ Entscheide gefunden"))
    total = 0
    if total_text:
        match = re.search(r"(\d+)\s+Entscheide", total_text.get_text())
        if match:
            total = int(match.group(1))

    # Parse decisions
    decisions = []
    for ruling in soup.select("div.box.ruling"):
        if ruling.select_one("p.cit-title"):
            dec = parse_decision_from_element(ruling, resp.text)
            if dec:
                decisions.append(dec)

    return decisions, total


def scrape_zh_steuerrekurs(
    limit: int | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> int:
    """Scrape decisions from ZH Steuerrekursgericht.

    Args:
        limit: Maximum number of decisions to import
        from_date: Only import decisions on or after this date
        to_date: Only import decisions on or before this date

    Returns:
        Number of decisions imported
    """
    print("Scraping strgzh.ch (Zürich Tax Appeals Court)...")

    if to_date is None:
        to_date = date.today()

    if from_date:
        print(f"  Date filter: {from_date} to {to_date}")
    else:
        print("  Full historical import (no date filter)")

    stats = ScraperStats()

    with get_session() as session:
        existing_count = session.exec(select(func.count(Decision.id))).one()
        print(f"Existing decisions in DB: {existing_count}")

        # Fetch first page to get total count
        print("Fetching decision list...")
        first_page_decisions, total_count = fetch_decisions_page(1)
        print(f"Found {total_count} total decisions")

        if total_count == 0:
            print("No decisions found")
            return 0

        # Calculate pages
        total_pages = (total_count + RESULTS_PER_PAGE - 1) // RESULTS_PER_PAGE

        # Process all pages
        all_decisions = first_page_decisions
        for page in range(2, total_pages + 1):
            print(f"  Fetching page {page}/{total_pages}...")
            page_decisions, _ = fetch_decisions_page(page)
            all_decisions.extend(page_decisions)

            if limit and len(all_decisions) >= limit * 2:
                break

        print(f"Collected {len(all_decisions)} decision references")

        for dec_info in all_decisions:
            # Apply date filter
            if dec_info["decision_date"]:
                if from_date and dec_info["decision_date"] < from_date:
                    stats.add_skipped()
                    continue
                if to_date and dec_info["decision_date"] > to_date:
                    stats.add_skipped()
                    continue

            # Generate stable ID
            stable_id = stable_uuid_url(f"zh-steuerrekurs:{dec_info['docket']}")

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
                "source": "strgzh.ch",
                "legal_area": "Steuerrecht",
            }
            if dec_info["norms"]:
                meta["norms"] = dec_info["norms"]
            if dec_info["headnote"]:
                meta["headnote"] = dec_info["headnote"]
            if dec_info["appealability"]:
                meta["appealability"] = dec_info["appealability"]
            if dec_info["additional_dockets"]:
                meta["additional_dockets"] = dec_info["additional_dockets"]

            # Create decision
            try:
                dec = Decision(
                    id=stable_id,
                    source_id="zh_steuerrekurs",
                    source_name="Zürich Steuerrekursgericht",
                    level="cantonal",
                    canton="ZH",
                    court="Steuerrekursgericht",
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
        print(stats.summary("ZH Steuerrekurs"))
        return stats.imported


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape decisions from strgzh.ch")
    parser.add_argument("--limit", type=int, help="Max decisions to import")
    parser.add_argument("--from-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to-date", help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    from_dt = date.fromisoformat(args.from_date) if args.from_date else None
    to_dt = date.fromisoformat(args.to_date) if args.to_date else None

    scrape_zh_steuerrekurs(limit=args.limit, from_date=from_dt, to_date=to_dt)
