#!/usr/bin/env python3
"""Scrape decisions from EDÖB (Eidgenössischer Datenschutz- und Öffentlichkeitsbeauftragter).

The Federal Data Protection and Information Commissioner publishes recommendations
and decisions on data protection and freedom of information matters.

Usage:
    python scripts/scrape_edoeb.py [--limit N] [--from-date YYYY-MM-DD]
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

BASE_URL = "https://www.edoeb.admin.ch"
# EDÖB publishes decisions in different sections (new URL structure as of 2024)
DECISIONS_URLS = [
    # Data protection - old law (before 01.09.2023)
    (f"{BASE_URL}/de/schlussberichte-empfehlungen-bis-31082023", "Datenschutz (aDSG)"),
    # Data protection - new law (from 01.09.2023)
    (f"{BASE_URL}/de/verfuegungen", "Datenschutz (DSG)"),
    # Freedom of information (BGÖ)
    (f"{BASE_URL}/de/empfehlungen-nach-bgo", "Öffentlichkeitsprinzip"),
]

# Rate limiter: 1 request per second
rate_limiter = RateLimiter(requests_per_second=1.0)


@retry(max_attempts=3, backoff_base=2.0)
def fetch_page(url: str, timeout: int = 60) -> httpx.Response:
    """Fetch a page with retry logic."""
    rate_limiter.wait()
    resp = httpx.get(url, headers=DEFAULT_HEADERS, timeout=timeout, follow_redirects=True)
    resp.raise_for_status()
    return resp


def parse_decision_meta(text: str, legal_area: str) -> dict:
    """Parse decision metadata from text."""
    result = {
        "docket": None,
        "decision_date": None,
        "legal_area": legal_area,
    }

    # Try to extract date
    date_match = re.search(r"(\d{1,2}\.?\s*\w+\s+\d{4}|\d{2}\.\d{2}\.\d{4})", text)
    if date_match:
        result["decision_date"] = parse_date_flexible(date_match.group(1))

    # Try to extract reference number
    ref_match = re.search(r"(\d{4}-\d+|\d+/\d{4})", text)
    if ref_match:
        result["docket"] = ref_match.group(1)

    return result


def fetch_decisions_from_page(page_url: str, legal_area: str) -> list[dict]:
    """Fetch decision links from a page."""
    try:
        resp = fetch_page(page_url)
    except Exception as e:
        print(f"Error fetching {page_url}: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    decisions = []

    # Find PDF links - new site uses /dam/ path for documents
    for link in soup.find_all("a", href=True):
        href = link.get("href", "")
        text = link.get_text(strip=True)

        # Match PDF links (direct .pdf or /dam/ paths which are documents)
        if href.endswith(".pdf") or "/dam/" in href:
            if not href.endswith(".pdf"):
                continue  # Only process actual PDFs
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            meta = parse_decision_meta(text, legal_area)

            decisions.append({
                "url": full_url,
                "title": text[:500] if text else href.split("/")[-1].replace(".pdf", ""),
                "docket": meta["docket"],
                "decision_date": meta["decision_date"],
                "legal_area": meta["legal_area"],
            })

    return decisions


def fetch_all_decisions() -> list[dict]:
    """Fetch all decisions from EDÖB website."""
    print("Fetching EDÖB decision lists...")

    all_decisions = []
    for url, legal_area in DECISIONS_URLS:
        print(f"  Checking {url.split('/')[-1]} ({legal_area})...")
        decisions = fetch_decisions_from_page(url, legal_area)
        print(f"    Found {len(decisions)} PDFs")
        all_decisions.extend(decisions)

    # Deduplicate by URL
    seen = set()
    unique = []
    for d in all_decisions:
        if d["url"] not in seen:
            seen.add(d["url"])
            unique.append(d)

    return unique


def scrape_edoeb(
    limit: int | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> int:
    """Scrape decisions from EDÖB.

    Args:
        limit: Maximum number of decisions to import
        from_date: Only import decisions on or after this date
        to_date: Only import decisions on or before this date

    Returns:
        Number of decisions imported
    """
    print("Scraping edoeb.admin.ch (Data Protection Commissioner)...")

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

        decisions = fetch_all_decisions()
        print(f"Found {len(decisions)} decisions on EDÖB website")

        for dec_info in decisions:
            # Apply date filter
            if dec_info["decision_date"]:
                if from_date and dec_info["decision_date"] < from_date:
                    stats.add_skipped()
                    continue
                if to_date and dec_info["decision_date"] > to_date:
                    stats.add_skipped()
                    continue

            # Generate stable ID
            stable_id = stable_uuid_url(f"edoeb:{dec_info['url']}")

            # Check if exists
            existing = session.get(Decision, stable_id)
            if existing:
                stats.add_skipped()
                continue

            print(f"  Fetching {dec_info['title'][:50]}...")

            # Download PDF
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

            # Create decision
            try:
                dec = Decision(
                    id=stable_id,
                    source_id="edoeb",
                    source_name="EDÖB",
                    level="federal",
                    canton=None,
                    court="Eidgenössischer Datenschutz- und Öffentlichkeitsbeauftragter",
                    chamber=None,
                    docket=dec_info["docket"],
                    decision_date=dec_info["decision_date"],
                    published_date=None,
                    title=dec_info["title"],
                    language="de",
                    url=dec_info["url"],
                    pdf_url=dec_info["url"],
                    content_text=content,
                    content_hash=compute_hash(content),
                    meta={
                        "source": "edoeb.admin.ch",
                        "legal_area": dec_info["legal_area"],
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
        print(stats.summary("EDÖB"))
        return stats.imported


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape decisions from edoeb.admin.ch")
    parser.add_argument("--limit", type=int, help="Max decisions to import")
    parser.add_argument("--from-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to-date", help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    from_dt = date.fromisoformat(args.from_date) if args.from_date else None
    to_dt = date.fromisoformat(args.to_date) if args.to_date else None

    scrape_edoeb(limit=args.limit, from_date=from_dt, to_date=to_dt)
