#!/usr/bin/env python3
"""Scrape decisions from WEKO (Wettbewerbskommission / Competition Commission).

The Swiss Competition Commission publishes decisions on competition law matters
including cartels, mergers, and abuse of dominant position cases.

Usage:
    python scripts/scrape_weko.py [--limit N] [--from-date YYYY-MM-DD]
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

BASE_URL = "https://www.weko.admin.ch"
DECISIONS_URL = f"{BASE_URL}/weko/de/home/praxis/publizierte-entscheide.html"

# Rate limiter: 1 request per second (conservative)
rate_limiter = RateLimiter(requests_per_second=1.0)


@retry(max_attempts=3, backoff_base=2.0)
def fetch_page(url: str, timeout: int = 60) -> httpx.Response:
    """Fetch a page with retry logic."""
    rate_limiter.wait()
    resp = httpx.get(url, headers=DEFAULT_HEADERS, timeout=timeout, follow_redirects=True)
    resp.raise_for_status()
    return resp


def parse_decision_meta(title_text: str) -> dict:
    """Parse decision metadata from title attribute.

    Examples:
        "RPW 2024/1: Verfügung vom 15. Januar 2024"
        "41-0829: Sanktion gegen XY vom 20.03.2024"
    """
    result = {
        "docket": None,
        "decision_type": None,
        "decision_date": None,
        "title": title_text,
    }

    # Try to extract case number (pattern: XX-XXXX or RPW YYYY/X)
    docket_match = re.search(r"(\d{2}-\d{4}|RPW\s+\d{4}/\d+)", title_text)
    if docket_match:
        result["docket"] = docket_match.group(1).strip()

    # Try to extract decision type (Verfügung, Sanktion, Entscheid, etc.)
    type_match = re.search(r"(Verfügung|Sanktion|Entscheid|Urteil|Beschluss)", title_text, re.I)
    if type_match:
        result["decision_type"] = type_match.group(1)

    # Try to extract date
    date_match = re.search(r"vom\s+(\d{1,2}\.?\s*\w+\s+\d{4}|\d{2}\.\d{2}\.\d{4})", title_text, re.I)
    if date_match:
        result["decision_date"] = parse_date_flexible(date_match.group(1))

    return result


def fetch_all_decisions() -> list[dict]:
    """Fetch all decision links from WEKO website."""
    print("Fetching WEKO decision list...")

    try:
        resp = fetch_page(DECISIONS_URL)
    except Exception as e:
        print(f"Error fetching decision list: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    decisions = []

    # Find all PDF links - WEKO uses /dam/weko/ path for documents
    for link in soup.find_all("a", href=True):
        href = link.get("href", "")
        title = link.get("title", "") or link.get_text(strip=True)

        # Match both old and new URL patterns
        if (".pdf" in href.lower() or "download" in href.lower()) and ("/dam/weko/" in href or "/praxis/" in href):
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"

            # Parse metadata from title or link text
            meta = parse_decision_meta(title)

            # Extract filename for cases without parsed docket
            if not meta["docket"]:
                filename = href.split("/")[-1].split(".")[0]
                meta["docket"] = filename[:50] if filename else None

            decisions.append({
                "url": full_url,
                "title": title[:500] if title else href.split("/")[-1],
                "docket": meta["docket"],
                "decision_type": meta["decision_type"],
                "decision_date": meta["decision_date"],
            })

    # Deduplicate by URL
    seen = set()
    unique = []
    for d in decisions:
        if d["url"] not in seen:
            seen.add(d["url"])
            unique.append(d)

    return unique


def scrape_weko(
    limit: int | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> int:
    """Scrape decisions from WEKO.

    Args:
        limit: Maximum number of decisions to import
        from_date: Only import decisions on or after this date
        to_date: Only import decisions on or before this date

    Returns:
        Number of decisions imported
    """
    print("Scraping weko.admin.ch (Competition Commission)...")

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
        print(f"Found {len(decisions)} decisions on WEKO website")

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
            stable_id = stable_uuid_url(f"weko:{dec_info['url']}")

            # Check if exists
            existing = session.get(Decision, stable_id)
            if existing:
                stats.add_skipped()
                continue

            print(f"  Fetching {dec_info['docket'] or dec_info['url'].split('/')[-1]}...")

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
                    source_id="weko",
                    source_name="Wettbewerbskommission",
                    level="federal",
                    canton=None,
                    court="Wettbewerbskommission",
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
                        "source": "weko.admin.ch",
                        "decision_type": dec_info["decision_type"],
                        "legal_area": "Wettbewerbsrecht",
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
        print(stats.summary("WEKO"))
        return stats.imported


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape decisions from weko.admin.ch")
    parser.add_argument("--limit", type=int, help="Max decisions to import")
    parser.add_argument("--from-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to-date", help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    from_dt = date.fromisoformat(args.from_date) if args.from_date else None
    to_dt = date.fromisoformat(args.to_date) if args.to_date else None

    scrape_weko(limit=args.limit, from_date=from_dt, to_date=to_dt)
