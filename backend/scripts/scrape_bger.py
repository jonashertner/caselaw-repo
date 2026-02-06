#!/usr/bin/env python3
"""Scrape decisions from bger.ch (Federal Supreme Court).

This scraper fetches decisions directly from the Federal Court website,
which may have decisions not indexed by entscheidsuche.ch.

Usage:
    python scripts/scrape_bger.py [--from-date YYYY-MM-DD] [--to-date YYYY-MM-DD] [--limit N]
"""
from __future__ import annotations

import argparse
import re
import sys
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import unquote

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
    retry,
    upsert_decision,
)

BASE_URL = "https://www.bger.ch/ext/eurospider/live/de/php/aza/http/index.php"

# Rate limiter: 2 requests per second
rate_limiter = RateLimiter(requests_per_second=2.0)


@retry(max_attempts=3, backoff_base=2.0)
def fetch_decision_list(from_date: date, to_date: date, page: int = 1) -> tuple[list[dict], int]:
    """Fetch list of decisions for a date range."""
    rate_limiter.wait()

    params = {
        "lang": "de",
        "type": "simple_query",
        "query_words": "",
        "top_subcollection_aza": "all",
        "from_date": from_date.strftime("%d.%m.%Y"),
        "to_date": to_date.strftime("%d.%m.%Y"),
        "sort": "relevance",
        "page": page,
        "azaclir": "aza",
    }

    resp = httpx.get(BASE_URL, params=params, headers=DEFAULT_HEADERS, timeout=60, follow_redirects=True)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    # Find total count
    total = 0
    count_match = re.search(r"(\d+)\s*Dokumente?", resp.text)
    if count_match:
        total = int(count_match.group(1))

    # Find decision links
    decisions = []
    for link in soup.find_all("a", href=True):
        href = link.get("href", "")
        if "highlight_docid=aza" in href:
            # Extract case ID from URL
            match = re.search(r"highlight_docid=aza%3A%2F%2F([^&]+)", href)
            if match:
                doc_id = unquote(match.group(1))
                # Parse doc_id format: DD-MM-YYYY-CASE_NUMBER
                parts = doc_id.split("-")
                if len(parts) >= 4:
                    case_date = f"{parts[2]}-{parts[1]}-{parts[0]}"  # YYYY-MM-DD
                    case_number = "-".join(parts[3:])
                    decisions.append({
                        "doc_id": doc_id,
                        "case_number": case_number,
                        "date": case_date,
                        "url": f"https://www.bger.ch{href}" if href.startswith("/") else href,
                    })

    return decisions, total


@retry(max_attempts=3, backoff_base=2.0)
def fetch_decision_content(url: str) -> str | None:
    """Fetch the full text content of a decision."""
    rate_limiter.wait()
    try:
        resp = httpx.get(url, headers=DEFAULT_HEADERS, timeout=60, follow_redirects=True)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")

        # Find the main content div
        content_div = soup.find("div", class_="content") or soup.find("div", id="content")
        if content_div:
            # Remove script and style elements
            for element in content_div.find_all(["script", "style", "nav", "header", "footer"]):
                element.decompose()
            return content_div.get_text(separator="\n", strip=True)

        # Fallback: get body text
        body = soup.find("body")
        if body:
            for element in body.find_all(["script", "style", "nav", "header", "footer"]):
                element.decompose()
            return body.get_text(separator="\n", strip=True)

        return None
    except Exception as e:
        print(f"  Error fetching content: {e}")
        return None


def scrape_bger(
    from_date: date | None = None,
    to_date: date | None = None,
    limit: int | None = None,
) -> int:
    """Scrape decisions from bger.ch.

    Args:
        from_date: Start date (None = 2000-01-01 for full historical)
        to_date: End date (None = today)
        limit: Max decisions to import

    Returns:
        Number of decisions imported
    """
    if to_date is None:
        to_date = date.today()
    if from_date is None:
        # Full historical import - BGer online archive starts ~2000
        from_date = date(2000, 1, 1)

    print(f"Scraping bger.ch from {from_date} to {to_date}...")

    stats = ScraperStats()

    with get_session() as session:
        # Get existing decision IDs
        existing_count = session.exec(select(func.count(Decision.id))).one()
        print(f"Existing decisions in DB: {existing_count}")

        # Iterate through date ranges (1 month at a time to avoid timeouts)
        current_start = from_date
        while current_start <= to_date:
            current_end = min(current_start + timedelta(days=30), to_date)

            print(f"  Fetching {current_start} to {current_end}...")

            page = 1
            while True:
                try:
                    decisions, total = fetch_decision_list(current_start, current_end, page)
                except Exception as e:
                    print(f"    Error fetching list (giving up after retries): {e}")
                    stats.add_error()
                    break

                if not decisions:
                    break

                for dec_info in decisions:
                    # Generate stable ID
                    stable_id = stable_uuid_url(f"bger:{dec_info['doc_id']}")

                    # Fetch content
                    content = fetch_decision_content(dec_info["url"])
                    if not content or len(content) < 100:
                        stats.add_skipped()
                        continue

                    # Parse date
                    try:
                        decision_date = date.fromisoformat(dec_info["date"])
                    except ValueError:
                        decision_date = None

                    # Create decision and upsert
                    try:
                        dec = Decision(
                            id=stable_id,
                            source_id="bger",
                            source_name="Bundesgericht",
                            level="federal",
                            canton=None,
                            court="Bundesgericht",
                            chamber=None,
                            docket=dec_info["case_number"],
                            decision_date=decision_date,
                            published_date=None,
                            title=f"BGer {dec_info['case_number']}",
                            language="de",  # Could detect from content
                            url=dec_info["url"],
                            pdf_url=None,
                            content_text=content,
                            content_hash=compute_hash(content),
                            meta={
                                "source": "bger.ch",
                                "doc_id": dec_info["doc_id"],
                            },
                        )
                        inserted, updated = upsert_decision(session, dec)
                        if inserted or updated:
                            stats.add_imported()
                        else:
                            stats.add_skipped()

                        if stats.imported % 100 == 0:
                            print(f"    Imported {stats.imported} (skipped {stats.skipped})...")
                            session.commit()

                        if limit and stats.imported >= limit:
                            break

                    except Exception as e:
                        print(f"    Error saving: {e}")
                        stats.add_error()
                        continue

                if limit and stats.imported >= limit:
                    break

                # Check if more pages
                if len(decisions) < 10 or page * 10 >= total:
                    break

                page += 1

            if limit and stats.imported >= limit:
                break

            current_start = current_end + timedelta(days=1)

        session.commit()
        print(stats.summary("BGer"))
        return stats.imported


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape decisions from bger.ch")
    parser.add_argument("--from-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to-date", help="End date (YYYY-MM-DD)")
    parser.add_argument("--limit", type=int, help="Max decisions to import")
    args = parser.parse_args()

    from_date = date.fromisoformat(args.from_date) if args.from_date else None
    to_date = date.fromisoformat(args.to_date) if args.to_date else None

    scrape_bger(from_date=from_date, to_date=to_date, limit=args.limit)
