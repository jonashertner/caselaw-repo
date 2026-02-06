#!/usr/bin/env python3
"""Scrape decisions from bundespatentgericht.ch (Federal Patent Court).

This scraper fetches decisions from the Federal Patent Court website.
Decisions are organized by year and procedure type.

Usage:
    python scripts/scrape_bpatger.py [--limit N] [--year YYYY] [--from-date YYYY-MM-DD]
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
    retry,
    upsert_decision,
)

BASE_URL = "https://www.bundespatentgericht.ch"

# Rate limiter: 1 request per second (conservative for PDF downloads)
rate_limiter = RateLimiter(requests_per_second=1.0)


@retry(max_attempts=3, backoff_base=2.0)
def fetch_page(url: str) -> httpx.Response:
    """Fetch a page with retry logic."""
    rate_limiter.wait()
    resp = httpx.get(url, headers=DEFAULT_HEADERS, timeout=60, follow_redirects=True)
    resp.raise_for_status()
    return resp


def get_decision_detail(detail_url: str) -> dict | None:
    """Fetch decision detail page and extract PDF link and metadata."""
    try:
        resp = fetch_page(detail_url)
    except Exception as e:
        print(f"    Error fetching detail page: {e}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # Find PDF link
    pdf_url = None
    for link in soup.find_all("a", href=True):
        href = link.get("href", "")
        if href.endswith(".pdf") and "fileadmin/entscheide" in href:
            pdf_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            break

    if not pdf_url:
        return None

    # Extract case ID from PDF filename (e.g., O2024_002_Urteil_2025-08-12.pdf)
    pdf_filename = pdf_url.split("/")[-1]
    case_match = re.match(r"([A-Z]\d{4}_\d+)", pdf_filename)
    case_id = case_match.group(1) if case_match else pdf_filename.replace(".pdf", "")

    # Extract date from filename (YYYY-MM-DD)
    date_match = re.search(r"(\d{4}-\d{2}-\d{2})", pdf_filename)
    decision_date = None
    if date_match:
        try:
            decision_date = date.fromisoformat(date_match.group(1))
        except ValueError:
            pass

    # Extract title from page
    title_elem = soup.find("h1")
    title = title_elem.get_text(strip=True) if title_elem else f"BPatGer {case_id}"

    # Determine procedure type
    procedure_type = "ordentlich"
    if "summarisch" in detail_url.lower() or "S20" in case_id:
        procedure_type = "summarisch"

    return {
        "case_id": case_id,
        "pdf_url": pdf_url,
        "decision_date": decision_date,
        "title": title,
        "procedure_type": procedure_type,
        "detail_url": detail_url,
    }


def fetch_year_decisions(year: int) -> list[dict]:
    """Fetch decisions for a specific year."""
    decisions = []

    # Both procedure types and URL variants (typo in some years: entschiede vs entscheide)
    for proc_type in ["ordentlichen", "summarischen"]:
        for year_variant in [f"entschiede-{year}", f"entscheide-{year}"]:
            url = f"{BASE_URL}/rechtsprechung/{year_variant}/entscheide-im-{proc_type}-verfahren"

            try:
                resp = fetch_page(url)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    continue
                print(f"  Error fetching {year} {proc_type} ({year_variant}): {e}")
                continue
            except Exception as e:
                print(f"  Error fetching {year} {proc_type} ({year_variant}): {e}")
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            # Find links to decision detail pages
            for link in soup.find_all("a", href=True):
                href = link.get("href", "")
                if "/rechtsprechung/entscheidanzeige/" in href:
                    detail_url = href if href.startswith("http") else f"{BASE_URL}{href}"
                    decisions.append({
                        "detail_url": detail_url,
                        "year": year,
                        "procedure": proc_type,
                    })

    # Deduplicate by detail URL
    seen = set()
    unique = []
    for d in decisions:
        if d["detail_url"] not in seen:
            seen.add(d["detail_url"])
            unique.append(d)

    return unique


def scrape_bpatger(
    limit: int | None = None,
    year: int | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> int:
    """Scrape decisions from BPatGer.

    Args:
        limit: Maximum number of decisions to import
        year: Specific year to scrape (overrides from_date/to_date)
        from_date: Only import decisions on or after this date
        to_date: Only import decisions on or before this date

    Returns:
        Number of decisions imported
    """
    print("Scraping bundespatentgericht.ch...")

    # Default to_date to today
    if to_date is None:
        to_date = date.today()

    # If no year and no from_date specified, do full historical import (2012+)
    # BPatGer was established in 2012
    if from_date is None and year is None:
        from_date = None  # Will scrape all available years

    if from_date:
        print(f"  Date filter: {from_date} to {to_date}")

    stats = ScraperStats()

    with get_session() as session:
        existing_count = session.exec(select(func.count(Decision.id))).one()
        print(f"Existing decisions in DB: {existing_count}")

        # Determine years to scrape
        if year:
            years = [year]
        elif from_date:
            # Only scrape years within the date range
            years = list(range(to_date.year, from_date.year - 1, -1))
        else:
            # Scrape all available years (2012-present)
            current_year = date.today().year
            years = list(range(current_year, 2011, -1))  # Newest first

        for yr in years:
            print(f"Fetching {yr}...")
            decision_refs = fetch_year_decisions(yr)
            print(f"  Found {len(decision_refs)} decision references for {yr}")

            for ref in decision_refs:
                # Get decision details
                detail = get_decision_detail(ref["detail_url"])
                if not detail:
                    stats.add_skipped()
                    continue

                # Apply date filter if specified
                if from_date and detail["decision_date"]:
                    if detail["decision_date"] < from_date:
                        stats.add_skipped()
                        continue
                if to_date and detail["decision_date"]:
                    if detail["decision_date"] > to_date:
                        stats.add_skipped()
                        continue

                # Generate stable ID
                stable_id = stable_uuid_url(f"bpatger:{detail['case_id']}")

                # Check if exists
                existing = session.get(Decision, stable_id)
                if existing:
                    stats.add_skipped()
                    continue

                print(f"  Fetching {detail['case_id']}...")

                # Download PDF with retry
                try:
                    resp = fetch_page(detail["pdf_url"])
                    pdf_content = resp.content
                except Exception as e:
                    print(f"    Error downloading PDF: {e}")
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
                        source_id="bpatger",
                        source_name="Bundespatentgericht",
                        level="federal",
                        canton=None,
                        court="Bundespatentgericht",
                        chamber=detail["procedure_type"],
                        docket=detail["case_id"],
                        decision_date=detail["decision_date"],
                        published_date=None,
                        title=detail["title"],
                        language="de",  # BPatGer publishes in German
                        url=detail["detail_url"],
                        pdf_url=detail["pdf_url"],
                        content_text=content,
                        content_hash=compute_hash(content),
                        meta={
                            "source": "bundespatentgericht.ch",
                            "procedure_type": detail["procedure_type"],
                        },
                    )
                    session.add(dec)
                    stats.add_imported()

                    if stats.imported % 5 == 0:
                        print(f"  Imported {stats.imported} (skipped {stats.skipped})...")
                        session.commit()

                    if limit and stats.imported >= limit:
                        session.commit()
                        print(stats.summary("BPatGer"))
                        return stats.imported

                except Exception as e:
                    print(f"    Error saving: {e}")
                    stats.add_error()
                    continue

        session.commit()
        print(stats.summary("BPatGer"))
        return stats.imported


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape decisions from bundespatentgericht.ch")
    parser.add_argument("--limit", type=int, help="Max decisions to import")
    parser.add_argument("--year", type=int, help="Specific year to scrape")
    parser.add_argument("--from-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to-date", help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    from_dt = date.fromisoformat(args.from_date) if args.from_date else None
    to_dt = date.fromisoformat(args.to_date) if args.to_date else None

    scrape_bpatger(limit=args.limit, year=args.year, from_date=from_dt, to_date=to_dt)
