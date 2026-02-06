#!/usr/bin/env python3
"""Scrape decisions from Zürich Sozialversicherungsgericht (Social Insurance Court).

The ZH Social Insurance Court publishes decisions via an API with metadata including
legal area, BGE references, and appealability status.

Usage:
    python scripts/scrape_zh_sozialversicherung.py [--limit N] [--from-date YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import json
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
    parse_date_flexible,
    retry,
    upsert_decision,
)

# API endpoints
SEARCH_API_URL = "https://api.findex.webgate.cloud/api/search/*"
DECISION_PAGE_URL = "https://findex.webgate.cloud/entscheide/"

# Custom headers for API requests
API_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.5",
    "Content-Type": "application/json",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://findex.webgate.cloud",
    "Referer": "https://findex.webgate.cloud/",
}

# Rate limiter: 2 requests per second
rate_limiter = RateLimiter(requests_per_second=2.0)


@retry(max_attempts=3, backoff_base=2.0)
def fetch_api(payload: dict, timeout: int = 60) -> dict:
    """Fetch from API with retry logic."""
    rate_limiter.wait()
    resp = httpx.post(
        SEARCH_API_URL,
        headers=API_HEADERS,
        json=payload,
        timeout=timeout,
    )
    resp.raise_for_status()
    return resp.json()


@retry(max_attempts=3, backoff_base=2.0)
def fetch_decision_html(url: str, timeout: int = 60) -> str:
    """Fetch decision HTML page."""
    rate_limiter.wait()
    resp = httpx.get(url, headers=API_HEADERS, timeout=timeout, follow_redirects=True)
    resp.raise_for_status()
    return resp.text


def extract_text_from_html(html: str) -> str:
    """Extract text content from decision HTML."""
    soup = BeautifulSoup(html, "html.parser")

    # Try to find the main content container
    content = soup.select_one("div.contentContainer, div.printArea, main, article")
    if content:
        # Remove scripts and styles
        for tag in content.select("script, style, nav, header, footer"):
            tag.decompose()
        return content.get_text(separator="\n", strip=True)

    # Fallback to body
    body = soup.find("body")
    if body:
        for tag in body.select("script, style, nav, header, footer"):
            tag.decompose()
        return body.get_text(separator="\n", strip=True)

    return soup.get_text(separator="\n", strip=True)


def fetch_all_decisions(from_date_str: str = "") -> list[dict]:
    """Fetch all decisions from the API.

    Args:
        from_date_str: Optional start date filter (YYYY-MM-DD format)

    Returns:
        List of decision metadata dictionaries
    """
    payload = {
        "Rechtsgebiet": "",
        "datum": from_date_str,
        "operation": ">",
        "prozessnummer": "",
    }

    try:
        results = fetch_api(payload)
    except Exception as e:
        print(f"Error fetching from API: {e}")
        return []

    if not isinstance(results, list):
        print(f"Unexpected API response format: {type(results)}")
        return []

    decisions = []
    for item in results:
        try:
            docket = item.get("prozessnummer", "")
            if not docket:
                continue

            # Parse date
            date_str = item.get("entscheiddatum", "")
            decision_date = None
            if date_str:
                decision_date = parse_date_flexible(date_str[:10])

            # Build title with BGE reference if available
            title = item.get("betreff", docket)
            bge_ref = item.get("bge", "")
            weiterzug = item.get("weiterzug", "")

            if bge_ref:
                title += f" (BGE {bge_ref.strip()})"
            if weiterzug:
                title += f" ({weiterzug.strip()})"

            decisions.append({
                "docket": docket,
                "decision_date": decision_date,
                "title": title[:500],
                "legal_area": item.get("rechtsgebiet", ""),
                "bge_reference": bge_ref.strip() if bge_ref else None,
                "appealability": weiterzug.strip() if weiterzug else None,
                "html_url": f"{DECISION_PAGE_URL}{docket}.html",
            })

        except Exception as e:
            print(f"    Error parsing item: {e}")
            continue

    # Sort by date (newest first) so recent decisions with HTML files are processed first
    decisions.sort(key=lambda d: d["decision_date"] or date.min, reverse=True)
    return decisions


def scrape_zh_sozialversicherung(
    limit: int | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> int:
    """Scrape decisions from ZH Sozialversicherungsgericht.

    Args:
        limit: Maximum number of decisions to import
        from_date: Only import decisions on or after this date
        to_date: Only import decisions on or before this date

    Returns:
        Number of decisions imported
    """
    print("Scraping sozialversicherungsgericht.zh.ch (Zürich Social Insurance Court)...")

    if to_date is None:
        to_date = date.today()

    if from_date:
        print(f"  Date filter: {from_date} to {to_date}")
        from_date_str = from_date.strftime("%Y-%m-%d")
    else:
        print("  Full historical import (no date filter)")
        from_date_str = ""

    stats = ScraperStats()

    with get_session() as session:
        existing_count = session.exec(select(func.count(Decision.id))).one()
        print(f"Existing decisions in DB: {existing_count}")

        # Fetch all decisions from API
        print("Fetching decision list from API...")
        decisions = fetch_all_decisions(from_date_str)
        print(f"Found {len(decisions)} decisions")

        if not decisions:
            print("No decisions found")
            return 0

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
            stable_id = stable_uuid_url(f"zh-sozialversicherung:{dec_info['docket']}")

            # Check if exists
            existing = session.get(Decision, stable_id)
            if existing:
                stats.add_skipped()
                continue

            print(f"  Fetching {dec_info['docket']}...")

            # Fetch HTML content
            try:
                html = fetch_decision_html(dec_info["html_url"])
                content = extract_text_from_html(html)
            except Exception as e:
                print(f"    Error downloading: {e}")
                stats.add_error()
                continue

            if not content or len(content) < 100:
                print(f"    No text content, skipping")
                stats.add_skipped()
                continue

            # Build metadata
            meta = {
                "source": "sozialversicherungsgericht.zh.ch",
            }
            if dec_info["legal_area"]:
                meta["legal_area"] = dec_info["legal_area"]
            if dec_info["bge_reference"]:
                meta["bge_reference"] = dec_info["bge_reference"]
            if dec_info["appealability"]:
                meta["appealability"] = dec_info["appealability"]

            # Create decision
            try:
                dec = Decision(
                    id=stable_id,
                    source_id="zh_sozialversicherung",
                    source_name="Zürich Sozialversicherungsgericht",
                    level="cantonal",
                    canton="ZH",
                    court="Sozialversicherungsgericht",
                    chamber=None,
                    docket=dec_info["docket"],
                    decision_date=dec_info["decision_date"],
                    published_date=None,
                    title=dec_info["title"],
                    language="de",
                    url=dec_info["html_url"],
                    pdf_url=None,  # HTML only
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
        print(stats.summary("ZH Sozialversicherung"))
        return stats.imported


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape decisions from sozialversicherungsgericht.zh.ch")
    parser.add_argument("--limit", type=int, help="Max decisions to import")
    parser.add_argument("--from-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to-date", help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    from_dt = date.fromisoformat(args.from_date) if args.from_date else None
    to_dt = date.fromisoformat(args.to_date) if args.to_date else None

    scrape_zh_sozialversicherung(limit=args.limit, from_date=from_dt, to_date=to_dt)
