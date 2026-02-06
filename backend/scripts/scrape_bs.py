#!/usr/bin/env python3
"""Scrape decisions from Basel-Stadt (BS) FindInfoWeb database.

This scraper fetches decisions from the official Basel-Stadt court database
at rechtsprechung.gerichte.bs.ch using Playwright to handle form submissions.

Total available: ~10,345 decisions

Usage:
    python scripts/scrape_bs.py [--limit N] [--from-date YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import asyncio
import re
import sys
from datetime import date
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlmodel import select, func
from app.db.session import get_session
from app.models.decision import Decision
from app.services.indexer import stable_uuid_url

from scripts.scraper_common import (
    ScraperStats,
    compute_hash,
    parse_date_flexible,
    upsert_decision,
)

BASE_URL = "https://rechtsprechung.gerichte.bs.ch/cgi-bin/nph-omniscgi.exe"


def parse_case_number(case_num: str) -> tuple[str, str | None]:
    """Parse case number and determine court/chamber."""
    # Map case type prefixes to courts
    court_map = {
        "AL": "Sozialversicherungsgericht",
        "AH": "Sozialversicherungsgericht",
        "EL": "Sozialversicherungsgericht",
        "EO": "Sozialversicherungsgericht",
        "IV": "Sozialversicherungsgericht",
        "UV": "Sozialversicherungsgericht",
        "KV": "Sozialversicherungsgericht",
        "DGS": "Appellationsgericht",
        "DGV": "Appellationsgericht",
        "DGZ": "Appellationsgericht",
        "VD": "Appellationsgericht",
        "VG": "Appellationsgericht",
        "BES": "Appellationsgericht",
        "BEZ": "Appellationsgericht",
        "ZB": "Appellationsgericht",
        "ZS": "Appellationsgericht",
        "ZK": "Appellationsgericht",
        "ZV": "Appellationsgericht",
        "SB": "Appellationsgericht",
        "SG": "Appellationsgericht",
    }

    match = re.match(r"([A-Z]+)\.\d+\.\d+", case_num)
    if match:
        prefix = match.group(1)
        court = court_map.get(prefix, "Appellationsgericht")
        return case_num, court

    return case_num, "Appellationsgericht"


async def fetch_decisions_page(page, page_num: int = 1, results_per_page: int = 100):
    """Fetch a page of decisions from FindInfoWeb."""

    if page_num == 1:
        # Load the search page
        url = f"{BASE_URL}?OmnisPlatform=WINDOWS&WebServerUrl=rechtsprechung.gerichte.bs.ch&WebServerScript=/cgi-bin/nph-omniscgi.exe&OmnisLibrary=JURISWEB&OmnisClass=rtFindinfoWebHtmlService&OmnisServer=JURISWEB,7000&Aufruf=loadTemplate&cTemplate=search.html&Schema=BS_FI_WEB&cSprache=DE&Parametername=WEB"
        await page.goto(url, timeout=60000)
        await page.wait_for_timeout(2000)

        # Set results per page
        await page.fill('input[name="nAnzahlTrefferProSeite"]', str(results_per_page))

        # Submit search (empty = all results)
        await page.click('button[name="evSubmit"]', timeout=10000)
        await page.wait_for_timeout(5000)
    else:
        # Navigate to specific page by filling in page number in form
        # First check if there's a page input or pagination links
        try:
            # Look for page navigation
            await page.fill('input[name="nSeite"]', str(page_num))
            await page.click('button[name="evSubmit"]', timeout=10000)
            await page.wait_for_timeout(3000)
        except Exception:
            # Try clicking next page link
            next_link = await page.query_selector(f'a[href*="nSeite={page_num}"]')
            if next_link:
                await next_link.click()
                await page.wait_for_timeout(3000)
            else:
                return [], 0

    html = await page.content()

    # Extract total count
    total_match = re.search(r'von\s+(\d+)', html)
    total = int(total_match.group(1)) if total_match else 0

    # Extract decision data
    # Pattern: nF30_KEY=(\d+).*?<span[^>]*>([^<]+)</span>
    decisions = []

    # Find all getMarkupDocument links with their context
    pattern = r'nF30_KEY=(\d+)[^"]*"[^>]*>.*?<span[^>]*>([^<]+)</span>'
    matches = re.findall(pattern, html, re.DOTALL)

    for doc_id, case_num in matches:
        case_num = case_num.strip()
        decisions.append({
            "id": doc_id,
            "case_number": case_num,
        })

    return decisions, total


async def fetch_decision_content(page, doc_id: str, case_number: str) -> str | None:
    """Fetch the full content of a decision."""
    # Build the detail URL
    detail_url = f"{BASE_URL}?OmnisPlatform=WINDOWS&WebServerScript=/cgi-bin/nph-omniscgi.exe&OmnisLibrary=JURISWEB&OmnisClass=rtFindinfoWebHtmlService&OmnisServer=JURISWEB,7000&Aufruf=getMarkupDocument&Schema=BS_FI_WEB&cSprache=DE&Parametername=WEB&nF30_KEY={doc_id}&Template=search_result_document.html"

    try:
        await page.goto(detail_url, timeout=60000)
        await page.wait_for_timeout(2000)

        # Get the main content
        content = await page.evaluate('''
            () => {
                // Try to find the main content area
                const content = document.querySelector('.content, #content, .document, #document, body');
                return content ? content.innerText : '';
            }
        ''')

        return content.strip() if content else None
    except Exception as e:
        print(f"    Error fetching {case_number}: {e}")
        return None


async def scrape_bs_async(
    limit: int | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> int:
    """Scrape decisions from Basel-Stadt FindInfoWeb.

    Returns:
        Number of decisions imported
    """
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        print("Playwright not installed. Run: pip install playwright && playwright install chromium")
        return 0

    print("Scraping Basel-Stadt (rechtsprechung.gerichte.bs.ch)...")
    if from_date:
        print(f"  Date filter: {from_date} to {to_date or date.today()}")

    stats = ScraperStats()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        page = await context.new_page()

        with get_session() as session:
            existing_count = session.exec(
                select(func.count(Decision.id)).where(Decision.canton == "BS")
            ).one()
            print(f"Existing BS decisions in DB: {existing_count}")

            page_num = 1
            results_per_page = 100
            total_available = None

            while True:
                print(f"  Fetching page {page_num}...")
                decisions, total = await fetch_decisions_page(page, page_num, results_per_page)

                if total_available is None:
                    total_available = total
                    print(f"  Total available: {total_available}")

                if not decisions:
                    print("  No more decisions found")
                    break

                print(f"    Found {len(decisions)} decisions on page {page_num}")

                for dec_info in decisions:
                    doc_id = dec_info["id"]
                    case_number = dec_info["case_number"]

                    # Generate stable ID
                    stable_id = stable_uuid_url(f"bs-findinfo:{doc_id}")

                    # Check if exists
                    existing = session.get(Decision, stable_id)
                    if existing:
                        stats.add_skipped()
                        continue

                    # Fetch full content
                    content = await fetch_decision_content(page, doc_id, case_number)

                    if not content or len(content) < 100:
                        stats.add_skipped()
                        continue

                    # Try to extract date from content
                    decision_date = None
                    date_match = re.search(r"(\d{1,2}\.\s*\w+\s+\d{4}|\d{2}\.\d{2}\.\d{4})", content[:1000])
                    if date_match:
                        decision_date = parse_date_flexible(date_match.group(1))

                    # Apply date filter
                    if from_date and decision_date and decision_date < from_date:
                        stats.add_skipped()
                        continue
                    if to_date and decision_date and decision_date > to_date:
                        stats.add_skipped()
                        continue

                    # Parse case number for court info
                    _, court = parse_case_number(case_number)

                    # Create decision
                    try:
                        dec = Decision(
                            id=stable_id,
                            source_id="bs",
                            source_name="Basel-Stadt",
                            level="cantonal",
                            canton="BS",
                            court=court,
                            chamber=None,
                            docket=case_number,
                            decision_date=decision_date,
                            published_date=None,
                            title=f"BS {case_number}",
                            language="de",
                            url=f"{BASE_URL}?Aufruf=getMarkupDocument&Schema=BS_FI_WEB&nF30_KEY={doc_id}",
                            pdf_url=None,
                            content_text=content,
                            content_hash=compute_hash(content),
                            meta={
                                "source": "rechtsprechung.gerichte.bs.ch",
                                "findinfo_id": doc_id,
                            },
                        )
                        session.add(dec)
                        stats.add_imported()

                        if stats.imported % 50 == 0:
                            print(f"    Imported {stats.imported} (skipped {stats.skipped})...")
                            session.commit()

                        if limit and stats.imported >= limit:
                            break

                    except Exception as e:
                        print(f"    Error saving {case_number}: {e}")
                        stats.add_error()
                        continue

                    # Small delay between requests
                    await page.wait_for_timeout(200)

                if limit and stats.imported >= limit:
                    break

                # Check if we've fetched all pages
                if page_num * results_per_page >= total_available:
                    print("  Reached end of results")
                    break

                page_num += 1
                await page.wait_for_timeout(1000)

            session.commit()

        await browser.close()

    print(stats.summary("Basel-Stadt"))
    return stats.imported


def scrape_bs(
    limit: int | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> int:
    """Wrapper to run async scraper."""
    return asyncio.run(scrape_bs_async(limit, from_date, to_date))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape Basel-Stadt decisions")
    parser.add_argument("--limit", type=int, help="Max decisions to import")
    parser.add_argument("--from-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to-date", help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    from_dt = date.fromisoformat(args.from_date) if args.from_date else None
    to_dt = date.fromisoformat(args.to_date) if args.to_date else None
    scrape_bs(limit=args.limit, from_date=from_dt, to_date=to_dt)
