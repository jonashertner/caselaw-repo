#!/usr/bin/env python3
"""Scrape decisions directly from BVGer Weblaw portal using Playwright.

The Federal Administrative Court (Bundesverwaltungsgericht/BVGer) publishes
decisions through bvger.weblaw.ch, a JavaScript SPA. This scraper uses
Playwright to intercept API calls and extract decision data.

Usage:
    python scripts/scrape_bvger_direct.py [--limit N] [--from-date YYYY-MM-DD] [--to-date YYYY-MM-DD]

Requirements:
    pip install playwright
    playwright install chromium
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlmodel import select, func
from app.db.session import get_session
from app.models.decision import Decision
from app.services.indexer import stable_uuid_url

from scripts.scraper_common import (
    RateLimiter,
    ScraperStats,
    compute_hash,
    extract_pdf_text,
    parse_date_flexible,
    upsert_decision,
)

# Portal URL
WEBLAW_URL = "https://bvger.weblaw.ch/dashboard?guiLanguage=de&sort-field=relevance&sort-direction=relevance"
API_SEARCH_ENDPOINT = ".netlify/functions/searchQueryService"
API_DOC_ENDPOINT = ".netlify/functions/singleDocQueryService"

# Rate limiter: 1 request per second (be polite to the SPA)
rate_limiter = RateLimiter(requests_per_second=1.0)


def extract_decisions_from_api_response(data: dict) -> list[dict]:
    """Extract decision metadata from Weblaw API response.

    Args:
        data: Raw API response data

    Returns:
        List of decision dictionaries with metadata
    """
    decisions = []

    hits = data.get("hits", {}).get("hits", [])
    for hit in hits:
        source = hit.get("_source", {})
        doc_id = hit.get("_id", "")

        # Extract case number and date from filename or metadata
        filename = source.get("filename", "")

        # Parse decision date
        date_str = source.get("decision_date") or source.get("date") or ""
        decision_date = None
        if date_str:
            decision_date = parse_date_flexible(date_str[:10])

        # Extract case number from filename pattern: E-1234-2024, D-5678-2025, etc.
        case_number = None
        case_match = re.search(r"([A-Z]-\d+-\d{4})", filename or doc_id)
        if case_match:
            case_number = case_match.group(1)

        # Get title
        title = source.get("title") or source.get("subject") or filename or doc_id
        if isinstance(title, dict):
            title = title.get("de") or title.get("fr") or title.get("it") or str(title)

        # Get language
        language = source.get("language", "de")
        if isinstance(language, list):
            language = language[0] if language else "de"

        # Get division/chamber
        division = source.get("division") or source.get("abteilung")

        decisions.append({
            "doc_id": doc_id,
            "case_number": case_number or doc_id,
            "decision_date": decision_date,
            "title": title[:500] if title else doc_id,
            "language": language,
            "division": division,
            "filename": filename,
        })

    return decisions


def scrape_with_playwright(
    from_date: date | None = None,
    to_date: date | None = None,
    limit: int | None = None,
) -> int:
    """Scrape BVGer decisions using Playwright to intercept API calls.

    Args:
        from_date: Start date for scraping
        to_date: End date for scraping
        limit: Maximum decisions to import

    Returns:
        Number of decisions imported
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("ERROR: Playwright not installed. Run: pip install playwright && playwright install chromium")
        return 0

    if to_date is None:
        to_date = date.today()
    if from_date is None:
        # Default: start from 2007 (BVGer established in 2007)
        from_date = date(2007, 1, 1)

    print(f"Scraping bvger.weblaw.ch from {from_date} to {to_date}...")
    print("  Using Playwright to intercept API calls")

    stats = ScraperStats()
    captured_responses: list[dict] = []
    captured_documents: dict[str, bytes] = {}

    with get_session() as session:
        existing_count = session.exec(select(func.count(Decision.id)).where(
            Decision.source_id == "bvger"
        )).one()
        print(f"Existing BVGer decisions in DB: {existing_count}")

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()

            # Intercept API responses
            def handle_response(response):
                if API_SEARCH_ENDPOINT in response.url:
                    try:
                        data = response.json()
                        captured_responses.append(data)
                    except Exception:
                        pass
                elif ".pdf" in response.url.lower():
                    try:
                        captured_documents[response.url] = response.body()
                    except Exception:
                        pass

            page.on("response", handle_response)

            # Navigate to portal - this triggers initial search
            print("  Loading Weblaw portal...")
            page.goto(WEBLAW_URL, wait_until="networkidle", timeout=60000)
            time.sleep(3)  # Wait for SPA to fully load

            # Process year by year for comprehensive coverage
            current_year = to_date.year
            start_year = from_date.year

            for year in range(current_year, start_year - 1, -1):
                if limit and stats.imported >= limit:
                    break

                print(f"  Processing year {year}...")

                # Try to filter by year using the search interface
                # Look for date filter input
                try:
                    # Clear and set date filter
                    date_input = page.query_selector('input[type="date"], input[placeholder*="datum"], input[name*="date"]')
                    if date_input:
                        date_input.fill(f"{year}-01-01")
                        page.wait_for_timeout(1000)

                    # Or try searching for year
                    search_input = page.query_selector('input[type="search"], input[type="text"][placeholder*="such"]')
                    if search_input:
                        search_input.fill(f"{year}")
                        search_input.press("Enter")
                        page.wait_for_timeout(2000)

                except Exception as e:
                    print(f"    Could not set date filter: {e}")

                # Wait for search results
                page.wait_for_timeout(3000)

                # Scroll to load more results (if pagination is scroll-based)
                for _ in range(10):
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    page.wait_for_timeout(500)

                # Process captured responses
                while captured_responses:
                    response_data = captured_responses.pop(0)
                    decisions = extract_decisions_from_api_response(response_data)

                    for dec_info in decisions:
                        if limit and stats.imported >= limit:
                            break

                        # Check date filter
                        if dec_info["decision_date"]:
                            if dec_info["decision_date"] < from_date:
                                stats.add_skipped()
                                continue
                            if dec_info["decision_date"] > to_date:
                                stats.add_skipped()
                                continue

                        # Generate stable ID
                        stable_id = stable_uuid_url(f"bvger:{dec_info['doc_id']}")

                        # Check if exists
                        existing = session.get(Decision, stable_id)
                        if existing:
                            stats.add_skipped()
                            continue

                        # Try to get document content
                        content = None
                        pdf_url = None

                        # Check captured documents
                        for url, pdf_bytes in list(captured_documents.items()):
                            if dec_info["doc_id"] in url or dec_info["case_number"] in url:
                                content = extract_pdf_text(pdf_bytes)
                                pdf_url = url
                                del captured_documents[url]
                                break

                        if not content or len(content) < 100:
                            # Skip documents without content for now
                            # In a full implementation, we would click through to get the PDF
                            stats.add_skipped()
                            continue

                        # Create decision
                        try:
                            dec = Decision(
                                id=stable_id,
                                source_id="bvger",
                                source_name="Bundesverwaltungsgericht",
                                level="federal",
                                canton=None,
                                court="Bundesverwaltungsgericht",
                                chamber=dec_info.get("division"),
                                docket=dec_info["case_number"],
                                decision_date=dec_info["decision_date"],
                                published_date=None,
                                title=f"BVGer {dec_info['case_number']}" if dec_info["case_number"] else dec_info["title"],
                                language=dec_info["language"],
                                url=f"https://bvger.weblaw.ch/cache/{dec_info['doc_id']}",
                                pdf_url=pdf_url,
                                content_text=content,
                                content_hash=compute_hash(content),
                                meta={
                                    "source": "bvger.weblaw.ch",
                                    "doc_id": dec_info["doc_id"],
                                },
                            )
                            session.add(dec)
                            stats.add_imported()

                            if stats.imported % 100 == 0:
                                print(f"    Imported {stats.imported} (skipped {stats.skipped})...")
                                session.commit()

                        except Exception as e:
                            print(f"    Error saving: {e}")
                            stats.add_error()
                            continue

            browser.close()

        session.commit()
        print(stats.summary("BVGer (Playwright)"))
        return stats.imported


def scrape_bvger_via_entscheidsuche(
    from_date: date | None = None,
    to_date: date | None = None,
    limit: int | None = None,
) -> int:
    """Alternative: Scrape BVGer via entscheidsuche.ch mirrors.

    This approach queries entscheidsuche.ch for BVGer decisions and downloads
    PDFs directly from their document store. The PDFs are official copies.

    This is more reliable than the Playwright approach but technically still
    uses entscheidsuche.ch as a discovery layer.
    """
    import httpx

    API_URL = "https://entscheidsuche.ch/_search.php"
    BATCH_SIZE = 100

    if to_date is None:
        to_date = date.today()
    if from_date is None:
        from_date = date(2007, 1, 1)

    print(f"Scraping BVGer via entscheidsuche.ch mirrors from {from_date} to {to_date}...")

    stats = ScraperStats()

    with get_session() as session:
        existing_count = session.exec(select(func.count(Decision.id)).where(
            Decision.source_id == "bvger"
        )).one()
        print(f"Existing BVGer decisions in DB: {existing_count}")

        search_after = None

        while True:
            rate_limiter.wait()

            # Query for BVGer decisions - identified by ID pattern CH_BVGE_*
            query = {
                "bool": {
                    "must": [
                        {"term": {"canton": "CH"}},
                        {"prefix": {"id": "CH_BVGE_"}},
                    ],
                    "filter": [
                        {"range": {"date": {"gte": from_date.isoformat(), "lte": to_date.isoformat()}}}
                    ]
                }
            }

            body: dict[str, Any] = {
                "query": query,
                "size": BATCH_SIZE,
                "sort": [{"date": "desc"}, {"_id": "asc"}],
                "_source": ["id", "date", "canton", "title", "abstract", "attachment", "hierarchy", "reference"]
            }

            if search_after:
                body["search_after"] = search_after

            try:
                resp = httpx.post(API_URL, json=body, timeout=60)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                print(f"  Error fetching: {e}")
                stats.add_error()
                break

            hits = data.get("hits", {}).get("hits", [])
            if not hits:
                break

            search_after = hits[-1].get("sort")

            for hit in hits:
                if limit and stats.imported >= limit:
                    break

                src = hit.get("_source", {})
                doc_id = src.get("id") or hit.get("_id")

                # Extract attachment info first (needed for URL check)
                attachment = src.get("attachment", {})
                content_url = attachment.get("content_url", "")
                url = content_url or f"https://bvger.weblaw.ch/cache/{doc_id}"

                # Generate stable ID
                stable_id = stable_uuid_url(f"bvger:{doc_id}")

                # Check if exists by ID
                existing = session.get(Decision, stable_id)
                if existing:
                    stats.add_skipped()
                    continue

                # Also check by URL (for records imported via entscheidsuche.ch importer)
                existing_by_url = session.exec(
                    select(Decision).where(Decision.url == url)
                ).first()
                if existing_by_url:
                    stats.add_skipped()
                    continue

                # Download PDF if available
                content = None
                if content_url and content_url.endswith(".pdf"):
                    try:
                        rate_limiter.wait()
                        pdf_resp = httpx.get(content_url, timeout=120, follow_redirects=True)
                        pdf_resp.raise_for_status()
                        content = extract_pdf_text(pdf_resp.content)
                    except Exception as e:
                        print(f"    Error downloading PDF: {e}")
                else:
                    # Use pre-extracted content from entscheidsuche
                    content = attachment.get("content", "")

                if not content or len(content) < 100:
                    stats.add_skipped()
                    continue

                # Parse date
                date_str = src.get("date")
                decision_date = None
                if date_str:
                    try:
                        decision_date = date.fromisoformat(date_str)
                    except ValueError:
                        decision_date = parse_date_flexible(date_str)

                # Extract case number from doc_id: CH_BVGE_001_E-1857-2025_2026-01-21
                case_number = None
                case_match = re.search(r"([A-Z]-\d+-\d{4})", doc_id)
                if case_match:
                    case_number = case_match.group(1)

                # Get title
                title_obj = src.get("title", {})
                title = title_obj.get("de") or title_obj.get("fr") or title_obj.get("it") or doc_id

                # Get language
                language = attachment.get("language", "de")

                try:
                    dec = Decision(
                        id=stable_id,
                        source_id="bvger",
                        source_name="Bundesverwaltungsgericht",
                        level="federal",
                        canton=None,
                        court="Bundesverwaltungsgericht",
                        chamber=None,
                        docket=case_number,
                        decision_date=decision_date,
                        published_date=None,
                        title=f"BVGer {case_number}" if case_number else title[:500],
                        language=language,
                        url=url,
                        pdf_url=content_url if content_url.endswith(".pdf") else None,
                        content_text=content,
                        content_hash=compute_hash(content),
                        meta={
                            "source": "bvger.weblaw.ch (via entscheidsuche.ch)",
                            "doc_id": doc_id,
                            "hierarchy": src.get("hierarchy"),
                        },
                    )
                    session.add(dec)
                    stats.add_imported()

                    if stats.imported % 100 == 0:
                        print(f"  Imported {stats.imported} (skipped {stats.skipped})...")
                        session.commit()

                except Exception as e:
                    print(f"  Error saving: {e}")
                    stats.add_error()
                    continue

            if limit and stats.imported >= limit:
                break

        session.commit()
        print(stats.summary("BVGer (entscheidsuche mirrors)"))
        return stats.imported


def scrape_bvger_direct(
    limit: int | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
    use_playwright: bool = False,
) -> int:
    """Scrape decisions from BVGer.

    Args:
        limit: Maximum number of decisions to import
        from_date: Only import decisions on or after this date
        to_date: Only import decisions on or before this date
        use_playwright: If True, use Playwright to scrape the SPA directly.
                       If False (default), use entscheidsuche.ch mirrors.

    Returns:
        Number of decisions imported
    """
    print("Scraping Bundesverwaltungsgericht (Federal Administrative Court)...")

    if use_playwright:
        return scrape_with_playwright(from_date, to_date, limit)
    else:
        return scrape_bvger_via_entscheidsuche(from_date, to_date, limit)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape decisions from BVGer")
    parser.add_argument("--limit", type=int, help="Max decisions to import")
    parser.add_argument("--from-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to-date", help="End date (YYYY-MM-DD)")
    parser.add_argument("--playwright", action="store_true",
                       help="Use Playwright to scrape SPA directly (experimental)")
    args = parser.parse_args()

    from_dt = date.fromisoformat(args.from_date) if args.from_date else None
    to_dt = date.fromisoformat(args.to_date) if args.to_date else None

    scrape_bvger_direct(
        limit=args.limit,
        from_date=from_dt,
        to_date=to_dt,
        use_playwright=args.playwright
    )
