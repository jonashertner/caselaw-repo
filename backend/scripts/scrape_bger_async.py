#!/usr/bin/env python3
"""Async/parallel scraper for bger.ch (Federal Supreme Court).

Optimized for speed with:
- Async HTTP requests with connection pooling
- Concurrent fetching (configurable workers)
- Batch database inserts
- Rate limiting per-worker

Usage:
    python scripts/scrape_bger_async.py [--workers 10] [--rate 5]
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import re
import sys
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import unquote

import httpx
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlmodel import select, func
from app.db.session import get_session
from app.models.decision import Decision
from app.services.indexer import stable_uuid_url
from scripts.scraper_common import compute_hash, upsert_decision, ScraperStats

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

BASE_URL = "https://www.bger.ch/ext/eurospider/live/de/php/aza/http/index.php"
DEFAULT_HEADERS = {
    "User-Agent": "swiss-caselaw-ai/0.1 (+https://github.com/jonashertner/swiss-caselaw)"
}


class AsyncRateLimiter:
    """Async-compatible rate limiter."""
    
    def __init__(self, requests_per_second: float = 5.0):
        self.min_interval = 1.0 / requests_per_second
        self.lock = asyncio.Lock()
        self.last_request_time = 0.0
    
    async def acquire(self):
        async with self.lock:
            now = asyncio.get_event_loop().time()
            elapsed = now - self.last_request_time
            if elapsed < self.min_interval:
                await asyncio.sleep(self.min_interval - elapsed)
            self.last_request_time = asyncio.get_event_loop().time()


async def fetch_decision_list(
    client: httpx.AsyncClient,
    rate_limiter: AsyncRateLimiter,
    from_date: date,
    to_date: date,
    page: int = 1
) -> tuple[list[dict], int]:
    """Fetch list of decisions for a date range."""
    await rate_limiter.acquire()
    
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

    for attempt in range(3):
        try:
            resp = await client.get(BASE_URL, params=params, timeout=60)
            resp.raise_for_status()
            break
        except Exception as e:
            if attempt == 2:
                logger.error(f"Failed to fetch list page {page}: {e}")
                return [], 0
            await asyncio.sleep(2 ** attempt)

    soup = BeautifulSoup(resp.text, "html.parser")

    total = 0
    count_match = re.search(r"(\d+)\s*Dokumente?", resp.text)
    if count_match:
        total = int(count_match.group(1))

    decisions = []
    for link in soup.find_all("a", href=True):
        href = link.get("href", "")
        if "highlight_docid=aza" in href:
            match = re.search(r"highlight_docid=aza%3A%2F%2F([^&]+)", href)
            if match:
                doc_id = unquote(match.group(1))
                parts = doc_id.split("-")
                if len(parts) >= 4:
                    case_date = f"{parts[2]}-{parts[1]}-{parts[0]}"
                    case_number = "-".join(parts[3:])
                    decisions.append({
                        "doc_id": doc_id,
                        "case_number": case_number,
                        "date": case_date,
                        "url": f"https://www.bger.ch{href}" if href.startswith("/") else href,
                    })

    return decisions, total


async def fetch_decision_content(
    client: httpx.AsyncClient,
    rate_limiter: AsyncRateLimiter,
    url: str
) -> str | None:
    """Fetch the full text content of a decision."""
    await rate_limiter.acquire()
    
    for attempt in range(3):
        try:
            resp = await client.get(url, timeout=60)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            
            soup = BeautifulSoup(resp.text, "html.parser")
            content_div = soup.find("div", {"class": "content"})
            if content_div:
                return content_div.get_text(separator="\n", strip=True)
            
            # Fallback to body
            body = soup.find("body")
            if body:
                return body.get_text(separator="\n", strip=True)
            return None
        except Exception as e:
            if attempt == 2:
                logger.error(f"Failed to fetch content {url}: {e}")
                return None
            await asyncio.sleep(2 ** attempt)


async def process_decision(
    client: httpx.AsyncClient,
    rate_limiter: AsyncRateLimiter,
    decision: dict,
    stats: ScraperStats,
    semaphore: asyncio.Semaphore
) -> dict | None:
    """Process a single decision - fetch content and prepare for DB."""
    async with semaphore:
        content = await fetch_decision_content(client, rate_limiter, decision["url"])
        if not content:
            stats.add_skipped()
            return None
        
        return {
            "source_id": f"bger:{decision['doc_id']}",
            "source_name": "bger",
            "level": "federal",
            "canton": None,
            "court": "Bundesgericht",
            "chamber": None,
            "docket": decision["case_number"],
            "decision_date": decision["date"],
            "url": decision["url"],
            "content_text": content,
            "content_hash": compute_hash(content),
        }


async def scrape_bger_async(
    from_date: date | None = None,
    to_date: date | None = None,
    workers: int = 10,
    requests_per_second: float = 5.0,
) -> int:
    """Scrape BGer with async parallelism."""
    
    if to_date is None:
        to_date = date.today()
    if from_date is None:
        from_date = date(1954, 1, 1)  # BGer archives go back to ~1954
    
    stats = ScraperStats()
    rate_limiter = AsyncRateLimiter(requests_per_second)
    semaphore = asyncio.Semaphore(workers)
    
    logger.info(f"Starting async BGer scrape: {from_date} to {to_date}")
    logger.info(f"Workers: {workers}, Rate: {requests_per_second}/sec")
    
    async with httpx.AsyncClient(headers=DEFAULT_HEADERS, follow_redirects=True) as client:
        # Process by month to avoid huge result sets
        current_start = from_date
        
        while current_start <= to_date:
            current_end = min(current_start + timedelta(days=30), to_date)
            
            # Fetch first page to get total count
            decisions, total = await fetch_decision_list(
                client, rate_limiter, current_start, current_end, page=1
            )
            
            if total == 0:
                current_start = current_end + timedelta(days=1)
                continue
            
            logger.info(f"Period {current_start} to {current_end}: {total} decisions")
            
            # Fetch all pages
            all_decisions = decisions.copy()
            pages = (total + 9) // 10  # 10 results per page
            
            if pages > 1:
                page_tasks = [
                    fetch_decision_list(client, rate_limiter, current_start, current_end, page=p)
                    for p in range(2, pages + 1)
                ]
                page_results = await asyncio.gather(*page_tasks)
                for page_decisions, _ in page_results:
                    all_decisions.extend(page_decisions)
            
            # Process all decisions in parallel
            tasks = [
                process_decision(client, rate_limiter, d, stats, semaphore)
                for d in all_decisions
            ]
            results = await asyncio.gather(*tasks)
            
            # Batch insert to database
            batch = [r for r in results if r is not None]
            if batch:
                with get_session() as session:
                    for decision_data in batch:
                        try:
                            upsert_decision(session, decision_data)
                            stats.add_imported()
                        except Exception as e:
                            logger.error(f"DB error: {e}")
                            stats.add_error()
                    session.commit()
            
            logger.info(f"  Processed {len(batch)} decisions. Total: {stats.imported}")
            
            current_start = current_end + timedelta(days=1)
    
    logger.info(f"BGer Async complete: {stats.imported} imported, {stats.skipped} skipped, {stats.errors} errors")
    return stats.imported


def main():
    parser = argparse.ArgumentParser(description="Async BGer scraper")
    parser.add_argument("--from-date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to-date", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("--workers", type=int, default=10, help="Concurrent workers")
    parser.add_argument("--rate", type=float, default=5.0, help="Requests per second")
    args = parser.parse_args()
    
    from_date = date.fromisoformat(args.from_date) if args.from_date else None
    to_date = date.fromisoformat(args.to_date) if args.to_date else None
    
    count = asyncio.run(scrape_bger_async(
        from_date=from_date,
        to_date=to_date,
        workers=args.workers,
        requests_per_second=args.rate,
    ))
    
    print(f"Scraped {count} decisions")


if __name__ == "__main__":
    main()
