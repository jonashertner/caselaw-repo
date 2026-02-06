#!/usr/bin/env python3
"""Scrape historical BGE (Bundesgerichtsentscheide) by volume.

The official BGE collection is organized by volume number (1-150+), 
starting from 1875. Each volume contains decisions from a calendar year,
organized by legal area (I-V).

This scraper iterates through all BGE volumes to capture historical
decisions not available through the standard date-based search.

BGE Structure:
- Volume 1 (1875) through current (~150+)
- Sections: I (Constitutional), II (Civil), III (Criminal), IV (Administrative), V (Social)
- Citation format: BGE 129 III 123 = Volume 129, Section III, Page 123

Usage:
    python scripts/scrape_bge_historical.py [--start-volume N] [--end-volume N]
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import re
import sys
from datetime import date
from pathlib import Path
from urllib.parse import urljoin, unquote

import httpx
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlmodel import select, func
from app.db.session import get_session
from app.models.decision import Decision
from scripts.scraper_common import compute_hash, upsert_decision, ScraperStats

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# BGE search endpoint
BASE_URL = "https://www.bger.ch/ext/eurospider/live/de/php/clir/http/index.php"
DEFAULT_HEADERS = {
    "User-Agent": "swiss-caselaw-ai/0.1 (+https://github.com/jonashertner/swiss-caselaw)"
}

# BGE sections (Abteilungen)
BGE_SECTIONS = ["I", "II", "III", "IV", "V"]

# Volume to approximate year mapping (BGE volume 1 = 1875)
def volume_to_year(volume: int) -> int:
    """Approximate year for a BGE volume."""
    return 1874 + volume


class AsyncRateLimiter:
    """Async-compatible rate limiter."""
    
    def __init__(self, requests_per_second: float = 2.0):
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


async def search_bge_volume(
    client: httpx.AsyncClient,
    rate_limiter: AsyncRateLimiter,
    volume: int,
    section: str | None = None,
    page: int = 1,
) -> tuple[list[dict], int]:
    """Search for BGE decisions by volume number."""
    await rate_limiter.acquire()
    
    # Build query for specific volume
    query = f"BGE {volume}"
    if section:
        query += f" {section}"
    
    params = {
        "lang": "de",
        "type": "simple_query",
        "query_words": query,
        "collection": "bge",
        "page": page,
    }
    
    try:
        resp = await client.get(BASE_URL, params=params, timeout=60, follow_redirects=True)
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to search volume {volume}: {e}")
        return [], 0
    
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
        text = link.get_text(strip=True)
        
        # Look for BGE citations in links
        bge_match = re.search(r"BGE[_\s]+(\d+)[_\s]+([IVX]+)[_\s]+(\d+)", text + " " + href)
        if bge_match:
            vol, sect, page_num = bge_match.groups()
            citation = f"BGE {vol} {sect} {page_num}"
            
            # Extract URL
            if href.startswith("/"):
                full_url = f"https://www.bger.ch{href}"
            elif href.startswith("http"):
                full_url = href
            else:
                continue
            
            decisions.append({
                "citation": citation,
                "volume": int(vol),
                "section": sect,
                "page": int(page_num),
                "url": full_url,
                "year": volume_to_year(int(vol)),
            })
    
    # Deduplicate by citation
    seen = set()
    unique = []
    for d in decisions:
        if d["citation"] not in seen:
            seen.add(d["citation"])
            unique.append(d)
    
    return unique, total


async def fetch_bge_content(
    client: httpx.AsyncClient,
    rate_limiter: AsyncRateLimiter,
    url: str,
) -> str | None:
    """Fetch the full text content of a BGE decision."""
    await rate_limiter.acquire()
    
    try:
        resp = await client.get(url, timeout=60, follow_redirects=True)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        
        soup = BeautifulSoup(resp.text, "html.parser")
        
        # Try various content selectors
        content = None
        for selector in [
            {"class": "content"},
            {"class": "paraatf"},
            {"id": "content"},
            "article",
        ]:
            if isinstance(selector, dict):
                elem = soup.find("div", selector)
            else:
                elem = soup.find(selector)
            if elem:
                content = elem.get_text(separator="\n", strip=True)
                break
        
        if not content:
            body = soup.find("body")
            if body:
                content = body.get_text(separator="\n", strip=True)
        
        return content
    except Exception as e:
        logger.error(f"Failed to fetch {url}: {e}")
        return None


async def process_volume(
    client: httpx.AsyncClient,
    rate_limiter: AsyncRateLimiter,
    volume: int,
    stats: ScraperStats,
    semaphore: asyncio.Semaphore,
) -> int:
    """Process all decisions from a single BGE volume."""
    logger.info(f"Processing BGE Volume {volume} (year ~{volume_to_year(volume)})")
    
    # Search for decisions in this volume
    decisions, total = await search_bge_volume(client, rate_limiter, volume)
    
    if not decisions:
        logger.info(f"  Volume {volume}: no decisions found")
        return 0
    
    logger.info(f"  Volume {volume}: found {len(decisions)} decisions (total claimed: {total})")
    
    # Fetch and save each decision
    saved = 0
    for decision in decisions:
        async with semaphore:
            content = await fetch_bge_content(client, rate_limiter, decision["url"])
            
            if not content or len(content) < 100:
                stats.add_skipped()
                continue
            
            # Prepare decision record
            record = {
                "source_id": f"bge:{decision['citation'].replace(' ', '_')}",
                "source_name": "bge",
                "level": "federal",
                "canton": None,
                "court": "Bundesgericht",
                "chamber": f"Abteilung {decision['section']}",
                "docket": decision["citation"],
                "decision_date": f"{decision['year']}-06-15",  # Approximate mid-year
                "url": decision["url"],
                "content_text": content,
                "content_hash": compute_hash(content),
            }
            
            try:
                result = upsert_decision(record)
                if result == "inserted":
                    stats.add_imported()
                    saved += 1
                elif result == "updated":
                    stats.add_imported()
                else:
                    stats.add_skipped()
            except Exception as e:
                logger.error(f"Failed to save {decision['citation']}: {e}")
                stats.add_error()
    
    return saved


async def scrape_bge_historical(
    start_volume: int = 1,
    end_volume: int = 150,
    workers: int = 5,
    requests_per_second: float = 2.0,
) -> int:
    """Scrape historical BGE decisions by volume."""
    
    stats = ScraperStats()
    rate_limiter = AsyncRateLimiter(requests_per_second)
    semaphore = asyncio.Semaphore(workers)
    
    logger.info(f"Starting BGE historical scrape: volumes {start_volume}-{end_volume}")
    logger.info(f"  Workers: {workers}, Rate: {requests_per_second} req/s")
    
    async with httpx.AsyncClient(headers=DEFAULT_HEADERS) as client:
        total_saved = 0
        
        for volume in range(start_volume, end_volume + 1):
            saved = await process_volume(
                client, rate_limiter, volume, stats, semaphore
            )
            total_saved += saved
            
            # Progress report every 10 volumes
            if volume % 10 == 0:
                logger.info(f"Progress: Volume {volume}/{end_volume}, Total saved: {total_saved}")
    
    logger.info(f"=== BGE Historical Scrape Complete ===")
    logger.info(f"Volumes processed: {start_volume}-{end_volume}")
    logger.info(f"Total saved: {total_saved}")
    logger.info(f"Stats: {stats.imported} imported, {stats.skipped} skipped, {stats.errors} errors")
    
    return total_saved


def scrape_bge_historical_sync(
    start_volume: int = 1,
    end_volume: int = 150,
    workers: int = 5,
    requests_per_second: float = 2.0,
) -> int:
    """Synchronous wrapper for the async scraper."""
    return asyncio.run(
        scrape_bge_historical(start_volume, end_volume, workers, requests_per_second)
    )


def main():
    parser = argparse.ArgumentParser(
        description="Scrape historical BGE decisions by volume number"
    )
    parser.add_argument(
        "--start-volume", type=int, default=1,
        help="Starting BGE volume (default: 1, year 1875)"
    )
    parser.add_argument(
        "--end-volume", type=int, default=150,
        help="Ending BGE volume (default: 150, year ~2024)"
    )
    parser.add_argument(
        "--workers", type=int, default=5,
        help="Concurrent workers (default: 5)"
    )
    parser.add_argument(
        "--rate", type=float, default=2.0,
        help="Requests per second (default: 2.0)"
    )
    args = parser.parse_args()
    
    count = scrape_bge_historical_sync(
        start_volume=args.start_volume,
        end_volume=args.end_volume,
        workers=args.workers,
        requests_per_second=args.rate,
    )
    
    print(f"\nTotal BGE decisions saved: {count}")


if __name__ == "__main__":
    main()
