#!/usr/bin/env python3
"""Scrape decisions from Geneva (GE) via entscheidsuche.ch.

This scraper uses search_after pagination to get all GE decisions,
bypassing the 10,000 document limit per query.

Total available: ~88,700 decisions

Usage:
    python scripts/scrape_ge.py [--limit N] [--from-date YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import httpx

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

API_URL = "https://entscheidsuche.ch/_search.php"
BATCH_SIZE = 100

# Rate limiter: 3 requests per second (entscheidsuche API is fast)
rate_limiter = RateLimiter(requests_per_second=3.0)


@retry(max_attempts=3, backoff_base=2.0)
def fetch_ge_decisions(
    search_after: list | None = None,
    from_date: date | None = None,
    size: int = BATCH_SIZE
) -> dict:
    """Fetch GE decisions from entscheidsuche.ch API."""
    rate_limiter.wait()

    query = {"term": {"canton": "GE"}}

    if from_date:
        query = {
            "bool": {
                "must": [
                    {"term": {"canton": "GE"}},
                    {"range": {"date": {"gte": from_date.isoformat()}}}
                ]
            }
        }

    body = {
        "query": query,
        "size": size,
        "sort": [{"date": "desc"}, {"_id": "asc"}],
        "_source": ["id", "date", "canton", "title", "abstract", "attachment", "hierarchy", "reference"]
    }

    if search_after:
        body["search_after"] = search_after

    resp = httpx.post(API_URL, json=body, timeout=60, headers=DEFAULT_HEADERS)
    resp.raise_for_status()
    return resp.json()


def extract_court_from_hierarchy(hierarchy: list[str]) -> tuple[str | None, str | None]:
    """Extract court and chamber from GE hierarchy codes."""
    court = "Pouvoir judiciaire"
    chamber = None

    court_map = {
        "GE_CJ": ("Cour de justice", None),
        "GE_CJ_001": ("Cour de justice", "Chambre civile"),
        "GE_CJ_002": ("Cour de justice", "Chambre des assurances sociales"),
        "GE_CJ_003": ("Cour de justice", "Chambre administrative"),
        "GE_CJ_004": ("Cour de justice", "Chambre pénale d'appel"),
        "GE_CJ_005": ("Cour de justice", "Chambre de surveillance"),
        "GE_CJ_006": ("Cour de justice", "Cour de droit public"),
        "GE_CJ_007": ("Cour de justice", "Chambre des prud'hommes"),
        "GE_CJ_009": ("Cour de justice", "Chambre des baux et loyers"),
        "GE_CJ_011": ("Cour de justice", "Chambre pénale de recours"),
        "GE_CJ_013": ("Cour de justice", "Chambre de la Cour de justice"),
        "GE_CJ_014": ("Cour de justice", "Présidence"),
        "GE_CJ_015": ("Cour de justice", "Tribunal arbitral"),
        "GE_TAPI": ("Tribunal administratif de première instance", None),
        "GE_TAPI_001": ("Tribunal administratif de première instance", "Chambre TAPI"),
        "GE_TP": ("Tribunal pénal", None),
        "GE_TP_001": ("Tribunal pénal", "Chambre pénale"),
        "GE_CAPJ": ("Cour d'appel du Pouvoir judiciaire", None),
    }

    for h in hierarchy:
        if h in court_map:
            court, chamber = court_map[h]
            break

    return court, chamber


def get_string_value(value, lang_priority: list[str] = ["fr", "de", "it", "en"]) -> str:
    """Extract string from potentially multilingual dict value."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        for lang in lang_priority:
            if lang in value and value[lang]:
                return str(value[lang])
        for v in value.values():
            if v:
                return str(v)
    return str(value)


def detect_language(text: str) -> str:
    """Detect language from text content."""
    try:
        from langdetect import detect
        lang = detect(text[:1000] if len(text) > 1000 else text)
        return {"de": "de", "fr": "fr", "it": "it", "en": "en"}.get(lang, "fr")
    except Exception:
        return "fr"  # Default to French for Geneva


def scrape_ge(
    limit: int | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> int:
    """Scrape GE decisions from entscheidsuche.ch.

    Returns:
        Number of decisions imported
    """
    print("Scraping Geneva (GE) decisions from entscheidsuche.ch...")
    if from_date:
        print(f"  Date filter: from {from_date}")

    stats = ScraperStats()

    with get_session() as session:
        existing_count = session.exec(
            select(func.count(Decision.id)).where(Decision.canton == "GE")
        ).one()
        print(f"Existing GE decisions in DB: {existing_count}")

        search_after = None
        batch_num = 0

        while True:
            batch_num += 1
            try:
                result = fetch_ge_decisions(search_after, from_date)
            except Exception as e:
                print(f"Error fetching batch {batch_num} (giving up after retries): {e}")
                stats.add_error()
                break

            hits = result.get("hits", {}).get("hits", [])
            if not hits:
                break

            if batch_num == 1:
                total = result.get("hits", {}).get("total", {}).get("value", 0)
                print(f"Total available (capped at 10000 shown): {total}")

            for hit in hits:
                doc = hit.get("_source", {})
                doc_id = doc.get("id", "")

                # Generate stable ID
                stable_id = stable_uuid_url(f"entscheidsuche:{doc_id}")

                # Check if exists
                existing = session.get(Decision, stable_id)
                if existing:
                    stats.add_skipped()
                    search_after = hit.get("sort")
                    continue

                # Extract content
                attachment = doc.get("attachment", {})
                if isinstance(attachment, dict):
                    content = get_string_value(attachment.get("content", ""))
                else:
                    content = ""

                abstract = get_string_value(doc.get("abstract", ""))

                if abstract and content:
                    full_content = f"{abstract}\n\n{content}"
                else:
                    full_content = abstract or content

                if not full_content or len(full_content) < 50:
                    stats.add_skipped()
                    search_after = hit.get("sort")
                    continue

                # Parse date
                decision_date = None
                date_str = doc.get("date")
                if date_str:
                    try:
                        decision_date = date.fromisoformat(date_str[:10])
                    except ValueError:
                        pass

                # Apply to_date filter if specified
                if to_date and decision_date and decision_date > to_date:
                    stats.add_skipped()
                    search_after = hit.get("sort")
                    continue

                # Extract metadata
                hierarchy = doc.get("hierarchy", [])
                if isinstance(hierarchy, str):
                    hierarchy = [hierarchy]

                court, chamber = extract_court_from_hierarchy(hierarchy)
                language = detect_language(full_content)

                # Build URL
                content_url = f"https://entscheidsuche.ch/docs/{doc_id}"

                # Extract title
                title = get_string_value(doc.get("title")) or f"GE {doc_id}"
                reference = get_string_value(doc.get("reference"))

                # Create decision
                try:
                    dec = Decision(
                        id=stable_id,
                        source_id="ge",
                        source_name="Genève",
                        level="cantonal",
                        canton="GE",
                        court=court,
                        chamber=chamber,
                        docket=reference or None,
                        decision_date=decision_date,
                        published_date=None,
                        title=title[:500] if title else f"GE Decision",
                        language=language,
                        url=content_url,
                        pdf_url=f"{content_url}.pdf" if doc_id else None,
                        content_text=full_content,
                        content_hash=compute_hash(full_content),
                        meta={
                            "source": "entscheidsuche.ch",
                            "hierarchy": hierarchy,
                            "reference": reference,
                        },
                    )
                    session.add(dec)
                    stats.add_imported()

                    if stats.imported % 500 == 0:
                        print(f"  Imported {stats.imported} (skipped {stats.skipped})...")
                        session.commit()

                    if limit and stats.imported >= limit:
                        break

                except Exception as e:
                    print(f"  Error saving {doc_id}: {e}")
                    stats.add_error()

                search_after = hit.get("sort")

            if limit and stats.imported >= limit:
                break

        session.commit()
        print(stats.summary("Geneva (GE)"))

        # Final count
        final_count = session.exec(
            select(func.count(Decision.id)).where(Decision.canton == "GE")
        ).one()
        print(f"Total GE decisions in DB: {final_count}")

        return stats.imported


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape Geneva decisions")
    parser.add_argument("--limit", type=int, help="Max decisions to import")
    parser.add_argument("--from-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to-date", help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    from_dt = date.fromisoformat(args.from_date) if args.from_date else None
    to_dt = date.fromisoformat(args.to_date) if args.to_date else None
    scrape_ge(limit=args.limit, from_date=from_dt, to_date=to_dt)
