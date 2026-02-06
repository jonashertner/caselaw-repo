#!/usr/bin/env python3
"""Import decisions from entscheidsuche.ch API.

This API provides 700K+ Swiss court decisions from all cantons.

Usage:
    python scripts/import_entscheidsuche.py [--canton XX] [--limit N] [--skip-federal]
"""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import httpx

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlmodel import select

from app.db.session import get_session
from app.models.decision import Decision
from app.services.indexer import stable_uuid_url

from scripts.scraper_common import (
    DEFAULT_HEADERS,
    RateLimiter,
    ScraperStats,
    compute_hash,
    retry,
)

API_URL = "https://entscheidsuche.ch/_search.php"
BATCH_SIZE = 100  # Elasticsearch default max

# Rate limiter: 5 requests per second (API is fast)
rate_limiter = RateLimiter(requests_per_second=5.0)


@retry(max_attempts=3, backoff_base=2.0)
def fetch_decisions(canton: str | None, search_after: list | None = None, size: int = BATCH_SIZE) -> dict:
    """Fetch decisions from entscheidsuche API using search_after for deep pagination."""
    rate_limiter.wait()

    query: dict = {"match_all": {}}
    if canton:
        query = {"term": {"canton": canton}}

    body = {
        "query": query,
        "size": size,
        "sort": [{"date": "desc"}, {"_id": "asc"}],  # Need two sort fields for search_after
        "_source": ["id", "date", "canton", "title", "abstract", "attachment", "hierarchy", "reference"]
    }

    if search_after:
        body["search_after"] = search_after

    resp = httpx.post(API_URL, json=body, timeout=60, headers=DEFAULT_HEADERS)
    resp.raise_for_status()
    return resp.json()


def map_canton_to_source(canton: str, hierarchy: list[str] | None) -> tuple[str, str, str]:
    """Map canton code to source_id, source_name, level.

    For federal courts (CH), check hierarchy to distinguish:
    - CH_BGer -> Bundesgericht
    - CH_BVGE -> Bundesverwaltungsgericht
    - CH_BStGer -> Bundesstrafgericht
    - CH_BPatGer -> Bundespatentgericht
    """
    # Check for specific federal courts first
    if canton == "CH" and hierarchy:
        hierarchy_str = " ".join(hierarchy)
        if "BVGE" in hierarchy_str or "BVGer" in hierarchy_str:
            return ("bvger", "Bundesverwaltungsgericht", "federal")
        if "BStGer" in hierarchy_str:
            return ("bstger", "Bundesstrafgericht", "federal")
        if "BPatGer" in hierarchy_str:
            return ("bpatger", "Bundespatentgericht", "federal")

    canton_names = {
        "CH": ("bger", "Bundesgericht", "federal"),
        "AG": ("ag", "Aargau Gerichte", "cantonal"),
        "AI": ("ai", "Appenzell Innerrhoden", "cantonal"),
        "AR": ("ar", "Appenzell Ausserrhoden", "cantonal"),
        "BE": ("be", "Bern Gerichte", "cantonal"),
        "BL": ("bl", "Basel-Landschaft", "cantonal"),
        "BS": ("bs", "Basel-Stadt", "cantonal"),
        "FR": ("fr", "Freiburg/Fribourg", "cantonal"),
        "GE": ("ge", "Genève", "cantonal"),
        "GL": ("gl", "Glarus", "cantonal"),
        "GR": ("gr", "Graubünden", "cantonal"),
        "JU": ("ju", "Jura", "cantonal"),
        "LU": ("lu", "Luzern", "cantonal"),
        "NE": ("ne", "Neuchâtel", "cantonal"),
        "NW": ("nw", "Nidwalden", "cantonal"),
        "OW": ("ow", "Obwalden", "cantonal"),
        "SG": ("sg", "St. Gallen", "cantonal"),
        "SH": ("sh", "Schaffhausen", "cantonal"),
        "SO": ("so", "Solothurn", "cantonal"),
        "SZ": ("sz", "Schwyz", "cantonal"),
        "TG": ("tg", "Thurgau", "cantonal"),
        "TI": ("ti", "Ticino", "cantonal"),
        "UR": ("ur", "Uri", "cantonal"),
        "VD": ("vd", "Vaud", "cantonal"),
        "VS": ("vs", "Valais/Wallis", "cantonal"),
        "ZG": ("zg", "Zug", "cantonal"),
        "ZH": ("zh", "Zürich", "cantonal"),
    }
    return canton_names.get(canton, (canton.lower(), canton, "cantonal"))


def import_entscheidsuche(
    canton: str | None = None,
    limit: int | None = None,
    skip_federal: bool = False,
    dry_run: bool = False,
) -> int:
    """Import decisions from entscheidsuche.ch.

    Args:
        canton: Filter by canton code (e.g., ZH, BE)
        limit: Max decisions to import
        skip_federal: Skip federal (CH) decisions
        dry_run: Report only, don't import

    Returns:
        Number of decisions imported (or would be imported in dry-run)
    """
    mode = "[DRY RUN] " if dry_run else ""
    print(f"{mode}Checking entscheidsuche.ch (canton={canton or 'all'}, limit={limit or 'none'})...")

    stats = ScraperStats()

    with get_session() as session:
        search_after = None

        while True:
            try:
                data = fetch_decisions(canton, search_after)
            except Exception as e:
                print(f"  Error fetching (giving up after retries): {e}")
                stats.add_error()
                break

            hits = data.get("hits", {}).get("hits", [])
            if not hits:
                break

            # Get sort values from last hit for next page
            last_hit = hits[-1]
            search_after = last_hit.get("sort")

            for hit in hits:
                src = hit.get("_source", {})
                doc_id = src.get("id") or hit.get("_id")
                doc_canton = src.get("canton", "")

                # Skip federal if requested
                if skip_federal and doc_canton == "CH":
                    stats.add_skipped()
                    continue

                # Generate stable ID
                stable_id = stable_uuid_url(f"entscheidsuche:{doc_id}")

                # Extract content
                attachment = src.get("attachment", {})
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
                        pass

                # Map to source
                source_id, source_name, level = map_canton_to_source(
                    doc_canton, src.get("hierarchy")
                )

                # Get title
                title_obj = src.get("title", {})
                title = title_obj.get("de") or title_obj.get("fr") or title_obj.get("it") or doc_id

                # Get URL
                content_url = attachment.get("content_url", "")
                url = content_url or f"https://entscheidsuche.ch/docs/{doc_id}"

                # Get language
                language = attachment.get("language")

                # Check if decision already exists (for gap verification)
                existing = session.exec(
                    select(Decision.id).where(Decision.id == stable_id)
                ).first()

                if existing:
                    stats.add_skipped()
                    continue

                # Gap found - decision not in our database
                if dry_run:
                    # Report gap but don't import
                    print(f"  [GAP] {doc_canton} | {decision_date or 'unknown'} | {title[:60] if title else doc_id}...")
                    stats.add_imported()  # Count as "would import"
                else:
                    try:
                        dec = Decision(
                            id=stable_id,
                            source_id=source_id,
                            source_name=source_name,
                            level=level,
                            canton=doc_canton if doc_canton != "CH" else None,
                            court=None,
                            chamber=None,
                            docket=None,
                            decision_date=decision_date,
                            published_date=None,
                            title=title[:500] if title else None,
                            language=language,
                            url=url,
                            pdf_url=content_url if content_url.endswith(".pdf") else None,
                            content_text=content,
                            content_hash=compute_hash(content),
                            meta={
                                "source": "entscheidsuche.ch",
                                "hierarchy": src.get("hierarchy"),
                                "reference": src.get("reference"),
                            },
                        )
                        session.merge(dec)
                        stats.add_imported()
                    except Exception as e:
                        stats.add_error()
                        continue

                if stats.imported % 1000 == 0:
                    action = "Found" if dry_run else "Imported"
                    print(f"  {action} {stats.imported} gaps (skipped {stats.skipped} existing)...")
                    if not dry_run:
                        session.commit()

                if limit and stats.imported >= limit:
                    break

            if limit and stats.imported >= limit:
                break

        if not dry_run:
            session.commit()

        action = "would import" if dry_run else "imported"
        print(f"\n=== Summary ===")
        print(f"Gaps found ({action}): {stats.imported}")
        print(f"Already in database: {stats.skipped}")
        print(f"Errors: {stats.errors}")
        return stats.imported


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Verify gaps using entscheidsuche.ch (competitor database for verification only)"
    )
    parser.add_argument("--canton", help="Filter by canton code (e.g., ZH, BE, GE)")
    parser.add_argument("--limit", type=int, help="Max decisions to check")
    parser.add_argument("--skip-federal", action="store_true", help="Skip federal (CH) decisions")
    parser.add_argument("--dry-run", action="store_true", help="Report gaps only, don't import")
    args = parser.parse_args()

    import_entscheidsuche(
        canton=args.canton,
        limit=args.limit,
        skip_federal=args.skip_federal,
        dry_run=args.dry_run,
    )
