#!/usr/bin/env python3
"""Robust import of decisions from entscheidsuche.ch API.

This script imports 700K+ Swiss court decisions with:
- Canton-by-canton processing for reliability
- Checkpointing and resume capability
- Proper transaction management
- Deduplication by URL
- Progress tracking

Usage:
    python scripts/import_entscheidsuche_robust.py
    python scripts/import_entscheidsuche_robust.py --canton OW  # Single canton
    python scripts/import_entscheidsuche_robust.py --resume     # Resume from checkpoint
    python scripts/import_entscheidsuche_robust.py --dry-run    # Count only
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any

import httpx

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlmodel import select, text
from sqlalchemy.exc import IntegrityError

from app.db.session import get_session
from app.models.decision import Decision
from app.services.indexer import stable_uuid_url

from scripts.scraper_common import (
    DEFAULT_HEADERS,
    compute_hash,
)

API_URL = "https://entscheidsuche.ch/_search.php"
BATCH_SIZE = 100  # Elasticsearch max per request
COMMIT_EVERY = 50  # Commit to DB every N decisions
CHECKPOINT_FILE = Path(__file__).parent / ".entscheidsuche_checkpoint.json"

# All cantons in entscheidsuche.ch
ALL_CANTONS = [
    "OW",  # Priority - we're missing 2,201!
    "GE",  # Priority - we're missing 5,432
    "AG", "AI", "AR", "BE", "BL", "BS", "FR", "GL", "GR",
    "JU", "LU", "NE", "NW", "SG", "SH", "SO", "SZ", "TA",
    "TG", "TI", "UR", "VD", "VS", "ZG", "ZH",
    "CH",  # Federal - do last (we already have most)
]

# Canton names for display
CANTON_NAMES = {
    "AG": "Aargau", "AI": "Appenzell I.", "AR": "Appenzell A.",
    "BE": "Bern", "BL": "Basel-Land", "BS": "Basel-Stadt",
    "CH": "Federal", "FR": "Fribourg", "GE": "Geneva",
    "GL": "Glarus", "GR": "Graubünden", "JU": "Jura",
    "LU": "Luzern", "NE": "Neuchâtel", "NW": "Nidwalden",
    "OW": "Obwalden", "SG": "St. Gallen", "SH": "Schaffhausen",
    "SO": "Solothurn", "SZ": "Schwyz", "TA": "Tessin Alt",
    "TG": "Thurgau", "TI": "Ticino", "UR": "Uri",
    "VD": "Vaud", "VS": "Valais", "ZG": "Zug", "ZH": "Zürich",
}


def log(msg: str):
    """Print with timestamp."""
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def fetch_decisions(
    canton: str | None = None,
    search_after: list | None = None,
    size: int = BATCH_SIZE,
    max_retries: int = 3,
) -> dict:
    """Fetch decisions from entscheidsuche API with retry logic."""
    query: dict = {"match_all": {}}
    if canton:
        query = {"term": {"canton": canton}}

    body = {
        "query": query,
        "size": size,
        "sort": [{"date": "desc"}, {"_id": "asc"}],
        "_source": ["id", "date", "canton", "title", "abstract", "attachment", "hierarchy", "reference"],
    }

    if search_after:
        body["search_after"] = search_after

    for attempt in range(max_retries):
        try:
            resp = httpx.post(API_URL, json=body, timeout=60, headers=DEFAULT_HEADERS)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            if attempt < max_retries - 1:
                wait = 2 ** attempt
                log(f"  Fetch error (retry in {wait}s): {e}")
                time.sleep(wait)
            else:
                raise


def get_canton_count(canton: str) -> int:
    """Get total count for a canton."""
    body = {
        "query": {"term": {"canton": canton}},
        "size": 0,
        "track_total_hits": True,
    }
    resp = httpx.post(API_URL, json=body, timeout=30, headers=DEFAULT_HEADERS)
    data = resp.json()
    return data.get("hits", {}).get("total", {}).get("value", 0)


def map_canton_to_source(canton: str, hierarchy: list[str] | None) -> tuple[str, str, str]:
    """Map canton code to source_id, source_name, level."""
    # Check for specific federal courts
    if canton == "CH" and hierarchy:
        hierarchy_str = " ".join(hierarchy)
        if "BVGE" in hierarchy_str or "BVGer" in hierarchy_str:
            return ("bvger_es", "Bundesverwaltungsgericht (entscheidsuche)", "federal")
        if "BStGer" in hierarchy_str:
            return ("bstger_es", "Bundesstrafgericht (entscheidsuche)", "federal")
        if "BPatGer" in hierarchy_str:
            return ("bpatger_es", "Bundespatentgericht (entscheidsuche)", "federal")

    canton_map = {
        "CH": ("bger_es", "Bundesgericht (entscheidsuche)", "federal"),
        "AG": ("ag_es", "Aargau (entscheidsuche)", "cantonal"),
        "AI": ("ai_es", "Appenzell I. (entscheidsuche)", "cantonal"),
        "AR": ("ar_es", "Appenzell A. (entscheidsuche)", "cantonal"),
        "BE": ("be_es", "Bern (entscheidsuche)", "cantonal"),
        "BL": ("bl_es", "Basel-Landschaft (entscheidsuche)", "cantonal"),
        "BS": ("bs_es", "Basel-Stadt (entscheidsuche)", "cantonal"),
        "FR": ("fr_es", "Fribourg (entscheidsuche)", "cantonal"),
        "GE": ("ge_es", "Genève (entscheidsuche)", "cantonal"),
        "GL": ("gl_es", "Glarus (entscheidsuche)", "cantonal"),
        "GR": ("gr_es", "Graubünden (entscheidsuche)", "cantonal"),
        "JU": ("ju_es", "Jura (entscheidsuche)", "cantonal"),
        "LU": ("lu_es", "Luzern (entscheidsuche)", "cantonal"),
        "NE": ("ne_es", "Neuchâtel (entscheidsuche)", "cantonal"),
        "NW": ("nw_es", "Nidwalden (entscheidsuche)", "cantonal"),
        "OW": ("ow_es", "Obwalden (entscheidsuche)", "cantonal"),
        "SG": ("sg_es", "St. Gallen (entscheidsuche)", "cantonal"),
        "SH": ("sh_es", "Schaffhausen (entscheidsuche)", "cantonal"),
        "SO": ("so_es", "Solothurn (entscheidsuche)", "cantonal"),
        "SZ": ("sz_es", "Schwyz (entscheidsuche)", "cantonal"),
        "TA": ("ta_es", "Tessin Alt (entscheidsuche)", "cantonal"),
        "TG": ("tg_es", "Thurgau (entscheidsuche)", "cantonal"),
        "TI": ("ti_es", "Ticino (entscheidsuche)", "cantonal"),
        "UR": ("ur_es", "Uri (entscheidsuche)", "cantonal"),
        "VD": ("vd_es", "Vaud (entscheidsuche)", "cantonal"),
        "VS": ("vs_es", "Valais (entscheidsuche)", "cantonal"),
        "ZG": ("zg_es", "Zug (entscheidsuche)", "cantonal"),
        "ZH": ("zh_es", "Zürich (entscheidsuche)", "cantonal"),
    }
    return canton_map.get(canton, (canton.lower() + "_es", f"{canton} (entscheidsuche)", "cantonal"))


def parse_decision(hit: dict) -> dict | None:
    """Parse a single decision from API response."""
    src = hit.get("_source", {})
    doc_id = src.get("id") or hit.get("_id")

    attachment = src.get("attachment", {})
    content = attachment.get("content", "")

    # Skip if no meaningful content
    if not content or len(content) < 50:
        return None

    # Get URL (primary deduplication key)
    content_url = attachment.get("content_url", "")
    url = content_url or f"https://entscheidsuche.ch/docs/{doc_id}"

    # Parse date
    date_str = src.get("date")
    decision_date = None
    if date_str:
        try:
            decision_date = date.fromisoformat(date_str)
        except ValueError:
            pass

    # Get canton
    canton = src.get("canton", "")

    # Map to source
    source_id, source_name, level = map_canton_to_source(canton, src.get("hierarchy"))

    # Get title
    title_obj = src.get("title", {})
    if isinstance(title_obj, dict):
        title = title_obj.get("de") or title_obj.get("fr") or title_obj.get("it") or doc_id
    else:
        title = str(title_obj) if title_obj else doc_id

    # Get language
    language = attachment.get("language")

    # Get reference/docket
    refs = src.get("reference", [])
    docket = refs[0] if refs else None

    return {
        "id": stable_uuid_url(f"entscheidsuche:{doc_id}"),
        "source_id": source_id,
        "source_name": source_name,
        "level": level,
        "canton": canton if canton != "CH" else None,
        "court": None,
        "chamber": None,
        "docket": docket,
        "decision_date": decision_date,
        "published_date": None,
        "title": title[:500] if title else None,
        "language": language,
        "url": url,
        "pdf_url": content_url if content_url and content_url.endswith(".pdf") else None,
        "content_text": content,
        "content_hash": compute_hash(content),
        "meta": {
            "source": "entscheidsuche.ch",
            "hierarchy": src.get("hierarchy"),
            "reference": src.get("reference"),
            "original_id": doc_id,
        },
    }


def load_checkpoint() -> dict:
    """Load checkpoint from file."""
    if CHECKPOINT_FILE.exists():
        return json.loads(CHECKPOINT_FILE.read_text())
    return {"completed_cantons": [], "current_canton": None, "search_after": None}


def save_checkpoint(checkpoint: dict):
    """Save checkpoint to file."""
    CHECKPOINT_FILE.write_text(json.dumps(checkpoint, indent=2))


def clear_checkpoint():
    """Clear checkpoint file."""
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()


def get_existing_urls(session, urls: list[str]) -> set[str]:
    """Check which URLs already exist in database."""
    if not urls:
        return set()

    # Query in batches to avoid too long queries
    existing = set()
    for i in range(0, len(urls), 100):
        batch = urls[i:i+100]
        result = session.exec(
            select(Decision.url).where(Decision.url.in_(batch))
        ).all()
        existing.update(result)
    return existing


def import_canton(
    canton: str,
    search_after: list | None = None,
    dry_run: bool = False,
    skip_existing_check: bool = False,
) -> tuple[int, int, int, list | None]:
    """Import all decisions for a single canton.

    Returns:
        (imported, skipped, errors, last_search_after)
    """
    imported = 0
    skipped = 0
    errors = 0

    canton_name = CANTON_NAMES.get(canton, canton)
    total = get_canton_count(canton)
    log(f"Processing {canton} ({canton_name}): {total:,} decisions")

    if total == 0:
        return 0, 0, 0, None

    batch_num = 0

    with get_session() as session:
        while True:
            batch_num += 1

            try:
                data = fetch_decisions(canton, search_after, BATCH_SIZE)
            except Exception as e:
                log(f"  Fatal fetch error: {e}")
                errors += 1
                break

            hits = data.get("hits", {}).get("hits", [])
            if not hits:
                break

            # Get sort values for next page
            last_hit = hits[-1]
            search_after = last_hit.get("sort")

            # Parse all decisions in this batch
            parsed = []
            for hit in hits:
                try:
                    dec_data = parse_decision(hit)
                    if dec_data:
                        parsed.append(dec_data)
                    else:
                        skipped += 1
                except Exception as e:
                    errors += 1

            if not parsed:
                continue

            # Check which URLs already exist
            if not skip_existing_check:
                urls = [d["url"] for d in parsed]
                existing_urls = get_existing_urls(session, urls)
            else:
                existing_urls = set()

            # Insert new decisions
            batch_imported = 0
            for dec_data in parsed:
                if dec_data["url"] in existing_urls:
                    skipped += 1
                    continue

                if dry_run:
                    imported += 1
                    batch_imported += 1
                    continue

                try:
                    dec = Decision(**dec_data)
                    session.add(dec)
                    imported += 1
                    batch_imported += 1
                except Exception as e:
                    errors += 1

            # Commit periodically
            if not dry_run and batch_imported > 0:
                try:
                    session.commit()
                except IntegrityError as e:
                    # Handle duplicate URL (race condition or existing)
                    session.rollback()
                    log(f"  Integrity error (batch {batch_num}), retrying one-by-one...")
                    # Re-process this batch one by one
                    for dec_data in parsed:
                        if dec_data["url"] in existing_urls:
                            continue
                        try:
                            # Check again if exists
                            exists = session.exec(
                                select(Decision.id).where(Decision.url == dec_data["url"])
                            ).first()
                            if exists:
                                skipped += 1
                                imported -= 1  # Undo the count
                                continue
                            dec = Decision(**dec_data)
                            session.add(dec)
                            session.commit()
                        except IntegrityError:
                            session.rollback()
                            skipped += 1
                            imported -= 1
                        except Exception:
                            session.rollback()
                            errors += 1
                            imported -= 1

            # Progress
            processed = imported + skipped + errors
            pct = (processed / total * 100) if total > 0 else 0
            mode = "[DRY RUN] " if dry_run else ""
            log(f"  {mode}{canton}: {imported:,} imported, {skipped:,} skipped, {errors:,} errors ({pct:.1f}%)")

    return imported, skipped, errors, search_after


def import_all(
    cantons: list[str] | None = None,
    resume: bool = False,
    dry_run: bool = False,
    skip_federal: bool = False,
) -> dict:
    """Import decisions from all cantons.

    Returns:
        Summary dict with counts per canton
    """
    cantons = cantons or ALL_CANTONS

    # Load checkpoint if resuming
    checkpoint = load_checkpoint() if resume else {"completed_cantons": [], "current_canton": None, "search_after": None}

    if resume and checkpoint.get("completed_cantons"):
        log(f"Resuming from checkpoint. Completed: {checkpoint['completed_cantons']}")

    results = {}
    total_imported = 0
    total_skipped = 0
    total_errors = 0

    start_time = time.time()

    for canton in cantons:
        if skip_federal and canton == "CH":
            log(f"Skipping {canton} (federal)")
            continue

        if canton in checkpoint.get("completed_cantons", []):
            log(f"Skipping {canton} (already completed)")
            continue

        # Check if we're resuming this canton
        search_after = None
        if checkpoint.get("current_canton") == canton:
            search_after = checkpoint.get("search_after")
            if search_after:
                log(f"Resuming {canton} from checkpoint...")

        # Import this canton
        imported, skipped, errors, last_search_after = import_canton(
            canton, search_after, dry_run
        )

        results[canton] = {"imported": imported, "skipped": skipped, "errors": errors}
        total_imported += imported
        total_skipped += skipped
        total_errors += errors

        # Update checkpoint
        checkpoint["completed_cantons"].append(canton)
        checkpoint["current_canton"] = None
        checkpoint["search_after"] = None
        if not dry_run:
            save_checkpoint(checkpoint)

        log(f"Completed {canton}: +{imported:,} decisions")

    elapsed = time.time() - start_time

    # Clear checkpoint on successful completion
    if not dry_run:
        clear_checkpoint()

    # Summary
    log("")
    log("=" * 60)
    log("IMPORT COMPLETE")
    log("=" * 60)
    log(f"Total imported: {total_imported:,}")
    log(f"Total skipped (existing or empty): {total_skipped:,}")
    log(f"Total errors: {total_errors:,}")
    log(f"Time: {elapsed/60:.1f} minutes")
    log("")

    if dry_run:
        log("This was a DRY RUN. No data was imported.")

    return {
        "total_imported": total_imported,
        "total_skipped": total_skipped,
        "total_errors": total_errors,
        "elapsed_seconds": elapsed,
        "by_canton": results,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Import decisions from entscheidsuche.ch"
    )
    parser.add_argument("--canton", help="Import single canton (e.g., OW, GE)")
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint")
    parser.add_argument("--dry-run", action="store_true", help="Count only, don't import")
    parser.add_argument("--skip-federal", action="store_true", help="Skip CH (federal) decisions")
    parser.add_argument("--clear-checkpoint", action="store_true", help="Clear checkpoint and start fresh")
    args = parser.parse_args()

    if args.clear_checkpoint:
        clear_checkpoint()
        log("Checkpoint cleared.")
        return

    cantons = [args.canton.upper()] if args.canton else None

    import_all(
        cantons=cantons,
        resume=args.resume,
        dry_run=args.dry_run,
        skip_federal=args.skip_federal,
    )


if __name__ == "__main__":
    main()
