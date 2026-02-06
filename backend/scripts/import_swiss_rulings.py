#!/usr/bin/env python3
"""Import decisions from rcds/swiss_rulings HuggingFace dataset.

This dataset contains 637K Swiss Federal Supreme Court decisions with full text.
Much more effective than crawling!

Usage:
    python scripts/import_swiss_rulings.py [--limit N] [--year-from YYYY] [--year-to YYYY]
"""
from __future__ import annotations

import argparse
import hashlib
import sys
from datetime import date, datetime
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from datasets import load_dataset
from sqlmodel import select

from app.db.session import get_session
from app.models.decision import Decision
from app.services.indexer import stable_uuid_url


def parse_date(timestamp_ms: int | float | None) -> date | None:
    """Convert millisecond timestamp to date."""
    if not timestamp_ms:
        return None
    try:
        return datetime.fromtimestamp(timestamp_ms / 1000).date()
    except Exception:
        return None


def compute_hash(text: str) -> str:
    """Compute content hash for deduplication."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:32]


def import_swiss_rulings(
    limit: int | None = None,
    year_from: int | None = None,
    year_to: int | None = None,
) -> None:
    """Import decisions from rcds/swiss_rulings dataset."""
    print("Loading rcds/swiss_rulings dataset (streaming)...")
    ds = load_dataset("rcds/swiss_rulings", split="train", streaming=True)

    with get_session() as session:
        # Get existing decision IDs for deduplication
        existing_ids = set(
            row[0] for row in session.exec(select(Decision.id)).all()
        )
        print(f"Existing decisions in DB: {len(existing_ids)}")

        imported = 0
        skipped = 0
        filtered = 0

        for row in ds:
            # Apply year filters
            year = row.get("year")
            if year_from and year and year < year_from:
                filtered += 1
                continue
            if year_to and year and year > year_to:
                filtered += 1
                continue

            # Generate stable ID from decision_id or URL
            decision_id = row.get("decision_id") or row.get("file_name")
            if not decision_id:
                continue

            stable_id = stable_uuid_url(f"swiss_rulings:{decision_id}")

            if stable_id in existing_ids:
                skipped += 1
                continue

            # Parse metadata
            decision_date = parse_date(row.get("date"))
            full_text = row.get("full_text", "")
            if not full_text or len(full_text) < 100:
                continue

            # Map court/canton
            court = row.get("court", "")
            canton = row.get("canton")
            if canton == "CH":
                canton = None  # Federal level
                level = "federal"
            else:
                level = "cantonal"

            # Determine source_id based on court
            if "BGer" in court:
                source_id = "bger"
                source_name = "Bundesgericht"
            elif "BVGer" in court:
                source_id = "bvger"
                source_name = "Bundesverwaltungsgericht"
            elif "BStGer" in court:
                source_id = "bstger"
                source_name = "Bundesstrafgericht"
            elif "BPatGer" in court:
                source_id = "bpatger"
                source_name = "Bundespatentgericht"
            else:
                source_id = "bger"  # Default to BGer
                source_name = court or "Bundesgericht"

            try:
                dec = Decision(
                    id=stable_id,
                    source_id=source_id,
                    source_name=source_name,
                    level=level,
                    canton=canton,
                    court=row.get("chamber") or court,
                    chamber=row.get("chamber"),
                    docket=row.get("file_number"),
                    decision_date=decision_date,
                    published_date=None,
                    title=row.get("file_name"),
                    language=row.get("language"),
                    url=row.get("html_url") or f"https://entscheidsuche.ch/{decision_id}",
                    pdf_url=row.get("pdf_url") or None,
                    content_text=full_text,
                    content_hash=compute_hash(full_text),
                    meta={
                        "source": "rcds/swiss_rulings",
                        "law_area": row.get("law_area"),
                        "law_sub_area": row.get("law_sub_area"),
                        "region": row.get("region"),
                        "year": year,
                    },
                )
                session.merge(dec)  # Use merge instead of add to handle duplicates
            except Exception:
                skipped += 1
                continue
            existing_ids.add(stable_id)
            imported += 1

            if imported % 1000 == 0:
                print(f"  Imported {imported} (skipped {skipped}, filtered {filtered})...")
                try:
                    session.commit()
                except Exception as e:
                    print(f"  Commit error (continuing): {e}")
                    session.rollback()
                    # Reload existing IDs after rollback
                    existing_ids = set(
                        row[0] for row in session.exec(select(Decision.id)).all()
                    )

            if limit and imported >= limit:
                break

        session.commit()
        print(f"\nImported {imported} decisions")
        print(f"Skipped {skipped} existing")
        print(f"Filtered {filtered} by year")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import from rcds/swiss_rulings")
    parser.add_argument("--limit", type=int, help="Max decisions to import")
    parser.add_argument("--year-from", type=int, help="Filter: year >= YYYY")
    parser.add_argument("--year-to", type=int, help="Filter: year <= YYYY")
    args = parser.parse_args()

    import_swiss_rulings(
        limit=args.limit,
        year_from=args.year_from,
        year_to=args.year_to,
    )
