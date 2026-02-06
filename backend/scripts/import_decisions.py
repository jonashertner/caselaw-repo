#!/usr/bin/env python3
"""Import decisions from a compressed JSON file."""
from __future__ import annotations

import datetime as dt
import gzip
import json
import sys
from pathlib import Path

from sqlmodel import select

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.db.session import get_session
from app.models.decision import Decision
from app.services.indexer import Indexer


def parse_date(value: str | None) -> dt.date | None:
    if not value:
        return None
    return dt.date.fromisoformat(value)


def import_decisions(input_path: str, skip_embeddings: bool = False) -> None:
    """Import decisions from gzipped JSON."""
    input_file = Path(input_path)
    if not input_file.exists():
        print(f"File not found: {input_file}")
        sys.exit(1)

    print(f"Loading {input_file}...")
    with gzip.open(input_file, "rt", encoding="utf-8") as f:
        data = json.load(f)

    decisions_data = data.get("decisions", [])
    print(f"Found {len(decisions_data)} decisions")

    indexer = Indexer() if not skip_embeddings else None
    imported = 0
    skipped = 0

    with get_session() as session:
        for i, d in enumerate(decisions_data):
            # Check if already exists
            existing = session.exec(
                select(Decision).where(Decision.id == d["id"])
            ).first()

            if existing:
                skipped += 1
                continue

            decision = Decision(
                id=d["id"],
                source_id=d["source_id"],
                source_name=d["source_name"],
                level=d["level"],
                canton=d.get("canton"),
                court=d.get("court"),
                chamber=d.get("chamber"),
                docket=d.get("docket"),
                decision_date=parse_date(d.get("decision_date")),
                published_date=parse_date(d.get("published_date")),
                title=d.get("title"),
                language=d.get("language"),
                url=d["url"],
                pdf_url=d.get("pdf_url"),
                content_text=d["content_text"],
                content_hash=d["content_hash"],
                meta=d.get("meta", {}),
            )

            session.add(decision)
            session.commit()

            # Generate embeddings for search
            if indexer and decision.content_text:
                try:
                    indexer._index_chunks(session, decision.id, decision.content_text)
                except Exception as e:
                    print(f"  Warning: Failed to index chunks for {decision.id}: {e}")

            imported += 1

            if (i + 1) % 100 == 0:
                print(f"  Processed {i + 1}/{len(decisions_data)} (imported: {imported}, skipped: {skipped})")

    print(f"Done! Imported: {imported}, Skipped (existing): {skipped}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <input.json.gz> [--skip-embeddings]")
        sys.exit(1)

    skip_emb = "--skip-embeddings" in sys.argv
    import_decisions(sys.argv[1], skip_embeddings=skip_emb)
