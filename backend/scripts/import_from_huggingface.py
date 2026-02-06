#!/usr/bin/env python3
"""Import decisions from Hugging Face dataset."""
from __future__ import annotations

import hashlib
import sys
from datetime import date
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from datasets import load_dataset
from sqlmodel import select

from app.db.session import get_session
from app.models.decision import Decision
from scripts.scraper_common import upsert_decision


def import_from_huggingface(repo_id: str, streaming: bool = True) -> None:
    """Import decisions from Hugging Face dataset.

    Uses streaming mode by default for memory efficiency with large datasets.
    Uses upsert logic to handle duplicate URLs gracefully.
    """
    print(f"Loading dataset from {repo_id} (streaming={streaming})...")

    if streaming:
        dataset = load_dataset(repo_id, split="train", streaming=True)
        print("Streaming mode - processing records...")
    else:
        dataset = load_dataset(repo_id, split="train")
        print(f"Found {len(dataset)} decisions")

    with get_session() as session:
        imported = 0
        skipped = 0

        for row in dataset:
            # Parse dates
            decision_date = None
            if row.get("decision_date"):
                try:
                    decision_date = date.fromisoformat(row["decision_date"])
                except ValueError:
                    pass

            published_date = None
            if row.get("published_date"):
                try:
                    published_date = date.fromisoformat(row["published_date"])
                except ValueError:
                    pass

            # Get content text and generate hash
            content_text = row.get("content_text") or ""
            content_hash = row.get("content_hash")
            if not content_hash and content_text:
                content_hash = hashlib.sha256(content_text.encode("utf-8")).hexdigest()
            elif not content_hash:
                content_hash = hashlib.sha256(row["id"].encode("utf-8")).hexdigest()

            dec = Decision(
                id=row["id"],
                source_id=row["source_id"],
                source_name=row["source_name"],
                level=row["level"],
                canton=row.get("canton") or None,
                court=row.get("court") or None,
                chamber=row.get("chamber") or None,
                docket=row.get("docket") or None,
                decision_date=decision_date,
                published_date=published_date,
                title=row.get("title") or None,
                language=row.get("language") or None,
                url=row["url"],
                pdf_url=row.get("pdf_url") or None,
                content_text=content_text,
                content_hash=content_hash,
                meta={},
            )

            try:
                inserted, updated = upsert_decision(session, dec)
                if inserted or updated:
                    imported += 1
                else:
                    skipped += 1
            except Exception as e:
                print(f"  Error upserting {row['id']}: {e}")
                skipped += 1
                continue

            if imported % 100 == 0:
                print(f"  Imported {imported} (skipped {skipped})...")
                session.commit()

        session.commit()
        print(f"Imported {imported} new decisions, skipped {skipped} existing")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <huggingface-repo-id>")
        print(f"Example: {sys.argv[0]} voilaj/swiss-caselaw")
        sys.exit(1)

    repo_id = sys.argv[1]
    import_from_huggingface(repo_id)
