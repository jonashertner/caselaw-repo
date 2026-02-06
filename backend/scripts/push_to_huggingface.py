#!/usr/bin/env python3
"""Push decisions to Hugging Face dataset."""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Generator

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from datasets import Dataset
from huggingface_hub import HfApi
from sqlmodel import select, func

from app.db.session import get_session
from app.models.decision import Decision

BATCH_SIZE = 10000


def generate_records() -> Generator[dict, None, None]:
    """Generate records in batches to avoid memory issues."""
    with get_session() as session:
        # Get total count
        total = session.exec(select(func.count(Decision.id))).one()
        print(f"Total decisions: {total}")

        offset = 0
        while offset < total:
            print(f"  Loading batch {offset}-{offset + BATCH_SIZE}...")
            # IMPORTANT: ORDER BY id ensures consistent pagination
            decisions = session.exec(
                select(Decision).order_by(Decision.id).offset(offset).limit(BATCH_SIZE)
            ).all()

            for d in decisions:
                yield {
                    "id": d.id,
                    "source_id": d.source_id,
                    "source_name": d.source_name,
                    "level": d.level,
                    "canton": d.canton or "",
                    "court": d.court or "",
                    "chamber": d.chamber or "",
                    "docket": d.docket or "",
                    "decision_date": d.decision_date.isoformat() if d.decision_date else "",
                    "published_date": d.published_date.isoformat() if d.published_date else "",
                    "title": d.title or "",
                    "language": d.language or "",
                    "url": d.url,
                    "pdf_url": d.pdf_url or "",
                    "content_text": d.content_text or "",
                }

            offset += BATCH_SIZE
            # Clear session to free memory
            session.expire_all()


def push_to_huggingface(repo_id: str) -> None:
    """Export all decisions and push to Hugging Face."""
    token = os.environ.get("HF_TOKEN")
    if not token:
        print("Error: HF_TOKEN environment variable required")
        sys.exit(1)

    # Get total count first
    with get_session() as session:
        total = session.exec(select(func.count(Decision.id))).one()
    print(f"Total decisions: {total}")

    # Create dataset from generator (streaming, memory efficient)
    print("Creating dataset from generator...")
    dataset = Dataset.from_generator(generate_records)

    # Push to hub with sharding for large datasets
    print(f"Pushing to {repo_id}...")
    dataset.push_to_hub(
        repo_id,
        token=token,
        commit_message=f"Update: {total} decisions",
        max_shard_size="500MB",
    )

    print(f"Done! Dataset available at https://huggingface.co/datasets/{repo_id}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <huggingface-repo-id>")
        print(f"Example: {sys.argv[0]} jonashertner/swiss-caselaw")
        sys.exit(1)
    push_to_huggingface(sys.argv[1])
