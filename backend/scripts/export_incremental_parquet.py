#!/usr/bin/env python3
"""Export incremental parquet shards to HuggingFace dataset.

This script exports only new/modified decisions since the last sync,
creating date-based parquet shard files.

Usage:
    python scripts/export_incremental_parquet.py [--repo REPO_ID] [--dry-run]

Example:
    python scripts/export_incremental_parquet.py --repo voilaj/swiss-caselaw
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlmodel import select, func
from huggingface_hub import HfApi, hf_hub_download

from app.db.session import get_session
from app.models.decision import Decision

BATCH_SIZE = 10000
SYNC_METADATA_FILE = "sync_metadata.json"


def get_parquet_schema() -> pa.Schema:
    """Define the parquet schema for decisions."""
    return pa.schema([
        pa.field("id", pa.string(), nullable=False),
        pa.field("source_id", pa.string(), nullable=False),
        pa.field("source_name", pa.string(), nullable=False),
        pa.field("level", pa.string(), nullable=False),
        pa.field("canton", pa.string(), nullable=True),
        pa.field("court", pa.string(), nullable=True),
        pa.field("chamber", pa.string(), nullable=True),
        pa.field("docket", pa.string(), nullable=True),
        pa.field("decision_date", pa.string(), nullable=True),  # ISO date string
        pa.field("published_date", pa.string(), nullable=True),
        pa.field("title", pa.string(), nullable=True),
        pa.field("language", pa.string(), nullable=True),
        pa.field("url", pa.string(), nullable=False),
        pa.field("pdf_url", pa.string(), nullable=True),
        pa.field("content_text", pa.string(), nullable=False),
        pa.field("content_hash", pa.string(), nullable=True),
        pa.field("meta", pa.string(), nullable=True),  # JSON string
        pa.field("indexed_at", pa.string(), nullable=True),  # ISO datetime
        pa.field("updated_at", pa.string(), nullable=True),
    ])


def decision_to_dict(d: Decision) -> dict:
    """Convert a Decision object to a dictionary for parquet."""
    return {
        "id": d.id,
        "source_id": d.source_id,
        "source_name": d.source_name,
        "level": d.level,
        "canton": d.canton,
        "court": d.court,
        "chamber": d.chamber,
        "docket": d.docket,
        "decision_date": str(d.decision_date) if d.decision_date else None,
        "published_date": str(d.published_date) if d.published_date else None,
        "title": d.title,
        "language": d.language,
        "url": d.url,
        "pdf_url": d.pdf_url,
        "content_text": d.content_text,
        "content_hash": d.content_hash,
        "meta": json.dumps(d.meta) if d.meta else None,
        "indexed_at": d.indexed_at.isoformat() if d.indexed_at else None,
        "updated_at": d.updated_at.isoformat() if d.updated_at else None,
    }


def load_sync_metadata(repo_id: str, token: Optional[str] = None) -> dict:
    """Load sync metadata from HuggingFace repo."""
    try:
        path = hf_hub_download(
            repo_id=repo_id,
            filename=SYNC_METADATA_FILE,
            repo_type="dataset",
            token=token,
        )
        with open(path) as f:
            return json.load(f)
    except Exception as e:
        print(f"No existing sync metadata found (starting fresh): {e}")
        return {
            "last_sync_timestamp": None,
            "last_sync_date": None,
            "total_records_synced": 0,
            "shards": [],
        }


def save_sync_metadata(metadata: dict, local_path: Path) -> None:
    """Save sync metadata to local file."""
    with open(local_path, "w") as f:
        json.dump(metadata, f, indent=2)


def get_new_decisions(
    since_timestamp: Optional[str] = None,
) -> Generator[Decision, None, None]:
    """Fetch decisions added or updated since the given timestamp."""
    with get_session() as session:
        query = select(Decision)
        
        if since_timestamp:
            since_dt = datetime.fromisoformat(since_timestamp.replace("Z", "+00:00"))
            # Get decisions where indexed_at or updated_at is after since_timestamp
            query = query.where(
                (Decision.indexed_at > since_dt) | 
                (Decision.updated_at > since_dt)
            )
        
        # Order by indexed_at to ensure consistent ordering
        query = query.order_by(Decision.indexed_at)
        
        # Count total
        count_query = select(func.count(Decision.id))
        if since_timestamp:
            since_dt = datetime.fromisoformat(since_timestamp.replace("Z", "+00:00"))
            count_query = count_query.where(
                (Decision.indexed_at > since_dt) | 
                (Decision.updated_at > since_dt)
            )
        
        total = session.exec(count_query).one()
        print(f"Found {total:,} decisions to export")
        
        if total == 0:
            return
        
        offset = 0
        while True:
            print(f"  Loading batch {offset}-{offset + BATCH_SIZE}...")
            decisions = session.exec(
                query.offset(offset).limit(BATCH_SIZE)
            ).all()
            
            if not decisions:
                break
            
            for d in decisions:
                yield d
            
            offset += BATCH_SIZE
            session.expire_all()


def export_incremental_parquet(
    repo_id: str = "voilaj/swiss-caselaw",
    dry_run: bool = False,
) -> None:
    """Export incremental parquet shard and push to HuggingFace.
    
    Args:
        repo_id: HuggingFace dataset repository ID.
        dry_run: If True, don't actually upload (just test export).
    """
    token = os.environ.get("HF_TOKEN")
    if not token and not dry_run:
        # Try loading from cached token
        token_path = Path.home() / ".cache" / "huggingface" / "token"
        if token_path.exists():
            token = token_path.read_text().strip()
    
    if not token and not dry_run:
        print("Error: HF_TOKEN environment variable required (or ~/.cache/huggingface/token)")
        sys.exit(1)
    
    # Load existing sync metadata
    metadata = load_sync_metadata(repo_id, token)
    last_sync = metadata.get("last_sync_timestamp")
    
    print(f"Last sync: {last_sync or 'Never (initial sync)'}")
    
    # Create temp directory for export
    import tempfile
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        
        # Collect decisions
        records = []
        for d in get_new_decisions(last_sync):
            records.append(decision_to_dict(d))
        
        if not records:
            print("No new decisions to export.")
            return
        
        print(f"Collected {len(records):,} decisions")
        
        # Generate shard filename with current date
        now = datetime.now(timezone.utc)
        shard_date = now.strftime("%Y-%m-%d")
        shard_filename = f"data/decisions-{shard_date}.parquet"
        
        # Check if this shard already exists - if so, append timestamp
        if shard_filename in [s.get("filename") for s in metadata.get("shards", [])]:
            shard_filename = f"data/decisions-{now.strftime('%Y-%m-%d-%H%M%S')}.parquet"
        
        local_parquet_path = tmpdir / "shard.parquet"
        
        # Create parquet file
        print(f"Creating parquet shard: {shard_filename}")
        schema = get_parquet_schema()
        table = pa.Table.from_pylist(records, schema=schema)
        pq.write_table(
            table,
            local_parquet_path,
            compression="zstd",
            compression_level=9,
        )
        
        size_mb = local_parquet_path.stat().st_size / (1024 * 1024)
        print(f"Shard size: {size_mb:.2f} MB ({len(records):,} records)")
        
        # Update metadata
        new_sync_timestamp = now.isoformat()
        metadata["last_sync_timestamp"] = new_sync_timestamp
        metadata["last_sync_date"] = shard_date
        metadata["total_records_synced"] = metadata.get("total_records_synced", 0) + len(records)
        metadata.setdefault("shards", []).append({
            "filename": shard_filename,
            "date": shard_date,
            "records": len(records),
            "size_mb": round(size_mb, 2),
            "created_at": new_sync_timestamp,
        })
        
        # Save metadata locally
        local_metadata_path = tmpdir / SYNC_METADATA_FILE
        save_sync_metadata(metadata, local_metadata_path)
        
        if dry_run:
            print("\n[DRY RUN] Would upload:")
            print(f"  - {shard_filename} ({size_mb:.2f} MB)")
            print(f"  - {SYNC_METADATA_FILE}")
            print(f"\nMetadata would be updated to:")
            print(json.dumps(metadata, indent=2))
            return
        
        # Upload to HuggingFace
        print(f"\nUploading to {repo_id}...")
        api = HfApi(token=token)
        
        # Ensure repo exists
        try:
            api.create_repo(repo_id, repo_type="dataset", exist_ok=True)
        except Exception as e:
            print(f"Note: {e}")
        
        # Upload parquet shard
        api.upload_file(
            path_or_fileobj=str(local_parquet_path),
            path_in_repo=shard_filename,
            repo_id=repo_id,
            repo_type="dataset",
            commit_message=f"Add shard {shard_filename} ({len(records):,} decisions)",
        )
        
        # Upload updated metadata
        api.upload_file(
            path_or_fileobj=str(local_metadata_path),
            path_in_repo=SYNC_METADATA_FILE,
            repo_id=repo_id,
            repo_type="dataset",
            commit_message=f"Update sync metadata ({shard_date})",
        )
        
        print(f"\nDone! Uploaded {shard_filename}")
        print(f"Total records synced: {metadata['total_records_synced']:,}")
        print(f"Dataset: https://huggingface.co/datasets/{repo_id}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export incremental parquet shards to HuggingFace")
    parser.add_argument("--repo", default="voilaj/swiss-caselaw", help="HuggingFace repo ID")
    parser.add_argument("--dry-run", action="store_true", help="Test export without uploading")
    args = parser.parse_args()
    
    export_incremental_parquet(repo_id=args.repo, dry_run=args.dry_run)
