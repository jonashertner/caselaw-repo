#!/usr/bin/env python3
"""Initial full export to parquet shards, partitioned by year.

This script exports all decisions from PostgreSQL to parquet files,
partitioned by decision year for efficient queries.

Usage:
    python scripts/export_initial_parquet.py [--output-dir DIR] [--push] [--repo REPO_ID]

Example:
    python scripts/export_initial_parquet.py --output-dir ./parquet_export --push
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import gc
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlmodel import select, func
from huggingface_hub import HfApi

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
        pa.field("decision_date", pa.string(), nullable=True),
        pa.field("published_date", pa.string(), nullable=True),
        pa.field("title", pa.string(), nullable=True),
        pa.field("language", pa.string(), nullable=True),
        pa.field("url", pa.string(), nullable=False),
        pa.field("pdf_url", pa.string(), nullable=True),
        pa.field("content_text", pa.string(), nullable=False),
        pa.field("content_hash", pa.string(), nullable=True),
        pa.field("meta", pa.string(), nullable=True),
        pa.field("indexed_at", pa.string(), nullable=True),
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


def get_decision_year(d: Decision) -> str:
    """Get year from decision_date, or 'unknown' if not available."""
    if d.decision_date:
        return str(d.decision_date.year)
    return "unknown"


def export_all_decisions_by_year(output_dir: Path) -> dict:
    """Export all decisions to parquet files partitioned by year.
    
    Returns dict with shard info for metadata.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    data_dir = output_dir / "data"
    data_dir.mkdir(exist_ok=True)
    
    # Group decisions by year
    year_records: dict[str, list[dict]] = defaultdict(list)
    
    with get_session() as session:
        total = session.exec(select(func.count(Decision.id))).one()
        print(f"Total decisions to export: {total:,}")
        
        offset = 0
        processed = 0
        
        while offset < total:
            print(f"  Processing batch {offset:,}-{offset + BATCH_SIZE:,}...")
            # IMPORTANT: ORDER BY id ensures consistent pagination
            decisions = session.exec(
                select(Decision).order_by(Decision.id).offset(offset).limit(BATCH_SIZE)
            ).all()
            
            for d in decisions:
                year = get_decision_year(d)
                year_records[year].append(decision_to_dict(d))
                processed += 1
            
            offset += BATCH_SIZE
            session.expire_all()
    
    print(f"\nProcessed {processed:,} decisions across {len(year_records)} years")
    
    # Write parquet files per year
    schema = get_parquet_schema()
    shards = []
    
    for year in sorted(year_records.keys()):
        records = year_records[year]
        record_count = len(records)
        filename = f"data/decisions-{year}.parquet"
        filepath = output_dir / filename
        
        print(f"Writing {filename} ({record_count:,} records)...")
        
        table = pa.Table.from_pylist(records, schema=schema)
        pq.write_table(
            table,
            filepath,
            compression="zstd",
            compression_level=3,  # Lower compression for faster writes
        )
        
        size_mb = filepath.stat().st_size / (1024 * 1024)
        
        shards.append({
            "filename": filename,
            "year": year,
            "records": record_count,
            "size_mb": round(size_mb, 2),
            "created_at": datetime.now(timezone.utc).isoformat(),
        })
        
        print(f"  -> {size_mb:.2f} MB", flush=True)
        
        # Clear memory after recording metadata
        del table
        del records
        year_records[year] = []  # Release memory
        gc.collect()  # Force garbage collection
    
    return {
        "shards": shards,
        "total_records": processed,
    }


def create_sync_metadata(export_info: dict, output_dir: Path) -> dict:
    """Create initial sync metadata file."""
    now = datetime.now(timezone.utc)
    
    metadata = {
        "last_sync_timestamp": now.isoformat(),
        "last_sync_date": now.strftime("%Y-%m-%d"),
        "total_records_synced": export_info["total_records"],
        "shards": export_info["shards"],
        "initial_export": True,
        "initial_export_date": now.isoformat(),
    }
    
    metadata_path = output_dir / SYNC_METADATA_FILE
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)
    
    print(f"\nCreated {SYNC_METADATA_FILE}")
    return metadata


def push_to_huggingface(
    output_dir: Path,
    repo_id: str,
    token: Optional[str] = None,
) -> None:
    """Push all parquet files and metadata to HuggingFace."""
    if not token:
        token = os.environ.get("HF_TOKEN")
    if not token:
        token_path = Path.home() / ".cache" / "huggingface" / "token"
        if token_path.exists():
            token = token_path.read_text().strip()
    
    if not token:
        print("Error: HF_TOKEN required for push")
        sys.exit(1)
    
    print(f"\nPushing to {repo_id}...")
    api = HfApi(token=token)
    
    # Ensure repo exists
    try:
        api.create_repo(repo_id, repo_type="dataset", exist_ok=True)
    except Exception as e:
        print(f"Note: {e}")
    
    # Upload all parquet files
    data_dir = output_dir / "data"
    parquet_files = list(data_dir.glob("*.parquet"))
    
    for i, filepath in enumerate(sorted(parquet_files), 1):
        print(f"  Uploading {filepath.name} ({i}/{len(parquet_files)})...")
        api.upload_file(
            path_or_fileobj=str(filepath),
            path_in_repo=f"data/{filepath.name}",
            repo_id=repo_id,
            repo_type="dataset",
            commit_message=f"Add {filepath.name}",
        )
    
    # Upload metadata
    metadata_path = output_dir / SYNC_METADATA_FILE
    if metadata_path.exists():
        print(f"  Uploading {SYNC_METADATA_FILE}...")
        api.upload_file(
            path_or_fileobj=str(metadata_path),
            path_in_repo=SYNC_METADATA_FILE,
            repo_id=repo_id,
            repo_type="dataset",
            commit_message="Add sync metadata",
        )
    
    print(f"\nDone! Dataset: https://huggingface.co/datasets/{repo_id}")


def main():
    parser = argparse.ArgumentParser(description="Initial full export to parquet shards")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./parquet_export"),
        help="Output directory for parquet files",
    )
    parser.add_argument(
        "--push",
        action="store_true",
        help="Push to HuggingFace after export",
    )
    parser.add_argument(
        "--repo",
        default="voilaj/swiss-caselaw",
        help="HuggingFace repo ID",
    )
    args = parser.parse_args()
    
    print(f"Exporting all decisions to {args.output_dir}...")
    print("=" * 60)
    
    # Export all decisions
    export_info = export_all_decisions_by_year(args.output_dir)
    
    # Create metadata
    metadata = create_sync_metadata(export_info, args.output_dir)
    
    # Summary
    print("\n" + "=" * 60)
    print("EXPORT SUMMARY")
    print("=" * 60)
    total_size = sum(s["size_mb"] for s in export_info["shards"])
    print(f"Total records: {export_info['total_records']:,}")
    print(f"Total shards: {len(export_info['shards'])}")
    print(f"Total size: {total_size:.2f} MB")
    print("\nShards by year:")
    for shard in sorted(export_info["shards"], key=lambda s: s.get("year", "")):
        print(f"  {shard['year']}: {shard['records']:,} records ({shard['size_mb']:.2f} MB)")
    
    if args.push:
        push_to_huggingface(args.output_dir, args.repo)
    else:
        print(f"\nTo push to HuggingFace, run:")
        print(f"  python {__file__} --output-dir {args.output_dir} --push --repo {args.repo}")


if __name__ == "__main__":
    main()
