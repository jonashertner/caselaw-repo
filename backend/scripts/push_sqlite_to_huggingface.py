#!/usr/bin/env python3
"""Push SQLite database to HuggingFace for Spaces deployment.

This exports the PostgreSQL database to SQLite, compresses it with zstd,
and uploads it to the voilaj/swiss-caselaw-db dataset.

Usage:
    python scripts/push_sqlite_to_huggingface.py [--repo REPO_ID]

Example:
    python scripts/push_sqlite_to_huggingface.py --repo voilaj/swiss-caselaw-db
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from huggingface_hub import HfApi


def push_sqlite_to_huggingface(
    sqlite_path: str | None = None,
    repo_id: str = "voilaj/swiss-caselaw-db",
) -> None:
    """Export SQLite and push to HuggingFace.

    Args:
        sqlite_path: Path to existing SQLite database. If None, exports from PostgreSQL.
        repo_id: HuggingFace dataset repository ID.
    """
    token = os.environ.get("HF_TOKEN")
    if not token:
        print("Error: HF_TOKEN environment variable required")
        sys.exit(1)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Export to SQLite if not provided
        if sqlite_path is None:
            print("Exporting PostgreSQL to SQLite...")
            sqlite_path = tmpdir / "swisslaw.db"
            from scripts.export_sqlite import export_to_sqlite
            export_to_sqlite(str(sqlite_path))
        else:
            sqlite_path = Path(sqlite_path)
            if not sqlite_path.exists():
                print(f"Error: SQLite file not found: {sqlite_path}")
                sys.exit(1)

        # Get file size
        size_mb = sqlite_path.stat().st_size / (1024 * 1024)
        print(f"SQLite database: {size_mb:.1f} MB")

        # Compress with zstd
        print("Compressing with zstd...")
        zst_path = tmpdir / "swisslaw.db.zst"
        result = subprocess.run(
            ["zstd", "-19", "-T0", str(sqlite_path), "-o", str(zst_path)],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print(f"zstd compression failed: {result.stderr}")
            sys.exit(1)

        zst_size_mb = zst_path.stat().st_size / (1024 * 1024)
        print(f"Compressed size: {zst_size_mb:.1f} MB (ratio: {size_mb/zst_size_mb:.1f}x)")

        # Upload to HuggingFace
        print(f"Uploading to {repo_id}...")
        api = HfApi(token=token)

        # Ensure repo exists
        try:
            api.create_repo(repo_id, repo_type="dataset", exist_ok=True)
        except Exception as e:
            print(f"Note: {e}")

        # Upload the compressed file
        api.upload_file(
            path_or_fileobj=str(zst_path),
            path_in_repo="swisslaw.db.zst",
            repo_id=repo_id,
            repo_type="dataset",
            commit_message=f"Update SQLite database ({size_mb:.0f} MB uncompressed)",
        )

        print(f"Done! Database available at https://huggingface.co/datasets/{repo_id}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Push SQLite database to HuggingFace")
    parser.add_argument("--sqlite", help="Path to existing SQLite database (optional)")
    parser.add_argument("--repo", default="voilaj/swiss-caselaw-db", help="HuggingFace repo ID")
    args = parser.parse_args()

    push_sqlite_to_huggingface(sqlite_path=args.sqlite, repo_id=args.repo)
