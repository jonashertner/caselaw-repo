#!/usr/bin/env python3
"""Export all decisions to a compressed JSON file."""
from __future__ import annotations

import gzip
import json
import sys
from pathlib import Path

from sqlmodel import select

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.db.session import get_session
from app.models.decision import Decision


def export_decisions(output_path: str) -> None:
    """Export all decisions to gzipped JSON."""
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    with get_session() as session:
        decisions = session.exec(select(Decision)).all()

        data = {
            "version": "1.0",
            "count": len(decisions),
            "decisions": [
                {
                    "id": d.id,
                    "source_id": d.source_id,
                    "source_name": d.source_name,
                    "level": d.level,
                    "canton": d.canton,
                    "court": d.court,
                    "chamber": d.chamber,
                    "docket": d.docket,
                    "decision_date": d.decision_date.isoformat() if d.decision_date else None,
                    "published_date": d.published_date.isoformat() if d.published_date else None,
                    "title": d.title,
                    "language": d.language,
                    "url": d.url,
                    "pdf_url": d.pdf_url,
                    "content_text": d.content_text,
                    "content_hash": d.content_hash,
                    "meta": d.meta,
                }
                for d in decisions
            ],
        }

    with gzip.open(output, "wt", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=None)

    print(f"Exported {len(decisions)} decisions to {output}")
    print(f"File size: {output.stat().st_size / 1024 / 1024:.1f} MB")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <output.json.gz>")
        sys.exit(1)
    export_decisions(sys.argv[1])
