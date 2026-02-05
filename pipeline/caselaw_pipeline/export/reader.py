from __future__ import annotations

import gzip
from pathlib import Path
from typing import Dict, Iterator, Any

import ijson


def iter_decisions_from_export(export_path: Path) -> Iterator[Dict[str, Any]]:
    """
    Streams decisions from a gzipped JSON export with shape:
      { "version": "...", "count": N, "decisions": [ {...}, {...} ] }

    Uses ijson so this works for very large exports.
    """
    with gzip.open(export_path, "rb") as f:
        # decisions.item streams array elements
        for item in ijson.items(f, "decisions.item"):
            if isinstance(item, dict):
                yield item
            else:
                # ignore unexpected
                continue
