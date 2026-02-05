from __future__ import annotations

from pathlib import Path
from typing import Dict, Any

from ..util.hashing import sha256_file


def file_meta(path: Path, path_in_repo: str) -> Dict[str, Any]:
    return {
        "path": path_in_repo,
        "sha256": sha256_file(path),
        "bytes": path.stat().st_size,
    }
