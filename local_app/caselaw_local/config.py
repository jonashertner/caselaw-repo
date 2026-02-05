from __future__ import annotations

import os
from pathlib import Path


def default_data_dir() -> Path:
    # Simple cross-platform default without extra deps.
    home = Path.home()
    if os.name == "nt":
        base = Path(os.environ.get("APPDATA", str(home)))
        return base / "swiss-caselaw"
    if sys_platform().startswith("darwin"):
        return home / "Library" / "Application Support" / "swiss-caselaw"
    # linux + others
    base = Path(os.environ.get("XDG_DATA_HOME", str(home / ".local" / "share")))
    return base / "swiss-caselaw"


def sys_platform() -> str:
    import sys
    return sys.platform


def data_dir() -> Path:
    return Path(os.environ.get("CASELAW_DATA_DIR", str(default_data_dir()))).expanduser().resolve()


# Default manifest URL pointing to the public HuggingFace dataset
DEFAULT_MANIFEST_URL = "https://huggingface.co/datasets/voilaj/swiss-caselaw-artifacts/resolve/main/manifest.json"


def manifest_url() -> str:
    return os.environ.get("CASELAW_MANIFEST_URL", DEFAULT_MANIFEST_URL)
