from __future__ import annotations

import hashlib
from pathlib import Path

import httpx


def download_to_path(url: str, dst: Path, expected_sha256: str | None = None, timeout_s: float = 300.0) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(dst.suffix + ".part")

    h = hashlib.sha256()
    with httpx.stream("GET", url, follow_redirects=True, timeout=timeout_s) as r:
        r.raise_for_status()
        with tmp.open("wb") as f:
            for chunk in r.iter_bytes():
                if not chunk:
                    continue
                f.write(chunk)
                h.update(chunk)

    if expected_sha256:
        got = h.hexdigest()
        if got.lower() != expected_sha256.lower():
            tmp.unlink(missing_ok=True)
            raise RuntimeError(f"SHA256 mismatch for {url}: expected {expected_sha256}, got {got}")

    tmp.replace(dst)
