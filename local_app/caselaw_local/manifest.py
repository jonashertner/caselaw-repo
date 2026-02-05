from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import httpx


@dataclass(frozen=True)
class FileRef:
    path: str
    sha256: str
    bytes: int


@dataclass(frozen=True)
class Delta:
    date: str
    sqlite_zst: FileRef


@dataclass(frozen=True)
class Snapshot:
    week: str
    sqlite_zst: FileRef


@dataclass(frozen=True)
class Manifest:
    schema: str
    generated_at: str
    base_url: str
    snapshot: Optional[Snapshot]
    deltas: List[Delta]


def _hf_base_from_manifest_url(manifest_url: str) -> str:
    # Supports Hugging Face resolve URLs:
    # https://huggingface.co/datasets/<repo>/resolve/<rev>/<path>
    if "/resolve/" not in manifest_url:
        # fallback: directory base
        return manifest_url.rsplit("/", 1)[0] + "/"
    prefix, rest = manifest_url.split("/resolve/", 1)
    rev = rest.split("/", 1)[0]
    return f"{prefix}/resolve/{rev}/"


def load_manifest_from_url(manifest_url: str, timeout_s: float = 60.0) -> Manifest:
    with httpx.Client(follow_redirects=True, timeout=timeout_s) as client:
        r = client.get(manifest_url)
        r.raise_for_status()
        data = r.json()

    base = _hf_base_from_manifest_url(manifest_url)

    snap = None
    if data.get("snapshot") and data["snapshot"].get("sqlite_zst"):
        s = data["snapshot"]["sqlite_zst"]
        snap = Snapshot(
            week=data["snapshot"]["week"],
            sqlite_zst=FileRef(path=s["path"], sha256=s["sha256"], bytes=int(s["bytes"])),
        )

    deltas: List[Delta] = []
    for d in data.get("deltas") or []:
        s = d["sqlite_zst"]
        deltas.append(
            Delta(
                date=d["date"],
                sqlite_zst=FileRef(path=s["path"], sha256=s["sha256"], bytes=int(s["bytes"])),
            )
        )

    return Manifest(
        schema=data.get("schema", ""),
        generated_at=data.get("generated_at", ""),
        base_url=base,
        snapshot=snap,
        deltas=deltas,
    )


def file_url(manifest: Manifest, ref: FileRef) -> str:
    return manifest.base_url + ref.path
