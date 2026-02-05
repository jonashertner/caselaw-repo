from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..util.timeutil import utc_now_iso


MANIFEST_SCHEMA = "swiss-caselaw-artifacts-v1"


def empty_manifest() -> Dict[str, Any]:
    return {
        "schema": MANIFEST_SCHEMA,
        "generated_at": utc_now_iso(),
        "snapshot": None,
        "deltas": [],
    }


def load_manifest(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return empty_manifest()
    return json.loads(path.read_text(encoding="utf-8"))


def save_manifest(path: Path, manifest: Dict[str, Any]) -> None:
    manifest = dict(manifest)
    manifest["schema"] = MANIFEST_SCHEMA
    manifest["generated_at"] = utc_now_iso()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def set_snapshot(manifest: Dict[str, Any], *, week: str, sqlite_zst: Dict[str, Any], parquet: Optional[Dict[str, Any]] = None, reset_deltas: bool = True) -> Dict[str, Any]:
    m = dict(manifest)
    m["snapshot"] = {
        "week": week,
        "sqlite_zst": sqlite_zst,
        "parquet": parquet,
    }
    if reset_deltas:
        m["deltas"] = []
    return m


def add_delta(manifest: Dict[str, Any], *, date: str, sqlite_zst: Dict[str, Any], parquet: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    m = dict(manifest)
    deltas: List[Dict[str, Any]] = list(m.get("deltas") or [])
    # de-dup by date
    deltas = [d for d in deltas if d.get("date") != date]
    deltas.append({"date": date, "sqlite_zst": sqlite_zst, "parquet": parquet})
    # sort ascending
    deltas.sort(key=lambda d: d.get("date") or "")
    m["deltas"] = deltas
    return m
