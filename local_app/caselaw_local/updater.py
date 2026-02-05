from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from .config import data_dir, manifest_url
from .download import download_to_path
from .manifest import Manifest, file_url, load_manifest_from_url
from .zstdutil import decompress_zst
from .db import apply_delta, ensure_schema, connect, apply_pragmas


STATE_FILE = "state.json"
DB_FILE = "caselaw.sqlite"
DOWNLOADS_DIR = "downloads"


def _load_state(dd: Path) -> Dict[str, Any]:
    p = dd / STATE_FILE
    if not p.exists():
        return {"snapshot_week": None, "applied_deltas": []}
    return json.loads(p.read_text(encoding="utf-8"))


def _save_state(dd: Path, state: Dict[str, Any]) -> None:
    p = dd / STATE_FILE
    p.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def local_db_path(dd: Optional[Path] = None) -> Path:
    dd = dd or data_dir()
    return dd / DB_FILE


def update() -> Dict[str, Any]:
    dd = data_dir()
    dd.mkdir(parents=True, exist_ok=True)
    downloads = dd / DOWNLOADS_DIR
    downloads.mkdir(parents=True, exist_ok=True)

    st = _load_state(dd)
    m = load_manifest_from_url(manifest_url())

    if not m.snapshot:
        raise RuntimeError("Remote manifest has no snapshot. Maintainer must publish an initial weekly snapshot.")

    db_path = local_db_path(dd)

    # Snapshot install/replace
    if (not db_path.exists()) or (st.get("snapshot_week") != m.snapshot.week):
        snap_ref = m.snapshot.sqlite_zst
        snap_url = file_url(m, snap_ref)

        zst_path = downloads / f"snapshot-{m.snapshot.week}.sqlite.zst"
        download_to_path(snap_url, zst_path, expected_sha256=snap_ref.sha256)

        tmp_sqlite = downloads / f"snapshot-{m.snapshot.week}.sqlite"
        decompress_zst(zst_path, tmp_sqlite)

        # Ensure schema/triggers exist (safety)
        conn = connect(tmp_sqlite, read_only=False)
        try:
            apply_pragmas(conn, read_only=False)
            ensure_schema(conn)
        finally:
            conn.close()

        # Atomic replace
        tmp_final = db_path.with_suffix(".sqlite.new")
        if tmp_final.exists():
            tmp_final.unlink()
        tmp_sqlite.replace(tmp_final)
        tmp_final.replace(db_path)

        st["snapshot_week"] = m.snapshot.week
        st["applied_deltas"] = []

    # Apply deltas
    applied = set(st.get("applied_deltas") or [])
    newly_applied: List[str] = []
    for d in m.deltas:
        if d.date in applied:
            continue
        ref = d.sqlite_zst
        url = file_url(m, ref)
        zst_path = downloads / f"delta-{d.date}.sqlite.zst"
        download_to_path(url, zst_path, expected_sha256=ref.sha256)

        delta_sqlite = downloads / f"delta-{d.date}.sqlite"
        decompress_zst(zst_path, delta_sqlite)

        apply_delta(db_path, delta_sqlite)
        newly_applied.append(d.date)
        applied.add(d.date)

        if os.environ.get("CASELAW_KEEP_DOWNLOADS", "0") != "1":
            delta_sqlite.unlink(missing_ok=True)
            zst_path.unlink(missing_ok=True)

    st["applied_deltas"] = sorted(applied)
    st["remote_generated_at"] = m.generated_at
    _save_state(dd, st)

    return {
        "snapshot_week": st["snapshot_week"],
        "applied_deltas": newly_applied,
        "total_applied_deltas": len(st["applied_deltas"]),
    }
