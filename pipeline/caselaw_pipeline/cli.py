from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

from .export.reader import iter_decisions_from_export
from .artifacts.sqlite_db import (
    apply_delta_to_snapshot,
    bulk_insert_delta,
    bulk_insert_snapshot,
    create_delta_db,
    create_snapshot_db,
    rebuild_fts,
    vacuum_into,
)
from .artifacts.parquet_io import export_delta_parquet_from_sqlite, export_snapshot_parquet_from_sqlite
from .artifacts.manifest import add_delta, empty_manifest, load_manifest, save_manifest, set_snapshot
from .artifacts.meta import file_meta
from .publish.hf import download_file, download_file_if_exists, download_text, resolve_url, upload_file
from .util.loggingutil import setup_logging
from .util.timeutil import iso_week

log = logging.getLogger(__name__)


MANIFEST_PATH_IN_REPO = "artifacts/manifest.json"


def _env(name: str, default: Optional[str] = None) -> str:
    v = os.environ.get(name, default)
    if v is None:
        raise SystemExit(f"Missing required env var: {name}")
    return v


def cmd_build_delta(args: argparse.Namespace) -> None:
    export_path = Path(args.export).resolve()
    build_dir = Path(args.out).resolve()
    date = args.date

    build_dir.mkdir(parents=True, exist_ok=True)
    work = build_dir / "delta" / date
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)

    delta_sqlite = work / f"delta-{date}.sqlite"
    create_delta_db(delta_sqlite)

    log.info("Reading export: %s", export_path)
    inserted = bulk_insert_delta(delta_sqlite, iter_decisions_from_export(export_path))
    log.info("Delta sqlite rows: %s", inserted)

    # compress
    from .util.zstdutil import compress_zst

    delta_sqlite_zst = work / f"delta-{date}.sqlite.zst"
    compress_zst(delta_sqlite, delta_sqlite_zst, level=args.zstd_level)
    log.info("Compressed delta sqlite: %s", delta_sqlite_zst.name)

    # parquet (optional)
    delta_parquet = None
    if args.parquet:
        delta_parquet = work / f"delta-{date}.parquet"
        rows = export_delta_parquet_from_sqlite(delta_sqlite, delta_parquet)
        log.info("Delta parquet rows: %s", rows)

    meta = {
        "date": date,
        "delta_sqlite_zst": str(delta_sqlite_zst),
        "delta_parquet": str(delta_parquet) if delta_parquet else None,
    }
    (work / "build_meta.json").write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")
    log.info("Build output: %s", work)


def cmd_publish_delta(args: argparse.Namespace) -> None:
    build_dir = Path(args.build_dir).resolve()
    date = args.date
    hf_repo = args.hf_repo or _env("HF_DATASET_REPO")
    hf_token = args.hf_token or _env("HF_TOKEN")

    work = build_dir / "delta" / date
    delta_sqlite_zst = work / f"delta-{date}.sqlite.zst"
    delta_parquet = work / f"delta-{date}.parquet"

    if not delta_sqlite_zst.exists():
        raise SystemExit(f"Missing build artifact: {delta_sqlite_zst}")

    # Download current manifest (if present)
    manifest_tmp = work / "manifest.json"
    manifest_url = resolve_url(hf_repo, MANIFEST_PATH_IN_REPO)
    try:
        manifest_text = download_text(manifest_url)
        manifest_tmp.write_text(manifest_text, encoding="utf-8")
        manifest = load_manifest(manifest_tmp)
        log.info("Loaded manifest from HF")
    except Exception:
        manifest = empty_manifest()
        log.info("No existing manifest on HF; creating new")

    # Upload delta sqlite zst
    sqlite_path_in_repo = f"artifacts/sqlite/deltas/{date}.sqlite.zst"
    upload_file(delta_sqlite_zst, hf_repo, sqlite_path_in_repo, hf_token, commit_message=f"delta {date} (sqlite)")
    sqlite_meta = file_meta(delta_sqlite_zst, sqlite_path_in_repo)

    parquet_meta = None
    if args.parquet and delta_parquet.exists():
        parquet_path_in_repo = f"artifacts/parquet/deltas/{date}.parquet"
        upload_file(delta_parquet, hf_repo, parquet_path_in_repo, hf_token, commit_message=f"delta {date} (parquet)")
        parquet_meta = file_meta(delta_parquet, parquet_path_in_repo)

    manifest = add_delta(manifest, date=date, sqlite_zst=sqlite_meta, parquet=parquet_meta)
    save_manifest(manifest_tmp, manifest)

    upload_file(manifest_tmp, hf_repo, MANIFEST_PATH_IN_REPO, hf_token, commit_message=f"manifest: add delta {date}")
    log.info("Published delta %s and updated manifest", date)


def cmd_build_snapshot(args: argparse.Namespace) -> None:
    export_path = Path(args.export).resolve()
    build_dir = Path(args.out).resolve()
    week = args.week or iso_week()

    build_dir.mkdir(parents=True, exist_ok=True)
    work = build_dir / "snapshot" / week
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)

    snapshot_sqlite = work / f"swiss-caselaw-{week}.sqlite"
    create_snapshot_db(snapshot_sqlite)

    log.info("Inserting snapshot rows from export: %s", export_path)
    inserted = bulk_insert_snapshot(snapshot_sqlite, iter_decisions_from_export(export_path))
    log.info("Snapshot inserted rows: %s", inserted)

    log.info("Building FTS index…")
    rebuild_fts(snapshot_sqlite)

    # compact copy (optional; default true)
    compact_sqlite = snapshot_sqlite
    if args.vacuum:
        compact_sqlite = work / f"swiss-caselaw-{week}.vacuum.sqlite"
        vacuum_into(snapshot_sqlite, compact_sqlite)
        log.info("VACUUM INTO done: %s", compact_sqlite.name)

    from .util.zstdutil import compress_zst

    snapshot_sqlite_zst = work / f"swiss-caselaw-{week}.sqlite.zst"
    compress_zst(compact_sqlite, snapshot_sqlite_zst, level=args.zstd_level)
    log.info("Compressed snapshot sqlite: %s", snapshot_sqlite_zst.name)

    parquet_meta = None
    if args.parquet:
        parquet_dir = work / "parquet"
        info = export_snapshot_parquet_from_sqlite(compact_sqlite, parquet_dir, shard_rows=args.parquet_shard_rows)
        (work / "parquet_meta.json").write_text(json.dumps(info, indent=2) + "\n", encoding="utf-8")
        log.info("Snapshot parquet shards: %s", len(info["shards"]))

    meta = {
        "week": week,
        "snapshot_sqlite_zst": str(snapshot_sqlite_zst),
        "parquet_dir": str((work / "parquet") if args.parquet else None),
    }
    (work / "build_meta.json").write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")
    log.info("Build output: %s", work)


def cmd_publish_snapshot(args: argparse.Namespace) -> None:
    build_dir = Path(args.build_dir).resolve()
    week = args.week
    hf_repo = args.hf_repo or _env("HF_DATASET_REPO")
    hf_token = args.hf_token or _env("HF_TOKEN")

    work = build_dir / "snapshot" / week
    snapshot_sqlite_zst = work / f"swiss-caselaw-{week}.sqlite.zst"
    parquet_dir = work / "parquet"

    if not snapshot_sqlite_zst.exists():
        raise SystemExit(f"Missing build artifact: {snapshot_sqlite_zst}")

    # Load manifest if exists
    manifest_tmp = work / "manifest.json"
    manifest_url = resolve_url(hf_repo, MANIFEST_PATH_IN_REPO)
    try:
        manifest_text = download_text(manifest_url)
        manifest_tmp.write_text(manifest_text, encoding="utf-8")
        manifest = load_manifest(manifest_tmp)
        log.info("Loaded manifest from HF")
    except Exception:
        manifest = empty_manifest()
        log.info("No existing manifest on HF; creating new")

    snapshot_path_in_repo = f"artifacts/sqlite/snapshots/swiss-caselaw-{week}.sqlite.zst"
    upload_file(snapshot_sqlite_zst, hf_repo, snapshot_path_in_repo, hf_token, commit_message=f"snapshot {week} (sqlite)")
    sqlite_meta = file_meta(snapshot_sqlite_zst, snapshot_path_in_repo)

    parquet_meta = None
    if args.parquet and parquet_dir.exists():
        # upload shards
        shards = sorted([p for p in parquet_dir.glob("*.parquet")])
        if shards:
            # Put shards under week folder
            prefix = f"artifacts/parquet/snapshots/{week}"
            for p in shards:
                upload_file(p, hf_repo, f"{prefix}/{p.name}", hf_token, commit_message=f"snapshot {week} (parquet shard {p.name})")
            parquet_meta = {
                "path_prefix": prefix,
                "shards": [p.name for p in shards],
            }

    manifest = set_snapshot(manifest, week=week, sqlite_zst=sqlite_meta, parquet=parquet_meta, reset_deltas=True)
    save_manifest(manifest_tmp, manifest)
    upload_file(manifest_tmp, hf_repo, MANIFEST_PATH_IN_REPO, hf_token, commit_message=f"manifest: set snapshot {week} (reset deltas)")
    log.info("Published snapshot %s and updated manifest", week)


def cmd_append_to_data(args: argparse.Namespace) -> None:
    """Upload delta parquet to data/ so load_dataset() sees it immediately.

    Aligns the delta schema to match the base dataset schema (column names and order).
    """
    import pyarrow.parquet as pq
    import pyarrow as pa

    build_dir = Path(args.build_dir).resolve()
    date = args.date
    hf_repo = args.hf_repo or _env("HF_DATASET_REPO")
    hf_token = args.hf_token or _env("HF_TOKEN")

    work = build_dir / "delta" / date
    delta_parquet = work / f"delta-{date}.parquet"

    if not delta_parquet.exists():
        raise SystemExit(f"Missing build artifact: {delta_parquet}")

    # Read delta and check it has rows
    table = pq.read_table(delta_parquet)
    if table.num_rows == 0:
        log.info("Delta parquet has 0 rows, skipping append-to-data")
        return

    # Align schema to match base dataset (select only base columns, in order)
    BASE_COLS = [
        "id", "source_id", "source_name", "level", "canton", "court", "chamber",
        "docket", "decision_date", "published_date", "title", "language",
        "url", "pdf_url", "content_text",
    ]
    columns_to_select = [c for c in BASE_COLS if c in table.column_names]
    table = table.select(columns_to_select)

    # Write aligned parquet
    aligned_path = work / f"delta-{date}-aligned.parquet"
    pq.write_table(table, aligned_path, compression="zstd")

    path_in_repo = f"data/delta-{date}.parquet"
    upload_file(aligned_path, hf_repo, path_in_repo, hf_token,
                commit_message=f"daily delta {date}")
    log.info("Appended delta to data/: %s (%d rows)", path_in_repo, table.num_rows)


def cmd_consolidate_weekly(args: argparse.Namespace) -> None:
    """
    Consolidation workflow:
      - download current snapshot sqlite.zst
      - download all deltas sqlite.zst
      - apply deltas into snapshot sqlite
      - optimize + vacuum
      - publish new weekly snapshot
      - reset deltas list
    """
    hf_repo = args.hf_repo or _env("HF_DATASET_REPO")
    hf_token = args.hf_token or _env("HF_TOKEN")
    week = args.week or iso_week()

    build_dir = Path(args.build_dir).resolve()
    work = build_dir / "consolidate" / week
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True, exist_ok=True)

    manifest_url = resolve_url(hf_repo, MANIFEST_PATH_IN_REPO)
    manifest_path = work / "manifest.json"
    manifest_text = download_text(manifest_url)
    manifest_path.write_text(manifest_text, encoding="utf-8")
    manifest = load_manifest(manifest_path)
    if not manifest.get("snapshot"):
        raise SystemExit("Manifest has no snapshot; run build+publish snapshot first.")

    # Download snapshot sqlite.zst
    snap_info = manifest["snapshot"]["sqlite_zst"]
    snap_path_in_repo = snap_info["path"]
    snap_url = resolve_url(hf_repo, snap_path_in_repo)
    snap_zst = work / "base.sqlite.zst"
    download_file(snap_url, snap_zst)
    from .util.zstdutil import decompress_zst, compress_zst

    snap_sqlite = work / "base.sqlite"
    decompress_zst(snap_zst, snap_sqlite)

    # Download and apply deltas
    delta_entries = list(manifest.get("deltas") or [])
    log.info("Deltas to apply: %s", len(delta_entries))
    for d in delta_entries:
        dp = d["sqlite_zst"]["path"]
        du = resolve_url(hf_repo, dp)
        dz = work / f"delta-{d['date']}.sqlite.zst"
        ds = work / f"delta-{d['date']}.sqlite"
        download_file(du, dz)
        decompress_zst(dz, ds)
        apply_delta_to_snapshot(snap_sqlite, ds)

    # Optimize + compact snapshot
    import sqlite3

    conn = sqlite3.connect(str(snap_sqlite))
    try:
        conn.execute("INSERT INTO decisions_fts(decisions_fts) VALUES('optimize');")
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        conn.commit()
    finally:
        conn.close()

    compact_sqlite = work / f"swiss-caselaw-{week}.sqlite"
    vacuum_into(snap_sqlite, compact_sqlite)

    snapshot_sqlite_zst = work / f"swiss-caselaw-{week}.sqlite.zst"
    compress_zst(compact_sqlite, snapshot_sqlite_zst, level=args.zstd_level)

    # Build parquet snapshot shards (optional)
    parquet_meta = None
    if args.parquet:
        parquet_dir = work / "parquet"
        info = export_snapshot_parquet_from_sqlite(compact_sqlite, parquet_dir, shard_rows=args.parquet_shard_rows)
        parquet_meta = {"path_prefix": f"artifacts/parquet/snapshots/{week}", "shards": info["shards"], "total_rows": info["total_rows"]}

        # upload parquet shards
        for shard_name in info["shards"]:
            upload_file(parquet_dir / shard_name, hf_repo, f"{parquet_meta['path_prefix']}/{shard_name}", hf_token, commit_message=f"snapshot {week} (parquet shard {shard_name})")

    # Upload snapshot sqlite.zst
    snapshot_path_in_repo = f"artifacts/sqlite/snapshots/swiss-caselaw-{week}.sqlite.zst"
    upload_file(snapshot_sqlite_zst, hf_repo, snapshot_path_in_repo, hf_token, commit_message=f"snapshot {week} (sqlite)")
    sqlite_meta = file_meta(snapshot_sqlite_zst, snapshot_path_in_repo)

    manifest = set_snapshot(manifest, week=week, sqlite_zst=sqlite_meta, parquet=parquet_meta, reset_deltas=True)
    save_manifest(manifest_path, manifest)
    upload_file(manifest_path, hf_repo, MANIFEST_PATH_IN_REPO, hf_token, commit_message=f"manifest: consolidated weekly snapshot {week}")
    log.info("Weekly consolidation published: %s", week)


def _read_base_ids_from_index(hf_repo: str, work: Path) -> set[str] | None:
    """Try to read base IDs from the lightweight id-index.parquet artifact."""
    import pyarrow.parquet as pq

    index_url = resolve_url(hf_repo, "artifacts/id-index.parquet")
    index_path = work / "id-index.parquet"
    if download_file_if_exists(index_url, index_path):
        ids = set(pq.read_table(index_path, columns=["id"]).column("id").to_pylist())
        log.info("Loaded %d base IDs from id-index.parquet", len(ids))
        return ids
    return None


def _read_base_ids_remote(hf_repo: str, base_files: list[str], hf_token: str) -> set[str]:
    """Read IDs from base shards via remote column projection (HfFileSystem)."""
    import pyarrow.parquet as pq

    try:
        from huggingface_hub import HfFileSystem
        fs = HfFileSystem(token=hf_token)
        base_ids: set[str] = set()
        for fname in base_files:
            remote_path = f"datasets/{hf_repo}/{fname}"
            pf = pq.ParquetFile(fs.open(remote_path, "rb"))
            for batch in pf.iter_batches(columns=["id"], batch_size=100_000):
                base_ids.update(batch.column("id").to_pylist())
            log.info("  Remote %s: total IDs so far %d", fname.split("/")[-1], len(base_ids))
        return base_ids
    except Exception as e:
        log.warning("HfFileSystem column projection failed: %s", e)
        raise


def _upload_id_index(all_ids: set[str], work: Path, hf_repo: str, hf_token: str) -> None:
    """Upload an updated id-index.parquet to artifacts/."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    index_path = work / "id-index.parquet"
    table = pa.table({"id": pa.array(sorted(all_ids), type=pa.string())})
    pq.write_table(table, index_path, compression="zstd")
    upload_file(index_path, hf_repo, "artifacts/id-index.parquet", hf_token,
                commit_message="update id-index after consolidation")
    log.info("Uploaded id-index.parquet with %d IDs", len(all_ids))


def cmd_consolidate_data(args: argparse.Namespace) -> None:
    """Merge delta-*.parquet files in data/ into a single file; remove duplicates against base.

    Uses a lightweight id-index.parquet (~15 MB) instead of downloading all base shards
    (9.6 GB). Falls back to remote column projection via HfFileSystem if index is missing.
    """
    hf_repo = args.hf_repo or _env("HF_DATASET_REPO")
    hf_token = args.hf_token or _env("HF_TOKEN")

    build_dir = Path(args.build_dir).resolve()
    work = build_dir / "consolidate-data"
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True, exist_ok=True)

    import pyarrow.parquet as pq
    import pyarrow as pa
    from huggingface_hub import HfApi

    api = HfApi(token=hf_token)

    # List all parquet files in data/
    all_files = [
        f.rfilename for f in api.list_repo_tree(hf_repo, path_in_repo="data", repo_type="dataset")
        if f.rfilename.endswith(".parquet")
    ]

    base_files = [f for f in all_files if "delta-" not in f.split("/")[-1]]
    delta_files = [f for f in all_files if "delta-" in f.split("/")[-1]]

    log.info("Base shards: %d, Delta files: %d", len(base_files), len(delta_files))

    if not delta_files:
        log.info("No delta files to consolidate")
        return

    download_dir = work / "download"
    download_dir.mkdir()

    # Step 1: Collect IDs from base shards using lightweight index
    base_ids = _read_base_ids_from_index(hf_repo, work)
    if base_ids is None:
        log.info("No id-index found; falling back to remote column projection")
        base_ids = _read_base_ids_remote(hf_repo, base_files, hf_token)
    log.info("Total base IDs: %d", len(base_ids))

    # Step 2: Read delta files, merge and deduplicate
    delta_tables = []
    for fname in delta_files:
        local = Path(api.hf_hub_download(hf_repo, fname, repo_type="dataset", local_dir=str(download_dir)))
        dt = pq.read_table(local)
        delta_tables.append(dt)
        log.info("  Delta %s: %d rows", fname.split("/")[-1], dt.num_rows)

    merged = pa.concat_tables(delta_tables)
    log.info("Merged deltas: %d rows", merged.num_rows)

    # Deduplicate within deltas (keep last occurrence)
    ids = merged.column("id").to_pylist()
    seen: set[str] = set()
    keep_mask = []
    for i in range(len(ids) - 1, -1, -1):
        if ids[i] not in seen:
            seen.add(ids[i])
            keep_mask.append(True)
        else:
            keep_mask.append(False)
    keep_mask.reverse()
    merged = merged.filter(pa.array(keep_mask))
    log.info("After self-dedup: %d rows", merged.num_rows)

    # Remove rows already in base
    ids = merged.column("id").to_pylist()
    novel_mask = pa.array([uid not in base_ids for uid in ids])
    novel = merged.filter(novel_mask)
    log.info("Novel (not in base): %d rows", novel.num_rows)

    # Step 3: Delete old delta files from data/
    for fname in delta_files:
        api.delete_file(fname, hf_repo, repo_type="dataset",
                        commit_message=f"consolidate: remove {fname.split('/')[-1]}")
    log.info("Deleted %d delta files from data/", len(delta_files))

    # Step 4: Upload merged novel decisions as single file (if any)
    if novel.num_rows > 0:
        base_cols = [c for c in ["id", "source_id", "source_name", "level", "canton", "court",
                                  "chamber", "docket", "decision_date", "published_date", "title",
                                  "language", "url", "pdf_url", "content_text"]
                     if c in novel.column_names]
        novel = novel.select(base_cols)
        out_path = work / "delta-consolidated.parquet"
        pq.write_table(novel, out_path, compression="zstd")
        upload_file(out_path, hf_repo, "data/delta-consolidated.parquet", hf_token,
                    commit_message=f"consolidate: {novel.num_rows} novel decisions")
        log.info("Uploaded consolidated delta: %d rows", novel.num_rows)
    else:
        log.info("No novel decisions — all deltas were duplicates of base")

    # Step 5: Update ID index with novel IDs
    all_ids = base_ids | set(novel.column("id").to_pylist()) if novel.num_rows > 0 else base_ids
    _upload_id_index(all_ids, work, hf_repo, hf_token)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="caselaw-pipeline")
    p.add_argument("--log-level", default=os.environ.get("LOG_LEVEL", "INFO"))
    sub = p.add_subparsers(dest="cmd", required=True)

    # build-delta
    s = sub.add_parser("build-delta", help="Build daily delta artifacts from decisions.json.gz")
    s.add_argument("--export", required=True, help="Path to decisions.json.gz")
    s.add_argument("--out", required=True, help="Build output directory (e.g. _build)")
    s.add_argument("--date", required=True, help="YYYY-MM-DD")
    s.add_argument("--zstd-level", type=int, default=10)
    s.add_argument("--parquet", action="store_true", help="Also build parquet delta")
    s.set_defaults(fn=cmd_build_delta)

    # publish-delta
    s = sub.add_parser("publish-delta", help="Upload delta artifacts + update manifest on HF")
    s.add_argument("--build-dir", required=True)
    s.add_argument("--date", required=True)
    s.add_argument("--hf-repo", default=os.environ.get("HF_DATASET_REPO"))
    s.add_argument("--hf-token", default=os.environ.get("HF_TOKEN"))
    s.add_argument("--parquet", action="store_true")
    s.set_defaults(fn=cmd_publish_delta)

    # build-snapshot
    s = sub.add_parser("build-snapshot", help="Build weekly snapshot artifacts from decisions.json.gz")
    s.add_argument("--export", required=True)
    s.add_argument("--out", required=True)
    s.add_argument("--week", default=None, help="YYYY-Www (default: current ISO week)")
    s.add_argument("--zstd-level", type=int, default=10)
    s.add_argument("--vacuum", action="store_true", help="VACUUM INTO a compact copy (recommended)")
    s.add_argument("--parquet", action="store_true", help="Also export parquet snapshot shards")
    s.add_argument("--parquet-shard-rows", type=int, default=50000)
    s.set_defaults(fn=cmd_build_snapshot)

    # publish-snapshot
    s = sub.add_parser("publish-snapshot", help="Upload snapshot artifacts + reset manifest deltas on HF")
    s.add_argument("--build-dir", required=True)
    s.add_argument("--week", required=True)
    s.add_argument("--hf-repo", default=os.environ.get("HF_DATASET_REPO"))
    s.add_argument("--hf-token", default=os.environ.get("HF_TOKEN"))
    s.add_argument("--parquet", action="store_true")
    s.set_defaults(fn=cmd_publish_snapshot)

    # append-to-data
    s = sub.add_parser("append-to-data", help="Upload delta parquet to data/ for immediate load_dataset() visibility")
    s.add_argument("--build-dir", required=True)
    s.add_argument("--date", required=True)
    s.add_argument("--hf-repo", default=os.environ.get("HF_DATASET_REPO"))
    s.add_argument("--hf-token", default=os.environ.get("HF_TOKEN"))
    s.set_defaults(fn=cmd_append_to_data)

    # consolidate-data
    s = sub.add_parser("consolidate-data", help="Merge delta files in data/, deduplicate against base shards")
    s.add_argument("--hf-repo", default=os.environ.get("HF_DATASET_REPO"))
    s.add_argument("--hf-token", default=os.environ.get("HF_TOKEN"))
    s.add_argument("--build-dir", required=True)
    s.set_defaults(fn=cmd_consolidate_data)

    # consolidate-weekly
    s = sub.add_parser("consolidate-weekly", help="Download snapshot + deltas; publish consolidated weekly snapshot")
    s.add_argument("--hf-repo", default=os.environ.get("HF_DATASET_REPO"))
    s.add_argument("--hf-token", default=os.environ.get("HF_TOKEN"))
    s.add_argument("--week", default=None)
    s.add_argument("--build-dir", required=True)
    s.add_argument("--zstd-level", type=int, default=10)
    s.add_argument("--parquet", action="store_true")
    s.add_argument("--parquet-shard-rows", type=int, default=50000)
    s.set_defaults(fn=cmd_consolidate_weekly)

    return p


def main(argv: Optional[List[str]] = None) -> None:
    p = build_parser()
    args = p.parse_args(argv)
    setup_logging(args.log_level)
    args.fn(args)


if __name__ == "__main__":
    main()
