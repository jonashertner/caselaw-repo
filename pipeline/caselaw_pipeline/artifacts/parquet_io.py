from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List

import pyarrow as pa
import pyarrow.parquet as pq

from .sqlite_db import DECISION_COLS, normalize_decision

log = logging.getLogger(__name__)


def _arrow_schema() -> pa.schema:
    fields = [pa.field(c, pa.string()) for c in DECISION_COLS]
    return pa.schema(fields)


def write_delta_parquet(decisions: Iterable[Dict[str, Any]], out_path: Path, batch_rows: int = 5000) -> int:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    schema = _arrow_schema()
    writer = pq.ParquetWriter(str(out_path), schema=schema, compression="zstd", use_dictionary=True)
    total = 0
    try:
        batch: Dict[str, List[str | None]] = {c: [] for c in DECISION_COLS}
        for d in decisions:
            nd = normalize_decision(d)
            for c in DECISION_COLS:
                batch[c].append(nd.get(c))
            if len(batch["id"]) >= batch_rows:
                table = pa.Table.from_pydict(batch, schema=schema)
                writer.write_table(table)
                total += table.num_rows
                batch = {c: [] for c in DECISION_COLS}
        if batch["id"]:
            table = pa.Table.from_pydict(batch, schema=schema)
            writer.write_table(table)
            total += table.num_rows
        return total
    finally:
        writer.close()


def export_snapshot_parquet_from_sqlite(snapshot_db: Path, out_dir: Path, shard_rows: int = 50000) -> Dict[str, Any]:
    """
    Export full decisions table from snapshot sqlite into parquet shards.
    Returns metadata with shards list and row count.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    schema = _arrow_schema()

    conn = sqlite3.connect(str(snapshot_db))
    conn.row_factory = sqlite3.Row
    total = 0
    shards: List[str] = []
    try:
        cols = ",".join(DECISION_COLS)
        cur = conn.execute(f"SELECT {cols} FROM decisions ORDER BY doc_id;")
        shard_idx = 0
        while True:
            rows = cur.fetchmany(shard_rows)
            if not rows:
                break
            batch: Dict[str, List[str | None]] = {c: [] for c in DECISION_COLS}
            for r in rows:
                for c in DECISION_COLS:
                    batch[c].append(r[c])
            table = pa.Table.from_pydict(batch, schema=schema)
            shard_name = f"part-{shard_idx:05d}.parquet"
            shard_path = out_dir / shard_name
            pq.write_table(table, str(shard_path), compression="zstd", use_dictionary=True)
            shards.append(shard_name)
            total += table.num_rows
            shard_idx += 1

        return {"total_rows": total, "shards": shards}
    finally:
        conn.close()


def export_delta_parquet_from_sqlite(delta_db: Path, out_path: Path, batch_rows: int = 5000) -> int:
    """
    Export decisions table from delta sqlite into a single parquet file.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    schema = _arrow_schema()

    conn = sqlite3.connect(str(delta_db))
    conn.row_factory = sqlite3.Row
    writer = pq.ParquetWriter(str(out_path), schema=schema, compression="zstd", use_dictionary=True)
    total = 0
    try:
        cols = ",".join(DECISION_COLS)
        cur = conn.execute(f"SELECT {cols} FROM decisions ORDER BY decision_date;")
        while True:
            rows = cur.fetchmany(batch_rows)
            if not rows:
                break
            batch: Dict[str, List[str | None]] = {c: [] for c in DECISION_COLS}
            for r in rows:
                for c in DECISION_COLS:
                    batch[c].append(r[c])
            table = pa.Table.from_pydict(batch, schema=schema)
            writer.write_table(table)
            total += table.num_rows
        return total
    finally:
        writer.close()
        conn.close()
