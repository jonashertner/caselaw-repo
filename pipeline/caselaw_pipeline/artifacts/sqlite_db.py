from __future__ import annotations

import hashlib
import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from ..util.timeutil import utc_now_iso

log = logging.getLogger(__name__)

# Canonical columns (excluding the internal doc_id used for FTS rowid)
DECISION_COLS: List[str] = [
    "id",
    "source_id",
    "source_name",
    "level",
    "canton",
    "court",
    "chamber",
    "language",
    "docket",
    "decision_date",
    "publication_date",
    "title",
    "url",
    "pdf_url",
    "content_text",
    "content_sha256",
    "fetched_at",
    "updated_at",
]


def _sha256_text(s: str) -> str:
    h = hashlib.sha256()
    h.update(s.encode("utf-8", errors="ignore"))
    return h.hexdigest()


def normalize_decision(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize incoming decision dict into our canonical set of columns.

    Missing keys become None.
    content_sha256 is computed from content_text + title + docket.
    fetched_at / updated_at default to now (UTC).
    """
    now = utc_now_iso()
    content_text = d.get("content_text") or ""
    title = d.get("title") or ""
    docket = d.get("docket") or ""
    # Hash includes fields that commonly change in updates
    content_sha256 = d.get("content_sha256") or _sha256_text(content_text + "\n" + title + "\n" + docket)

    out = {k: d.get(k) for k in DECISION_COLS}
    out["content_text"] = content_text
    out["title"] = title
    out["docket"] = docket
    out["content_sha256"] = content_sha256
    out["fetched_at"] = d.get("fetched_at") or now
    out["updated_at"] = d.get("updated_at") or now

    # Common optional renames in upstream exports
    if out.get("publication_date") is None and d.get("published_date"):
        out["publication_date"] = d.get("published_date")
    if out.get("pdf_url") is None and d.get("pdf"):
        out["pdf_url"] = d.get("pdf")
    if out.get("url") is None and d.get("permalink"):
        out["url"] = d.get("permalink")

    return out


def create_delta_db(path: Path) -> None:
    """
    Delta DB: decision rows only (id PRIMARY KEY).
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("PRAGMA journal_mode=OFF;")
        conn.execute("PRAGMA synchronous=OFF;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        cols_sql = ",\n  ".join([f"{c} TEXT" for c in DECISION_COLS if c != "content_text"])  # content_text handled separately
        # Keep content_text as TEXT as well, but put it near the end.
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS decisions (
              id TEXT PRIMARY KEY,
              source_id TEXT,
              source_name TEXT,
              level TEXT,
              canton TEXT,
              court TEXT,
              chamber TEXT,
              language TEXT,
              docket TEXT,
              decision_date TEXT,
              publication_date TEXT,
              title TEXT,
              url TEXT,
              pdf_url TEXT,
              content_text TEXT,
              content_sha256 TEXT,
              fetched_at TEXT,
              updated_at TEXT
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_delta_decision_date ON decisions(decision_date);")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS meta (
              key TEXT PRIMARY KEY,
              value TEXT
            );
            """
        )
        conn.commit()
    finally:
        conn.close()


def create_snapshot_db(path: Path) -> None:
    """
    Snapshot DB: decisions + FTS index + triggers.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    try:
        conn.execute("PRAGMA journal_mode=OFF;")
        conn.execute("PRAGMA synchronous=OFF;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        conn.execute("PRAGMA cache_size=-200000;")  # ~200MB
        conn.execute("PRAGMA page_size=4096;")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS decisions (
              doc_id INTEGER PRIMARY KEY,
              id TEXT NOT NULL UNIQUE,
              source_id TEXT,
              source_name TEXT,
              level TEXT,
              canton TEXT,
              court TEXT,
              chamber TEXT,
              language TEXT,
              docket TEXT,
              decision_date TEXT,
              publication_date TEXT,
              title TEXT,
              url TEXT,
              pdf_url TEXT,
              content_text TEXT,
              content_sha256 TEXT,
              fetched_at TEXT,
              updated_at TEXT
            );
            """
        )

        # Narrow indexes for common filters
        conn.execute("CREATE INDEX IF NOT EXISTS idx_decisions_decision_date ON decisions(decision_date);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_decisions_source_id ON decisions(source_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_decisions_canton ON decisions(canton);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_decisions_language ON decisions(language);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_decisions_docket ON decisions(docket);")

        # FTS5 (external content; stores index, not duplicate text)
        conn.execute(
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS decisions_fts USING fts5(
              title,
              docket,
              content_text,
              content='decisions',
              content_rowid='doc_id',
              tokenize='unicode61 remove_diacritics 2',
              prefix='2 3 4'
            );
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS meta (
              key TEXT PRIMARY KEY,
              value TEXT
            );
            """
        )
        conn.commit()
    finally:
        conn.close()


def ensure_snapshot_triggers(conn: sqlite3.Connection) -> None:
    """
    Ensure triggers exist (used both in pipeline snapshot creation and by local app safety checks).
    """
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND name='decisions_ai';")
    if cur.fetchone():
        return

    conn.executescript(
        """
        CREATE TRIGGER IF NOT EXISTS decisions_ai AFTER INSERT ON decisions BEGIN
          INSERT INTO decisions_fts(rowid, title, docket, content_text)
          VALUES (new.doc_id, new.title, new.docket, new.content_text);
        END;

        CREATE TRIGGER IF NOT EXISTS decisions_ad AFTER DELETE ON decisions BEGIN
          INSERT INTO decisions_fts(decisions_fts, rowid, title, docket, content_text)
          VALUES('delete', old.doc_id, old.title, old.docket, old.content_text);
        END;

        CREATE TRIGGER IF NOT EXISTS decisions_au AFTER UPDATE ON decisions BEGIN
          INSERT INTO decisions_fts(decisions_fts, rowid, title, docket, content_text)
          VALUES('delete', old.doc_id, old.title, old.docket, old.content_text);
          INSERT INTO decisions_fts(rowid, title, docket, content_text)
          VALUES (new.doc_id, new.title, new.docket, new.content_text);
        END;
        """
    )
    conn.commit()


def bulk_insert_delta(delta_db: Path, decisions: Iterable[Dict[str, Any]], batch_size: int = 2000) -> int:
    """
    Inserts normalized decisions into delta DB.
    Returns number of inserted rows.
    """
    conn = sqlite3.connect(str(delta_db))
    conn.row_factory = sqlite3.Row
    inserted = 0
    try:
        conn.execute("PRAGMA journal_mode=OFF;")
        conn.execute("PRAGMA synchronous=OFF;")
        conn.execute("PRAGMA temp_store=MEMORY;")

        cols = [c for c in DECISION_COLS]
        placeholders = ",".join(["?"] * len(cols))
        sql = f"INSERT OR REPLACE INTO decisions ({','.join(cols)}) VALUES ({placeholders})"

        buf: List[List[Optional[str]]] = []
        for d in decisions:
            nd = normalize_decision(d)
            buf.append([nd.get(c) for c in cols])
            if len(buf) >= batch_size:
                conn.executemany(sql, buf)
                inserted += len(buf)
                buf.clear()
        if buf:
            conn.executemany(sql, buf)
            inserted += len(buf)
        conn.commit()
        return inserted
    finally:
        conn.close()


def bulk_insert_snapshot(snapshot_db: Path, decisions: Iterable[Dict[str, Any]], batch_size: int = 2000) -> int:
    """
    Inserts normalized decisions into snapshot DB (decisions table only).
    FTS is built later via 'rebuild' for speed.
    Returns number of inserted rows.
    """
    conn = sqlite3.connect(str(snapshot_db))
    conn.row_factory = sqlite3.Row
    inserted = 0
    try:
        conn.execute("PRAGMA journal_mode=OFF;")
        conn.execute("PRAGMA synchronous=OFF;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        conn.execute("PRAGMA cache_size=-200000;")

        cols = [c for c in DECISION_COLS]  # doc_id is auto
        placeholders = ",".join(["?"] * len(cols))
        sql = f"INSERT OR REPLACE INTO decisions ({','.join(cols)}) VALUES ({placeholders})"

        buf: List[List[Optional[str]]] = []
        for d in decisions:
            nd = normalize_decision(d)
            buf.append([nd.get(c) for c in cols])
            if len(buf) >= batch_size:
                conn.executemany(sql, buf)
                inserted += len(buf)
                buf.clear()
        if buf:
            conn.executemany(sql, buf)
            inserted += len(buf)
        conn.commit()
        return inserted
    finally:
        conn.close()


def rebuild_fts(snapshot_db: Path) -> None:
    conn = sqlite3.connect(str(snapshot_db))
    try:
        conn.execute("INSERT INTO decisions_fts(decisions_fts) VALUES('rebuild');")
        conn.execute("INSERT INTO decisions_fts(decisions_fts) VALUES('optimize');")
        conn.commit()
        ensure_snapshot_triggers(conn)
    finally:
        conn.close()


def apply_delta_to_snapshot(snapshot_db: Path, delta_db: Path) -> int:
    """
    Apply a delta DB (id PRIMARY KEY) into a snapshot DB (id UNIQUE + doc_id).
    Returns number of rows affected (approx).
    """
    conn = sqlite3.connect(str(snapshot_db))
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        conn.execute("PRAGMA cache_size=-200000;")

        ensure_snapshot_triggers(conn)

        conn.execute("ATTACH DATABASE ? AS delta", (str(delta_db),))
        cols = [c for c in DECISION_COLS]
        col_list = ",".join(cols)
        excluded_set = ",".join([f"{c}=excluded.{c}" for c in cols if c != "id"])
        sql = f"""
        INSERT INTO decisions ({col_list})
        SELECT {col_list} FROM delta.decisions
        ON CONFLICT(id) DO UPDATE SET {excluded_set};
        """
        cur = conn.execute(sql)
        conn.execute("DETACH DATABASE delta;")
        conn.commit()
        # sqlite doesn't reliably report rowcount for upserts; return -1 to signal unknown
        return cur.rowcount if cur.rowcount is not None else -1
    finally:
        conn.close()


def vacuum_into(src_db: Path, dst_db: Path) -> None:
    dst_db.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(src_db))
    try:
        conn.execute(f"VACUUM INTO '{dst_db.as_posix()}';")
        conn.commit()
    finally:
        conn.close()
