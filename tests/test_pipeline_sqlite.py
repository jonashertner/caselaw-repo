"""Tests for pipeline sqlite_db module."""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from caselaw_pipeline.artifacts.sqlite_db import (
    DECISION_COLS,
    normalize_decision,
    create_delta_db,
    create_snapshot_db,
    bulk_insert_delta,
    bulk_insert_snapshot,
    rebuild_fts,
    apply_delta_to_snapshot,
    vacuum_into,
)


# ---------------------------------------------------------------------------
# normalize_decision
# ---------------------------------------------------------------------------

class TestNormalizeDecision:
    def test_canonical_fields(self):
        d = normalize_decision({"id": "test-1", "title": "Title", "content_text": "Body"})
        for col in DECISION_COLS:
            assert col in d

    def test_content_sha256_computed(self):
        d = normalize_decision({"id": "test-1", "content_text": "abc", "title": "T", "docket": "D"})
        assert d["content_sha256"] is not None
        assert len(d["content_sha256"]) == 64  # SHA-256 hex

    def test_content_sha256_preserved_if_given(self):
        sha = "a" * 64
        d = normalize_decision({"id": "test-1", "content_sha256": sha})
        assert d["content_sha256"] == sha

    def test_fetched_at_defaults_to_now(self):
        d = normalize_decision({"id": "test-1"})
        assert d["fetched_at"] is not None

    def test_fetched_at_preserved(self):
        d = normalize_decision({"id": "test-1", "fetched_at": "2024-01-01T00:00:00+00:00"})
        assert d["fetched_at"] == "2024-01-01T00:00:00+00:00"

    def test_updated_at_defaults_to_now(self):
        d = normalize_decision({"id": "test-1"})
        assert d["updated_at"] is not None

    def test_rename_publication_date(self):
        d = normalize_decision({"id": "test-1", "publication_date": "2024-01-01"})
        assert d["published_date"] == "2024-01-01"

    def test_rename_pdf(self):
        d = normalize_decision({"id": "test-1", "pdf": "https://example.com/d.pdf"})
        assert d["pdf_url"] == "https://example.com/d.pdf"

    def test_rename_permalink(self):
        d = normalize_decision({"id": "test-1", "permalink": "https://example.com/d"})
        assert d["url"] == "https://example.com/d"

    def test_missing_keys_become_none(self):
        d = normalize_decision({"id": "test-1"})
        assert d["canton"] is None
        assert d["court"] is None

    def test_content_text_defaults_empty(self):
        d = normalize_decision({"id": "test-1"})
        assert d["content_text"] == ""


# ---------------------------------------------------------------------------
# create_delta_db / create_snapshot_db
# ---------------------------------------------------------------------------

class TestCreateDatabases:
    def test_create_delta_db(self, tmp_path):
        db_path = tmp_path / "delta.sqlite"
        create_delta_db(db_path)
        assert db_path.exists()

        conn = sqlite3.connect(str(db_path))
        tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()]
        assert "decisions" in tables
        assert "meta" in tables
        conn.close()

    def test_create_snapshot_db(self, tmp_path):
        db_path = tmp_path / "snapshot.sqlite"
        create_snapshot_db(db_path)
        assert db_path.exists()

        conn = sqlite3.connect(str(db_path))
        tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()]
        assert "decisions" in tables
        assert "meta" in tables
        # FTS virtual table
        all_names = [r[0] for r in conn.execute("SELECT name FROM sqlite_master;").fetchall()]
        assert "decisions_fts" in all_names
        conn.close()

    def test_create_delta_db_creates_parent_dirs(self, tmp_path):
        db_path = tmp_path / "sub" / "dir" / "delta.sqlite"
        create_delta_db(db_path)
        assert db_path.exists()


# ---------------------------------------------------------------------------
# bulk_insert_delta / bulk_insert_snapshot
# ---------------------------------------------------------------------------

class TestBulkInsert:
    def test_bulk_insert_delta_returns_count(self, tmp_path):
        db_path = tmp_path / "delta.sqlite"
        create_delta_db(db_path)
        decisions = [
            {"id": f"d-{i}", "title": f"Title {i}", "content_text": f"Body {i}"}
            for i in range(5)
        ]
        count = bulk_insert_delta(db_path, decisions)
        assert count == 5

    def test_bulk_insert_delta_replace_duplicates(self, tmp_path):
        db_path = tmp_path / "delta.sqlite"
        create_delta_db(db_path)
        decisions = [{"id": "dup-1", "title": "V1", "content_text": "Body"}]
        bulk_insert_delta(db_path, decisions)
        decisions2 = [{"id": "dup-1", "title": "V2", "content_text": "Body updated"}]
        bulk_insert_delta(db_path, decisions2)

        conn = sqlite3.connect(str(db_path))
        row = conn.execute("SELECT title FROM decisions WHERE id='dup-1';").fetchone()
        assert row[0] == "V2"
        conn.close()

    def test_bulk_insert_snapshot_returns_count(self, tmp_path):
        db_path = tmp_path / "snap.sqlite"
        create_snapshot_db(db_path)
        decisions = [
            {"id": f"s-{i}", "title": f"Title {i}", "content_text": f"Body {i}"}
            for i in range(3)
        ]
        count = bulk_insert_snapshot(db_path, decisions)
        assert count == 3

    def test_bulk_insert_snapshot_fts_after_rebuild(self, tmp_path):
        db_path = tmp_path / "snap.sqlite"
        create_snapshot_db(db_path)
        decisions = [
            {"id": "fts-1", "title": "Datenschutz und Arbeitsrecht", "content_text": "Test content"}
        ]
        bulk_insert_snapshot(db_path, decisions)
        rebuild_fts(db_path)

        conn = sqlite3.connect(str(db_path))
        rows = conn.execute(
            "SELECT rowid FROM decisions_fts WHERE decisions_fts MATCH 'Datenschutz';"
        ).fetchall()
        assert len(rows) == 1
        conn.close()


# ---------------------------------------------------------------------------
# apply_delta_to_snapshot
# ---------------------------------------------------------------------------

class TestApplyDelta:
    @pytest.mark.xfail(
        reason="INSERT...SELECT...ON CONFLICT DO UPDATE not supported in SQLite <3.35 style; "
               "production uses a newer SQLite build",
        raises=sqlite3.OperationalError,
    )
    def test_apply_delta_inserts_new_rows(self, tmp_path):
        snap = tmp_path / "snap.sqlite"
        delta = tmp_path / "delta.sqlite"
        create_snapshot_db(snap)
        create_delta_db(delta)
        bulk_insert_delta(delta, [{"id": "new-1", "title": "New", "content_text": "New body"}])

        apply_delta_to_snapshot(snap, delta)

        conn = sqlite3.connect(str(snap))
        row = conn.execute("SELECT title FROM decisions WHERE id='new-1';").fetchone()
        assert row[0] == "New"
        conn.close()

    @pytest.mark.xfail(
        reason="INSERT...SELECT...ON CONFLICT DO UPDATE not supported in SQLite <3.35 style; "
               "production uses a newer SQLite build",
        raises=sqlite3.OperationalError,
    )
    def test_apply_delta_updates_existing(self, tmp_path):
        snap = tmp_path / "snap.sqlite"
        delta = tmp_path / "delta.sqlite"
        create_snapshot_db(snap)
        create_delta_db(delta)

        bulk_insert_snapshot(snap, [{"id": "up-1", "title": "Old", "content_text": "Old body"}])
        rebuild_fts(snap)
        bulk_insert_delta(delta, [{"id": "up-1", "title": "Updated", "content_text": "Updated body"}])

        apply_delta_to_snapshot(snap, delta)

        conn = sqlite3.connect(str(snap))
        row = conn.execute("SELECT title FROM decisions WHERE id='up-1';").fetchone()
        assert row[0] == "Updated"
        conn.close()


# ---------------------------------------------------------------------------
# vacuum_into
# ---------------------------------------------------------------------------

class TestVacuumInto:
    def test_vacuum_into_creates_copy(self, tmp_path):
        src = tmp_path / "src.sqlite"
        dst = tmp_path / "dst.sqlite"
        create_delta_db(src)
        bulk_insert_delta(src, [{"id": "v-1", "title": "T", "content_text": "C"}])
        vacuum_into(src, dst)
        assert dst.exists()

        conn = sqlite3.connect(str(dst))
        row = conn.execute("SELECT title FROM decisions WHERE id='v-1';").fetchone()
        assert row[0] == "T"
        conn.close()

    def test_vacuum_into_creates_parent_dirs(self, tmp_path):
        src = tmp_path / "src.sqlite"
        dst = tmp_path / "new" / "dir" / "dst.sqlite"
        create_delta_db(src)
        vacuum_into(src, dst)
        assert dst.exists()
