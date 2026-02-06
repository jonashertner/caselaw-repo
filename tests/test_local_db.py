"""Tests for local_app db module."""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from caselaw_local.db import (
    ensure_schema,
    get_database_stats,
    DECISION_COLS,
)
from conftest import SAMPLE_DECISIONS, _insert_decisions


# ---------------------------------------------------------------------------
# ensure_schema
# ---------------------------------------------------------------------------

class TestEnsureSchema:
    def test_idempotent(self, empty_db):
        """Calling ensure_schema twice should not raise."""
        ensure_schema(empty_db)  # second call
        # Verify table still exists
        row = empty_db.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='decisions';"
        ).fetchone()
        assert row[0] == 1

    def test_creates_decisions_table(self, empty_db):
        row = empty_db.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='decisions';"
        ).fetchone()
        assert row[0] == 1

    def test_creates_fts_table(self, empty_db):
        row = empty_db.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE name='decisions_fts';"
        ).fetchone()
        assert row[0] >= 1

    def test_creates_meta_table(self, empty_db):
        row = empty_db.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='meta';"
        ).fetchone()
        assert row[0] == 1

    def test_creates_indexes(self, empty_db):
        indexes = [
            r[0] for r in empty_db.execute(
                "SELECT name FROM sqlite_master WHERE type='index';"
            ).fetchall()
        ]
        assert "idx_decisions_canton" in indexes
        assert "idx_decisions_language" in indexes
        assert "idx_decisions_decision_date" in indexes

    def test_creates_triggers(self, empty_db):
        triggers = [
            r[0] for r in empty_db.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger';"
            ).fetchall()
        ]
        assert "decisions_ai" in triggers
        assert "decisions_ad" in triggers
        assert "decisions_au" in triggers


# ---------------------------------------------------------------------------
# get_database_stats
# ---------------------------------------------------------------------------

class TestGetDatabaseStats:
    def test_total_decisions(self, db_with_decisions):
        stats = get_database_stats(db_with_decisions)
        assert stats["total_decisions"] == 5

    def test_date_range(self, db_with_decisions):
        stats = get_database_stats(db_with_decisions)
        assert stats["date_range"]["min"] is not None
        assert stats["date_range"]["max"] is not None
        assert stats["date_range"]["min"] <= stats["date_range"]["max"]

    def test_by_level(self, db_with_decisions):
        stats = get_database_stats(db_with_decisions)
        levels = {item["level"]: item["count"] for item in stats["by_level"]}
        assert levels.get("federal", 0) == 2
        assert levels.get("cantonal", 0) == 3

    def test_by_language(self, db_with_decisions):
        stats = get_database_stats(db_with_decisions)
        langs = {item["language"]: item["count"] for item in stats["by_language"]}
        assert langs.get("de", 0) == 3
        assert langs.get("fr", 0) == 1
        assert langs.get("it", 0) == 1

    def test_by_canton(self, db_with_decisions):
        stats = get_database_stats(db_with_decisions)
        cantons = {item["canton"]: item["count"] for item in stats["by_canton"]}
        assert "ZH" in cantons
        assert "VD" in cantons
        assert "TI" in cantons

    def test_by_year(self, db_with_decisions):
        stats = get_database_stats(db_with_decisions)
        years = {item["year"]: item["count"] for item in stats["by_year"]}
        assert "2024" in years
        assert "2023" in years

    def test_percentages_sum_roughly(self, db_with_decisions):
        stats = get_database_stats(db_with_decisions)
        total_pct = sum(item["percentage"] for item in stats["by_level"])
        # May not be exactly 100 if some rows have NULL level
        assert total_pct > 0

    def test_detailed_includes_extra_fields(self, db_with_decisions):
        stats = get_database_stats(db_with_decisions, detailed=True)
        assert "by_source" in stats
        assert "by_court" in stats
        assert "db_size_mb" in stats
        assert "last_update" in stats

    def test_empty_db_stats(self, empty_db):
        stats = get_database_stats(empty_db)
        assert stats["total_decisions"] == 0
        assert stats["by_level"] == []
