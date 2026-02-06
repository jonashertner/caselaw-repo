"""Tests for pipeline manifest module."""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from caselaw_pipeline.artifacts.manifest import (
    MANIFEST_SCHEMA,
    empty_manifest,
    load_manifest,
    save_manifest,
    set_snapshot,
    add_delta,
)


class TestEmptyManifest:
    def test_has_schema(self):
        m = empty_manifest()
        assert m["schema"] == MANIFEST_SCHEMA

    def test_has_generated_at(self):
        m = empty_manifest()
        assert m["generated_at"] is not None

    def test_no_snapshot(self):
        m = empty_manifest()
        assert m["snapshot"] is None

    def test_empty_deltas(self):
        m = empty_manifest()
        assert m["deltas"] == []


class TestSetSnapshot:
    def test_sets_snapshot(self):
        m = empty_manifest()
        zst = {"path": "snap.sqlite.zst", "sha256": "abc", "bytes": 100}
        m2 = set_snapshot(m, week="2024-W01", sqlite_zst=zst)
        assert m2["snapshot"]["week"] == "2024-W01"
        assert m2["snapshot"]["sqlite_zst"] == zst

    def test_resets_deltas_by_default(self):
        m = empty_manifest()
        m = add_delta(m, date="2024-01-01", sqlite_zst={"path": "d.zst", "sha256": "x", "bytes": 10})
        assert len(m["deltas"]) == 1
        m2 = set_snapshot(m, week="2024-W01", sqlite_zst={"path": "s.zst", "sha256": "y", "bytes": 50})
        assert m2["deltas"] == []

    def test_preserves_deltas_when_flag_false(self):
        m = empty_manifest()
        m = add_delta(m, date="2024-01-01", sqlite_zst={"path": "d.zst", "sha256": "x", "bytes": 10})
        m2 = set_snapshot(m, week="2024-W01", sqlite_zst={"path": "s.zst", "sha256": "y", "bytes": 50}, reset_deltas=False)
        assert len(m2["deltas"]) == 1


class TestAddDelta:
    def test_adds_delta(self):
        m = empty_manifest()
        m2 = add_delta(m, date="2024-01-02", sqlite_zst={"path": "d.zst", "sha256": "a", "bytes": 10})
        assert len(m2["deltas"]) == 1
        assert m2["deltas"][0]["date"] == "2024-01-02"

    def test_dedup_by_date(self):
        m = empty_manifest()
        zst1 = {"path": "d1.zst", "sha256": "a", "bytes": 10}
        zst2 = {"path": "d2.zst", "sha256": "b", "bytes": 20}
        m = add_delta(m, date="2024-01-02", sqlite_zst=zst1)
        m = add_delta(m, date="2024-01-02", sqlite_zst=zst2)
        assert len(m["deltas"]) == 1
        assert m["deltas"][0]["sqlite_zst"]["sha256"] == "b"

    def test_sorts_ascending(self):
        m = empty_manifest()
        m = add_delta(m, date="2024-01-05", sqlite_zst={"path": "a", "sha256": "a", "bytes": 1})
        m = add_delta(m, date="2024-01-01", sqlite_zst={"path": "b", "sha256": "b", "bytes": 1})
        m = add_delta(m, date="2024-01-03", sqlite_zst={"path": "c", "sha256": "c", "bytes": 1})
        dates = [d["date"] for d in m["deltas"]]
        assert dates == ["2024-01-01", "2024-01-03", "2024-01-05"]


class TestLoadSaveManifest:
    def test_round_trip(self, tmp_path):
        path = tmp_path / "manifest.json"
        m = empty_manifest()
        m = set_snapshot(m, week="2024-W05", sqlite_zst={"path": "s.zst", "sha256": "s", "bytes": 100})
        m = add_delta(m, date="2024-02-01", sqlite_zst={"path": "d.zst", "sha256": "d", "bytes": 10})
        save_manifest(path, m)

        loaded = load_manifest(path)
        assert loaded["schema"] == MANIFEST_SCHEMA
        assert loaded["snapshot"]["week"] == "2024-W05"
        assert len(loaded["deltas"]) == 1
        assert loaded["deltas"][0]["date"] == "2024-02-01"

    def test_load_nonexistent_returns_empty(self, tmp_path):
        path = tmp_path / "does_not_exist.json"
        m = load_manifest(path)
        assert m["schema"] == MANIFEST_SCHEMA
        assert m["snapshot"] is None

    def test_save_creates_parent_dirs(self, tmp_path):
        path = tmp_path / "sub" / "dir" / "manifest.json"
        save_manifest(path, empty_manifest())
        assert path.exists()
