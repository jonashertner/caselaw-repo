"""Tests for local_app manifest module."""
from __future__ import annotations

from unittest.mock import patch, MagicMock

import pytest
from caselaw_local.manifest import (
    FileRef,
    Delta,
    Snapshot,
    Manifest,
    _hf_base_from_manifest_url,
    load_manifest_from_url,
    file_url,
)


# ---------------------------------------------------------------------------
# _hf_base_from_manifest_url
# ---------------------------------------------------------------------------

class TestHfBaseFromManifestUrl:
    def test_resolve_style_url(self):
        url = "https://huggingface.co/datasets/myuser/myrepo/resolve/main/data/manifest.json"
        base = _hf_base_from_manifest_url(url)
        assert base == "https://huggingface.co/datasets/myuser/myrepo/resolve/main/"

    def test_fallback_url(self):
        url = "https://example.com/data/manifest.json"
        base = _hf_base_from_manifest_url(url)
        assert base == "https://example.com/data/"

    def test_resolve_with_ref(self):
        url = "https://huggingface.co/datasets/user/repo/resolve/abc123/manifest.json"
        base = _hf_base_from_manifest_url(url)
        assert base == "https://huggingface.co/datasets/user/repo/resolve/abc123/"


# ---------------------------------------------------------------------------
# file_url
# ---------------------------------------------------------------------------

class TestFileUrl:
    def test_concatenates_base_and_path(self):
        m = Manifest(
            schema="v1",
            generated_at="2024-01-01",
            base_url="https://example.com/data/",
            snapshot=None,
            deltas=[],
        )
        ref = FileRef(path="snapshots/snap.sqlite.zst", sha256="abc", bytes=100)
        assert file_url(m, ref) == "https://example.com/data/snapshots/snap.sqlite.zst"


# ---------------------------------------------------------------------------
# Dataclass construction and immutability
# ---------------------------------------------------------------------------

class TestDataclasses:
    def test_fileref_fields(self):
        fr = FileRef(path="a.zst", sha256="abc", bytes=42)
        assert fr.path == "a.zst"
        assert fr.sha256 == "abc"
        assert fr.bytes == 42

    def test_fileref_immutable(self):
        fr = FileRef(path="a.zst", sha256="abc", bytes=42)
        with pytest.raises(AttributeError):
            fr.path = "b.zst"

    def test_delta_fields(self):
        fr = FileRef(path="d.zst", sha256="x", bytes=10)
        d = Delta(date="2024-01-01", sqlite_zst=fr)
        assert d.date == "2024-01-01"
        assert d.sqlite_zst.path == "d.zst"

    def test_snapshot_fields(self):
        fr = FileRef(path="s.zst", sha256="y", bytes=100)
        s = Snapshot(week="2024-W01", sqlite_zst=fr)
        assert s.week == "2024-W01"

    def test_manifest_fields(self):
        m = Manifest(
            schema="v1",
            generated_at="2024-01-01",
            base_url="https://example.com/",
            snapshot=None,
            deltas=[],
        )
        assert m.schema == "v1"
        assert m.snapshot is None
        assert m.deltas == []


# ---------------------------------------------------------------------------
# load_manifest_from_url (mocked network)
# ---------------------------------------------------------------------------

class TestLoadManifestFromUrl:
    def test_parses_valid_response(self):
        manifest_json = {
            "schema": "swiss-caselaw-artifacts-v1",
            "generated_at": "2024-01-01T00:00:00+00:00",
            "snapshot": {
                "week": "2024-W01",
                "sqlite_zst": {"path": "snap.sqlite.zst", "sha256": "abc", "bytes": 500},
            },
            "deltas": [
                {
                    "date": "2024-01-02",
                    "sqlite_zst": {"path": "d1.sqlite.zst", "sha256": "def", "bytes": 50},
                }
            ],
        }

        mock_response = MagicMock()
        mock_response.json.return_value = manifest_json
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.get.return_value = mock_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)

        with patch("caselaw_local.manifest.httpx.Client", return_value=mock_client):
            m = load_manifest_from_url(
                "https://huggingface.co/datasets/user/repo/resolve/main/manifest.json"
            )

        assert isinstance(m, Manifest)
        assert m.snapshot is not None
        assert m.snapshot.week == "2024-W01"
        assert len(m.deltas) == 1
        assert m.deltas[0].date == "2024-01-02"
        assert m.base_url == "https://huggingface.co/datasets/user/repo/resolve/main/"

    def test_no_snapshot(self):
        manifest_json = {
            "schema": "swiss-caselaw-artifacts-v1",
            "generated_at": "2024-01-01",
            "snapshot": None,
            "deltas": [],
        }

        mock_response = MagicMock()
        mock_response.json.return_value = manifest_json
        mock_response.raise_for_status = MagicMock()

        mock_client = MagicMock()
        mock_client.get.return_value = mock_response
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)

        with patch("caselaw_local.manifest.httpx.Client", return_value=mock_client):
            m = load_manifest_from_url("https://example.com/data/manifest.json")

        assert m.snapshot is None
        assert m.deltas == []
