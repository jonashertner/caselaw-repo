"""Tests for pipeline utility modules."""
from __future__ import annotations

import datetime as dt

import pytest
from caselaw_pipeline.util.hashing import sha256_file
from caselaw_pipeline.util.timeutil import utc_now_iso, parse_date, iso_week


class TestSha256File:
    def test_known_hash(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("hello world\n")
        h = sha256_file(f)
        assert h == "a948904f2f0f479b8f8197694b30184b0d2ed1c1cd2a1ec0fb85d299a192a447"
        assert len(h) == 64

    def test_empty_file(self, tmp_path):
        f = tmp_path / "empty.txt"
        f.write_bytes(b"")
        h = sha256_file(f)
        # sha256 of empty input
        assert h == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


class TestUtcNowIso:
    def test_returns_iso_format(self):
        s = utc_now_iso()
        # Should parse without error
        dt.datetime.fromisoformat(s)

    def test_contains_utc_offset(self):
        s = utc_now_iso()
        assert "+00:00" in s or "Z" in s


class TestParseDate:
    def test_valid_date(self):
        d = parse_date("2024-03-15")
        assert d == dt.date(2024, 3, 15)

    def test_invalid_date_raises(self):
        with pytest.raises(ValueError):
            parse_date("not-a-date")

    def test_partial_date_raises(self):
        with pytest.raises(ValueError):
            parse_date("2024-13-01")


class TestIsoWeek:
    def test_known_date(self):
        # 2024-01-01 is Monday of ISO week 1
        result = iso_week(dt.date(2024, 1, 1))
        assert result == "2024-W01"

    def test_end_of_year(self):
        # 2024-12-31 is Tuesday of ISO week 1 of 2025
        result = iso_week(dt.date(2024, 12, 31))
        assert result == "2025-W01"

    def test_none_uses_today(self):
        result = iso_week(None)
        assert result.startswith("20")
        assert "-W" in result
