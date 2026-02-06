"""Tests for the merged MCP server (FastMCP-based, 7 tools)."""
from __future__ import annotations

import json
import sqlite3
from unittest.mock import patch

import pytest
from conftest import SAMPLE_DECISIONS, _insert_decisions
from caselaw_local.db import ensure_schema

import mcp_server.server as mcp_mod


def _make_test_db() -> sqlite3.Connection:
    """Create an in-memory DB with schema + sample data for MCP tests."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    ensure_schema(conn)
    _insert_decisions(conn, SAMPLE_DECISIONS)
    return conn


@pytest.fixture(autouse=True)
def _patch_mcp_db():
    """Patch get_db to return a shared in-memory SQLite DB for all MCP tests."""
    conn = _make_test_db()
    with patch.object(mcp_mod, "_db_conn", conn), \
         patch.object(mcp_mod, "_db_type", "sqlite"), \
         patch.object(mcp_mod, "_sqlite_schema", "pipeline"):
        yield
    conn.close()


# ---------------------------------------------------------------------------
# search_caselaw
# ---------------------------------------------------------------------------

class TestSearchCaselaw:
    def test_fts_returns_results(self):
        result = json.loads(mcp_mod.search_caselaw(query="Datenschutz"))
        assert result["total"] > 0
        assert result["count"] > 0

    def test_browse_mode(self):
        result = json.loads(mcp_mod.search_caselaw(query="", language="de"))
        assert result["total"] >= 1

    def test_canton_filter(self):
        result = json.loads(mcp_mod.search_caselaw(query="", canton="ZH"))
        assert result["total"] >= 1
        for r in result["results"]:
            assert r["canton"] == "ZH"

    def test_level_filter(self):
        result = json.loads(mcp_mod.search_caselaw(query="", level="federal"))
        assert result["total"] >= 1
        for r in result["results"]:
            assert r["level"] == "federal"

    def test_limit_capped_at_500(self):
        result = json.loads(mcp_mod.search_caselaw(query="", limit=9999))
        assert result["count"] <= 500


# ---------------------------------------------------------------------------
# get_decision
# ---------------------------------------------------------------------------

class TestGetDecision:
    def test_found(self):
        result = json.loads(mcp_mod.get_decision(decision_id="bge-140-iii-264"))
        assert result["id"] == "bge-140-iii-264"
        assert "content_text" in result

    def test_not_found(self):
        result = json.loads(mcp_mod.get_decision(decision_id="nonexistent"))
        assert "error" in result


# ---------------------------------------------------------------------------
# get_caselaw_statistics
# ---------------------------------------------------------------------------

class TestGetStatistics:
    def test_returns_stats(self):
        result = json.loads(mcp_mod.get_caselaw_statistics())
        assert result["total_decisions"] == 5
        assert "date_range" in result
        assert "by_level" in result
        assert "by_language" in result
        assert "top_cantons" in result


# ---------------------------------------------------------------------------
# find_citing_decisions
# ---------------------------------------------------------------------------

class TestFindCitingDecisions:
    def test_returns_citing(self):
        result = json.loads(mcp_mod.find_citing_decisions(citation="BGE 140 III 264"))
        assert result["citation"] == "BGE 140 III 264"
        assert result["count"] >= 1

    def test_limit_respected(self):
        result = json.loads(mcp_mod.find_citing_decisions(citation="BGE 140 III 264", limit=1))
        assert result["count"] <= 1


# ---------------------------------------------------------------------------
# analyze_search_results
# ---------------------------------------------------------------------------

class TestAnalyzeSearchResults:
    def test_returns_analysis_structure(self):
        result = json.loads(mcp_mod.analyze_search_results(query="Kündigung"))
        assert "total_results" in result
        assert "analysis" in result
        assert "key_decisions" in result
        assert "by_year" in result["analysis"]
        assert "by_canton" in result["analysis"]
        assert "by_level" in result["analysis"]
        assert "by_language" in result["analysis"]
        assert "by_court" in result["analysis"]

    def test_with_filters(self):
        result = json.loads(mcp_mod.analyze_search_results(query="Kündigung", language="de"))
        assert result["total_results"] >= 1


# ---------------------------------------------------------------------------
# search_by_court
# ---------------------------------------------------------------------------

class TestSearchByCourt:
    def test_finds_bundesgericht(self):
        result = json.loads(mcp_mod.search_by_court(court="Bundesgericht"))
        assert result["count"] >= 1
        assert result["court_query"] == "Bundesgericht"

    def test_with_year_filter(self):
        result = json.loads(mcp_mod.search_by_court(court="Bundesgericht", year=2024))
        for d in result["decisions"]:
            assert d["decision_date"].startswith("2024")

    def test_limit_capped(self):
        result = json.loads(mcp_mod.search_by_court(court="Bundesgericht", limit=9999))
        assert result["count"] <= 100


# ---------------------------------------------------------------------------
# list_cantons
# ---------------------------------------------------------------------------

class TestListCantons:
    def test_returns_all_26_cantons(self):
        result = json.loads(mcp_mod.list_cantons())
        assert len(result["cantons"]) == 26

    def test_counts_are_present(self):
        result = json.loads(mcp_mod.list_cantons())
        zh = next(c for c in result["cantons"] if c["code"] == "ZH")
        assert zh["decisions"] >= 1
        assert "total_cantonal_decisions" in result
