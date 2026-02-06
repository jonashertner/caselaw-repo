"""Tests for MCP server request handling."""
from __future__ import annotations

import json
import sqlite3
from unittest.mock import patch

import pytest
from conftest import SAMPLE_DECISIONS, _insert_decisions
from caselaw_local.db import ensure_schema

# We import the server module and patch get_db_connection to use our test DB.
import mcp_server.server as mcp


def _make_test_db() -> sqlite3.Connection:
    """Create an in-memory DB with schema + sample data for MCP tests."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    ensure_schema(conn)
    _insert_decisions(conn, SAMPLE_DECISIONS)
    return conn


@pytest.fixture(autouse=True)
def _patch_mcp_db():
    """Patch get_db_connection to return a shared in-memory DB for all MCP tests."""
    conn = _make_test_db()
    with patch.object(mcp, "get_db_connection", return_value=conn):
        yield
    conn.close()


# ---------------------------------------------------------------------------
# initialize
# ---------------------------------------------------------------------------

class TestInitialize:
    def test_returns_protocol_version(self):
        resp = mcp.handle_request({"method": "initialize", "id": 1})
        assert resp["result"]["protocolVersion"] == "2024-11-05"

    def test_returns_capabilities(self):
        resp = mcp.handle_request({"method": "initialize", "id": 1})
        assert "tools" in resp["result"]["capabilities"]

    def test_returns_server_info(self):
        resp = mcp.handle_request({"method": "initialize", "id": 1})
        assert resp["result"]["serverInfo"]["name"] == "swiss-caselaw"


# ---------------------------------------------------------------------------
# tools/list
# ---------------------------------------------------------------------------

class TestToolsList:
    def test_returns_all_tools(self):
        resp = mcp.handle_request({"method": "tools/list", "id": 2})
        tools = resp["result"]["tools"]
        names = {t["name"] for t in tools}
        assert names == {
            "search_caselaw",
            "get_decision",
            "get_caselaw_statistics",
            "find_citing_decisions",
            "analyze_search_results",
        }

    def test_returns_five_tools(self):
        resp = mcp.handle_request({"method": "tools/list", "id": 2})
        assert len(resp["result"]["tools"]) == 5


# ---------------------------------------------------------------------------
# tools/call – search_caselaw
# ---------------------------------------------------------------------------

class TestSearchCaselaw:
    def test_returns_results(self):
        resp = mcp.handle_request({
            "method": "tools/call",
            "id": 3,
            "params": {"name": "search_caselaw", "arguments": {"query": "Datenschutz"}},
        })
        content = json.loads(resp["result"]["content"][0]["text"])
        assert content["total"] > 0

    def test_browse_mode(self):
        resp = mcp.handle_request({
            "method": "tools/call",
            "id": 4,
            "params": {"name": "search_caselaw", "arguments": {"query": "", "language": "de"}},
        })
        content = json.loads(resp["result"]["content"][0]["text"])
        assert content["total"] >= 1

    def test_with_canton_filter(self):
        resp = mcp.handle_request({
            "method": "tools/call",
            "id": 5,
            "params": {"name": "search_caselaw", "arguments": {"query": "", "canton": "ZH"}},
        })
        content = json.loads(resp["result"]["content"][0]["text"])
        assert content["total"] >= 1


# ---------------------------------------------------------------------------
# tools/call – get_decision
# ---------------------------------------------------------------------------

class TestGetDecision:
    def test_found(self):
        resp = mcp.handle_request({
            "method": "tools/call",
            "id": 6,
            "params": {"name": "get_decision", "arguments": {"decision_id": "bge-140-iii-264"}},
        })
        content = json.loads(resp["result"]["content"][0]["text"])
        assert content["id"] == "bge-140-iii-264"

    def test_not_found(self):
        resp = mcp.handle_request({
            "method": "tools/call",
            "id": 7,
            "params": {"name": "get_decision", "arguments": {"decision_id": "nonexistent"}},
        })
        content = json.loads(resp["result"]["content"][0]["text"])
        assert "error" in content


# ---------------------------------------------------------------------------
# tools/call – get_caselaw_statistics
# ---------------------------------------------------------------------------

class TestGetStatistics:
    def test_returns_stats(self):
        resp = mcp.handle_request({
            "method": "tools/call",
            "id": 8,
            "params": {"name": "get_caselaw_statistics", "arguments": {}},
        })
        content = json.loads(resp["result"]["content"][0]["text"])
        assert content["total_decisions"] == 5
        assert "date_range" in content


# ---------------------------------------------------------------------------
# tools/call – find_citing_decisions
# ---------------------------------------------------------------------------

class TestFindCitingDecisions:
    def test_returns_citing(self):
        resp = mcp.handle_request({
            "method": "tools/call",
            "id": 9,
            "params": {"name": "find_citing_decisions", "arguments": {"citation": "BGE 140 III 264"}},
        })
        content = json.loads(resp["result"]["content"][0]["text"])
        assert content["citation"] == "BGE 140 III 264"
        # Our sample data includes this citation in content
        assert content["count"] >= 1


# ---------------------------------------------------------------------------
# tools/call – analyze_search_results
# ---------------------------------------------------------------------------

class TestAnalyzeSearchResults:
    def test_returns_analysis_structure(self):
        resp = mcp.handle_request({
            "method": "tools/call",
            "id": 10,
            "params": {"name": "analyze_search_results", "arguments": {"query": "Kündigung"}},
        })
        content = json.loads(resp["result"]["content"][0]["text"])
        assert "total_results" in content
        assert "analysis" in content
        assert "key_decisions" in content
        assert "by_year" in content["analysis"]
        assert "by_canton" in content["analysis"]


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------

class TestErrorCases:
    def test_unknown_tool(self):
        resp = mcp.handle_request({
            "method": "tools/call",
            "id": 11,
            "params": {"name": "nonexistent_tool", "arguments": {}},
        })
        assert "error" in resp
        assert resp["error"]["code"] == -32601

    def test_unknown_method(self):
        resp = mcp.handle_request({
            "method": "unknown/method",
            "id": 12,
        })
        assert "error" in resp
        assert resp["error"]["code"] == -32601

    def test_notification_returns_none(self):
        resp = mcp.handle_request({"method": "notifications/initialized"})
        assert resp is None

    def test_exception_returns_iserror(self):
        """When a tool raises, the response should include isError."""
        with patch.object(mcp, "search_caselaw", side_effect=RuntimeError("boom")):
            resp = mcp.handle_request({
                "method": "tools/call",
                "id": 13,
                "params": {"name": "search_caselaw", "arguments": {"query": "test"}},
            })
            assert resp["result"]["isError"] is True
            assert "Error" in resp["result"]["content"][0]["text"]
