"""Tests for local_app search module."""
from __future__ import annotations

import pytest
from caselaw_local.search import (
    _build_filter_sql,
    search,
    validate_and_search,
    get_doc,
    suggest,
    search_for_export,
)


# ---------------------------------------------------------------------------
# _build_filter_sql
# ---------------------------------------------------------------------------

class TestBuildFilterSql:
    def test_empty_filters(self):
        sql, params = _build_filter_sql({})
        assert sql == ""
        assert params == {}

    def test_single_canton_filter(self):
        sql, params = _build_filter_sql({"canton": ["ZH"]})
        assert "d.canton IN" in sql
        assert params["canton_0"] == "ZH"

    def test_multiple_canton_filter(self):
        sql, params = _build_filter_sql({"canton": ["ZH", "BE"]})
        assert "d.canton IN" in sql
        assert params["canton_0"] == "ZH"
        assert params["canton_1"] == "BE"

    def test_language_filter(self):
        sql, params = _build_filter_sql({"language": ["de", "fr"]})
        assert "d.language IN" in sql

    def test_level_filter(self):
        sql, params = _build_filter_sql({"level": ["federal"]})
        assert "d.level IN" in sql

    def test_date_from(self):
        sql, params = _build_filter_sql({"date_from": "2024-01-01"})
        assert "d.decision_date >=" in sql
        assert params["date_from"] == "2024-01-01"

    def test_date_to(self):
        sql, params = _build_filter_sql({"date_to": "2024-12-31"})
        assert "d.decision_date <=" in sql
        assert params["date_to"] == "2024-12-31"

    def test_docket_prefix(self):
        sql, params = _build_filter_sql({"docket": "4A_"})
        assert "d.docket LIKE" in sql
        assert params["docket_like"] == "4A_%"

    def test_combined_filters(self):
        sql, params = _build_filter_sql({
            "canton": ["ZH"],
            "language": ["de"],
            "date_from": "2024-01-01",
        })
        assert "d.canton IN" in sql
        assert "d.language IN" in sql
        assert "d.decision_date >=" in sql
        assert sql.startswith(" AND ")


# ---------------------------------------------------------------------------
# search – browse mode (empty query)
# ---------------------------------------------------------------------------

class TestSearchBrowse:
    def test_returns_results(self, db_with_decisions):
        result = search(db_with_decisions, q="", page=1, page_size=20)
        assert result["total"] == 5
        assert len(result["results"]) == 5

    def test_pagination(self, db_with_decisions):
        result = search(db_with_decisions, q="", page=1, page_size=2)
        assert len(result["results"]) == 2
        assert result["page"] == 1
        assert result["page_size"] == 2

    def test_facets_present(self, db_with_decisions):
        result = search(db_with_decisions, q="", page=1)
        assert "facets" in result
        assert "language" in result["facets"]
        assert "canton" in result["facets"]

    def test_browse_with_canton_filter(self, db_with_decisions):
        result = search(db_with_decisions, q="", filters={"canton": ["ZH"]})
        assert result["total"] == 1
        assert result["results"][0]["canton"] == "ZH"


# ---------------------------------------------------------------------------
# search – FTS mode
# ---------------------------------------------------------------------------

class TestSearchFTS:
    def test_fts_returns_results(self, db_with_decisions):
        result = search(db_with_decisions, q="Datenschutz")
        assert result["total"] > 0
        assert len(result["results"]) > 0

    def test_fts_has_snippets(self, db_with_decisions):
        result = search(db_with_decisions, q="Kündigung")
        assert result["total"] > 0
        # Snippet field should exist
        for r in result["results"]:
            assert "snippet" in r

    def test_fts_has_rank(self, db_with_decisions):
        result = search(db_with_decisions, q="Bundesgericht")
        for r in result["results"]:
            assert "rank" in r

    def test_fts_with_language_filter(self, db_with_decisions):
        result = search(db_with_decisions, q="licenciement", filters={"language": ["fr"]})
        assert result["total"] >= 1
        for r in result["results"]:
            assert r["language"] == "fr"

    def test_fts_with_level_filter(self, db_with_decisions):
        result = search(db_with_decisions, q="Bundesgericht", filters={"level": ["federal"]})
        for r in result["results"]:
            assert r["level"] == "federal"

    def test_fts_facets_present(self, db_with_decisions):
        result = search(db_with_decisions, q="Kündigung")
        assert "facets" in result


# ---------------------------------------------------------------------------
# validate_and_search
# ---------------------------------------------------------------------------

class TestValidateAndSearch:
    def test_invalid_query_returns_error(self, db_with_decisions):
        result = validate_and_search(db_with_decisions, q='"unclosed')
        assert result["error"] is True
        assert result["total"] == 0

    def test_valid_query_returns_results(self, db_with_decisions):
        result = validate_and_search(db_with_decisions, q="Datenschutz")
        assert "error" not in result or result.get("error") is not True
        assert result["total"] > 0

    def test_empty_query_browse(self, db_with_decisions):
        result = validate_and_search(db_with_decisions, q="")
        assert result["total"] == 5


# ---------------------------------------------------------------------------
# get_doc
# ---------------------------------------------------------------------------

class TestGetDoc:
    def test_existing_doc(self, db_with_decisions):
        doc = get_doc(db_with_decisions, "bge-140-iii-264")
        assert doc is not None
        assert doc["id"] == "bge-140-iii-264"
        assert doc["title"] is not None

    def test_nonexistent_doc(self, db_with_decisions):
        doc = get_doc(db_with_decisions, "nonexistent-id")
        assert doc is None


# ---------------------------------------------------------------------------
# suggest
# ---------------------------------------------------------------------------

class TestSuggest:
    def test_prefix_matching(self, db_with_decisions):
        results = suggest(db_with_decisions, "Datenschutz")
        assert len(results) > 0

    def test_empty_prefix(self, db_with_decisions):
        results = suggest(db_with_decisions, "")
        assert results == []

    def test_whitespace_prefix(self, db_with_decisions):
        results = suggest(db_with_decisions, "   ")
        assert results == []


# ---------------------------------------------------------------------------
# search_for_export
# ---------------------------------------------------------------------------

class TestSearchForExport:
    def test_browse_export(self, db_with_decisions):
        results = search_for_export(db_with_decisions, q="")
        assert len(results) == 5
        # Should have limited fields
        for r in results:
            assert "id" in r
            assert "title" in r
            assert "content_text" not in r

    def test_fts_export(self, db_with_decisions):
        results = search_for_export(db_with_decisions, q="Kündigung")
        assert len(results) > 0
        for r in results:
            assert "id" in r

    def test_export_max_results(self, db_with_decisions):
        results = search_for_export(db_with_decisions, q="", max_results=2)
        assert len(results) == 2
