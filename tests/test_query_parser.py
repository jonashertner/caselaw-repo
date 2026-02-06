"""Tests for local_app query_parser module."""
from __future__ import annotations

import pytest
from caselaw_local.query_parser import (
    QueryValidationResult,
    validate_fts5_query,
    sanitize_query,
    suggest_correction,
    extract_search_terms,
)


# ---------------------------------------------------------------------------
# validate_fts5_query – valid queries
# ---------------------------------------------------------------------------

class TestValidQueries:
    def test_simple_term(self):
        r = validate_fts5_query("Datenschutz")
        assert r.valid is True

    def test_quoted_phrase(self):
        r = validate_fts5_query('"fristlose Kündigung"')
        assert r.valid is True

    def test_and_operator(self):
        r = validate_fts5_query("Arbeitsrecht AND Kündigung")
        assert r.valid is True

    def test_or_operator(self):
        r = validate_fts5_query("Mietrecht OR Pachtrecht")
        assert r.valid is True

    def test_not_operator(self):
        r = validate_fts5_query("Steuerrecht NOT Bundesgericht")
        assert r.valid is True

    def test_column_prefix_title(self):
        r = validate_fts5_query("title:BGE")
        assert r.valid is True

    def test_column_prefix_docket(self):
        r = validate_fts5_query("docket:6B_123")
        assert r.valid is True

    def test_column_prefix_content_text(self):
        r = validate_fts5_query("content_text:Datenschutz")
        assert r.valid is True

    def test_nested_parens(self):
        r = validate_fts5_query("(Arbeitsrecht OR Mietrecht) AND Kündigung")
        assert r.valid is True

    def test_empty_query(self):
        r = validate_fts5_query("")
        assert r.valid is True
        assert r.sanitized == ""

    def test_whitespace_only(self):
        r = validate_fts5_query("   ")
        assert r.valid is True


# ---------------------------------------------------------------------------
# validate_fts5_query – invalid queries
# ---------------------------------------------------------------------------

class TestInvalidQueries:
    def test_unclosed_double_quote(self):
        r = validate_fts5_query('"fristlose Kündigung')
        assert r.valid is False
        assert "unclosed quote" in r.error.lower()
        assert r.suggestion is not None

    def test_unclosed_single_quote(self):
        r = validate_fts5_query("'partial")
        assert r.valid is False

    def test_unbalanced_open_paren(self):
        r = validate_fts5_query("(Arbeitsrecht AND Kündigung")
        assert r.valid is False
        assert "parenthesis" in r.error.lower()
        assert r.suggestion is not None

    def test_unbalanced_close_paren(self):
        r = validate_fts5_query("Arbeitsrecht) AND Kündigung")
        assert r.valid is False
        assert "parenthesis" in r.error.lower()

    def test_operator_at_start_and(self):
        r = validate_fts5_query("AND Kündigung")
        assert r.valid is False
        assert "start" in r.error.lower()

    def test_operator_at_start_or(self):
        r = validate_fts5_query("OR Datenschutz")
        assert r.valid is False

    def test_operator_at_end(self):
        r = validate_fts5_query("Datenschutz AND")
        assert r.valid is False
        assert "end" in r.error.lower()

    def test_consecutive_operators(self):
        r = validate_fts5_query("Datenschutz AND OR Kündigung")
        assert r.valid is False
        assert "consecutive" in r.error.lower()

    def test_invalid_column(self):
        r = validate_fts5_query("author:Müller")
        assert r.valid is False
        assert "unknown column" in r.error.lower()


# ---------------------------------------------------------------------------
# sanitize_query
# ---------------------------------------------------------------------------

class TestSanitizeQuery:
    def test_german_und_to_and(self):
        assert "AND" in sanitize_query("Datenschutz und Kündigung")

    def test_german_oder_to_or(self):
        assert "OR" in sanitize_query("Mietrecht oder Pachtrecht")

    def test_german_nicht_to_not(self):
        assert "NOT" in sanitize_query("Steuerrecht nicht Bundesgericht")

    def test_double_ampersand(self):
        assert "AND" in sanitize_query("Arbeitsrecht && Kündigung")

    def test_double_pipe(self):
        assert "OR" in sanitize_query("Arbeitsrecht || Kündigung")

    def test_whitespace_normalization(self):
        result = sanitize_query("Datenschutz   und    Kündigung")
        assert "  " not in result

    def test_empty_string(self):
        assert sanitize_query("") == ""

    def test_none_passthrough(self):
        # sanitize_query expects str; empty str returned
        assert sanitize_query("") == ""


# ---------------------------------------------------------------------------
# extract_search_terms
# ---------------------------------------------------------------------------

class TestExtractSearchTerms:
    def test_simple_terms(self):
        terms = extract_search_terms("Datenschutz Kündigung")
        assert "Datenschutz" in terms
        assert "Kündigung" in terms

    def test_removes_operators(self):
        terms = extract_search_terms("Datenschutz AND Kündigung")
        assert "AND" not in terms
        assert "Datenschutz" in terms

    def test_quoted_phrase_kept(self):
        terms = extract_search_terms('"fristlose Kündigung" AND Arbeitsrecht')
        assert "fristlose Kündigung" in terms
        assert "Arbeitsrecht" in terms

    def test_removes_column_prefix(self):
        terms = extract_search_terms("title:BGE Datenschutz")
        # "BGE" has 3 chars → kept; "title" prefix removed
        assert "BGE" in terms

    def test_filters_short_terms(self):
        terms = extract_search_terms("a b cd Datenschutz")
        assert "a" not in terms
        assert "cd" in terms
        assert "Datenschutz" in terms

    def test_empty_input(self):
        assert extract_search_terms("") == []

    def test_parentheses_removed(self):
        terms = extract_search_terms("(Mietrecht OR Pachtrecht)")
        assert "Mietrecht" in terms
        assert "Pachtrecht" in terms


# ---------------------------------------------------------------------------
# suggest_correction
# ---------------------------------------------------------------------------

class TestSuggestCorrection:
    def test_unclosed_double_quote(self):
        s = suggest_correction('"fristlose', "Unclosed quote")
        assert s is not None
        assert s.endswith('"')

    def test_unclosed_single_quote(self):
        s = suggest_correction("'partial", "Unclosed quote")
        assert s is not None
        assert s.endswith("'")

    def test_unclosed_parenthesis(self):
        s = suggest_correction("(Datenschutz", "Unclosed parenthesis (1 missing)")
        assert s is not None
        assert s.endswith(")")

    def test_operator_at_end(self):
        s = suggest_correction("Datenschutz AND", "Query cannot end with AND")
        assert s == "Datenschutz"

    def test_no_fix_possible(self):
        s = suggest_correction("Datenschutz", "some random error")
        assert s is None

    def test_empty_query(self):
        assert suggest_correction("", "error") is None
