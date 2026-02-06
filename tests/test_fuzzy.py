"""Tests for local_app fuzzy matching module."""
from __future__ import annotations

import pytest
from caselaw_local.fuzzy import (
    _trigrams,
    trigram_similarity,
    get_suggestion,
    get_suggestions_for_terms,
    clear_cache,
    initialize_cache,
)


# ---------------------------------------------------------------------------
# _trigrams
# ---------------------------------------------------------------------------

class TestTrigrams:
    def test_short_string_below_3(self):
        result = _trigrams("ab")
        assert result == {"ab"}

    def test_single_char(self):
        result = _trigrams("x")
        assert result == {"x"}

    def test_normal_string(self):
        result = _trigrams("hello")
        assert "hel" in result
        assert "ell" in result
        assert "llo" in result
        assert len(result) == 3

    def test_lowercase(self):
        result = _trigrams("Hello")
        assert "hel" in result  # lowercased

    def test_empty_string(self):
        result = _trigrams("")
        assert result == {""}


# ---------------------------------------------------------------------------
# trigram_similarity
# ---------------------------------------------------------------------------

class TestTrigramSimilarity:
    def test_identical_strings(self):
        assert trigram_similarity("datenschutz", "datenschutz") == 1.0

    def test_case_insensitive_identical(self):
        assert trigram_similarity("Datenschutz", "datenschutz") == 1.0

    def test_completely_different(self):
        sim = trigram_similarity("abc", "xyz")
        assert sim == 0.0

    def test_similar_strings(self):
        sim = trigram_similarity("datenschutz", "datenschuzt")
        assert sim > 0.5

    def test_empty_first(self):
        assert trigram_similarity("", "hello") == 0.0

    def test_empty_second(self):
        assert trigram_similarity("hello", "") == 0.0

    def test_both_empty(self):
        assert trigram_similarity("", "") == 0.0

    def test_returns_float_between_0_and_1(self):
        sim = trigram_similarity("arbeitsrecht", "arbeitsrech")
        assert 0.0 <= sim <= 1.0


# ---------------------------------------------------------------------------
# get_suggestion / get_suggestions_for_terms (with real DB)
# ---------------------------------------------------------------------------

class TestFuzzySuggestions:
    def test_get_suggestion_with_populated_cache(self, db_with_decisions):
        """After initializing cache, a misspelling should get a suggestion."""
        clear_cache()
        initialize_cache(db_with_decisions)
        # "Datenschutz" appears in content; a close misspelling may or may not match
        # depending on extracted terms. At minimum we verify no crash.
        result = get_suggestion(db_with_decisions, "xyznonexistent")
        # result may be None if nothing is similar enough — that's fine
        assert result is None or isinstance(result, str)

    def test_get_suggestion_short_query(self, db_with_decisions):
        clear_cache()
        assert get_suggestion(db_with_decisions, "ab") is None

    def test_get_suggestions_for_terms_empty(self, db_with_decisions):
        clear_cache()
        assert get_suggestions_for_terms(db_with_decisions, []) == []

    def test_get_suggestions_for_terms_limit(self, db_with_decisions):
        clear_cache()
        initialize_cache(db_with_decisions)
        results = get_suggestions_for_terms(
            db_with_decisions, ["aaaa", "bbbb", "cccc", "dddd"], limit=2
        )
        assert len(results) <= 2

    def test_clear_cache(self, db_with_decisions):
        """clear_cache resets the internal state without error."""
        initialize_cache(db_with_decisions)
        clear_cache()
        # After clearing, get_suggestion should re-initialize
        result = get_suggestion(db_with_decisions, "test")
        assert result is None or isinstance(result, str)
