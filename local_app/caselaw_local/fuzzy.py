"""Fuzzy matching for "did you mean" suggestions using trigram similarity."""
from __future__ import annotations

import sqlite3
from typing import Optional
import threading


# Global cache for terms
_term_cache: list[str] = []
_cache_lock = threading.Lock()
_cache_initialized = False

# Similarity threshold for suggestions
SIMILARITY_THRESHOLD = 0.4
MAX_CACHED_TERMS = 10000


def _trigrams(s: str) -> set[str]:
    """Generate trigrams from a string."""
    s = s.lower().strip()
    if len(s) < 3:
        return {s}
    return {s[i:i+3] for i in range(len(s) - 2)}


def trigram_similarity(s1: str, s2: str) -> float:
    """
    Calculate trigram similarity between two strings.

    Returns a value between 0 and 1, where 1 means identical.
    Uses Jaccard similarity of trigram sets.
    """
    if not s1 or not s2:
        return 0.0

    if s1.lower() == s2.lower():
        return 1.0

    t1 = _trigrams(s1)
    t2 = _trigrams(s2)

    if not t1 or not t2:
        return 0.0

    intersection = len(t1 & t2)
    union = len(t1 | t2)

    return intersection / union if union > 0 else 0.0


def _load_term_cache(conn: sqlite3.Connection) -> None:
    """
    Load frequently occurring terms from titles into the cache.

    Called lazily on first zero-result query.
    """
    global _term_cache, _cache_initialized

    with _cache_lock:
        if _cache_initialized:
            return

        # Extract terms from titles - focus on distinct words
        # This query gets the most common words from titles
        try:
            rows = conn.execute(
                """
                WITH words AS (
                    SELECT DISTINCT lower(word) AS word
                    FROM (
                        SELECT DISTINCT
                            trim(
                                replace(replace(replace(replace(
                                    substr(title,
                                        instr(' ' || title, ' ') +
                                        ((numbers.n - 1) * (SELECT avg(length(title)/10) FROM (SELECT title FROM decisions LIMIT 100))),
                                        instr(substr(title || ' ',
                                            instr(' ' || title, ' ') +
                                            ((numbers.n - 1) * (SELECT avg(length(title)/10) FROM (SELECT title FROM decisions LIMIT 100)))), ' ')
                                    ),
                                    ',', ''), '.', ''), '(', ''), ')', '')
                            ) AS word
                        FROM decisions,
                             (SELECT 1 AS n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) AS numbers
                        WHERE title IS NOT NULL AND title != ''
                        LIMIT 100000
                    )
                    WHERE length(word) >= 4
                )
                SELECT word FROM words
                WHERE word NOT IN ('und', 'der', 'die', 'das', 'von', 'vom', 'den', 'dem', 'des',
                                   'ein', 'eine', 'einer', 'eines', 'mit', 'bei', 'auf', 'aus',
                                   'für', 'zur', 'zum', 'als', 'bis', 'nach', 'über', 'unter',
                                   'the', 'and', 'for', 'with', 'from', 'this', 'that')
                LIMIT ?;
                """,
                (MAX_CACHED_TERMS,)
            ).fetchall()
            _term_cache = [r[0] for r in rows if r[0]]
        except Exception:
            # Fallback: simpler query
            rows = conn.execute(
                """
                SELECT DISTINCT
                    lower(title) AS term
                FROM decisions
                WHERE title IS NOT NULL AND length(title) >= 4
                ORDER BY decision_date DESC
                LIMIT ?;
                """,
                (MAX_CACHED_TERMS,)
            ).fetchall()
            _term_cache = [r[0] for r in rows if r[0]]

        _cache_initialized = True


def initialize_cache(conn: sqlite3.Connection) -> None:
    """Explicitly initialize the term cache."""
    _load_term_cache(conn)


def get_suggestion(conn: sqlite3.Connection, query: str) -> Optional[str]:
    """
    Get a "did you mean" suggestion for a query.

    Only call this when search returns zero results.

    Args:
        conn: Database connection
        query: The user's search query

    Returns:
        A suggested correction if similarity > threshold, else None
    """
    if not query or len(query.strip()) < 3:
        return None

    # Ensure cache is loaded
    _load_term_cache(conn)

    if not _term_cache:
        return None

    query_lower = query.lower().strip()

    # Find best match
    best_match = None
    best_score = 0.0

    for term in _term_cache:
        # Quick length filter
        if abs(len(term) - len(query_lower)) > 5:
            continue

        score = trigram_similarity(query_lower, term)
        if score > best_score:
            best_score = score
            best_match = term

    if best_score >= SIMILARITY_THRESHOLD and best_match and best_match != query_lower:
        return best_match

    return None


def get_suggestions_for_terms(conn: sqlite3.Connection, terms: list[str], limit: int = 3) -> list[tuple[str, str]]:
    """
    Get suggestions for multiple terms.

    Args:
        conn: Database connection
        terms: List of search terms to check
        limit: Maximum number of suggestions to return

    Returns:
        List of (original_term, suggested_term) tuples
    """
    if not terms:
        return []

    # Ensure cache is loaded
    _load_term_cache(conn)

    if not _term_cache:
        return []

    suggestions = []

    for term in terms:
        if len(term) < 3:
            continue

        term_lower = term.lower()

        best_match = None
        best_score = 0.0

        for cached in _term_cache:
            if abs(len(cached) - len(term_lower)) > 4:
                continue

            score = trigram_similarity(term_lower, cached)
            if score > best_score and cached != term_lower:
                best_score = score
                best_match = cached

        if best_score >= SIMILARITY_THRESHOLD and best_match:
            suggestions.append((term, best_match))

        if len(suggestions) >= limit:
            break

    return suggestions


def clear_cache() -> None:
    """Clear the term cache (useful for testing)."""
    global _term_cache, _cache_initialized
    with _cache_lock:
        _term_cache = []
        _cache_initialized = False
