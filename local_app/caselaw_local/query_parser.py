"""FTS5 query validation, sanitization, and error suggestions."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass
class QueryValidationResult:
    """Result of query validation."""
    valid: bool
    error: Optional[str] = None
    suggestion: Optional[str] = None
    sanitized: Optional[str] = None


# FTS5 operators and reserved words
FTS5_OPERATORS = {"AND", "OR", "NOT", "NEAR"}
FTS5_COLUMNS = {"title", "docket", "content_text"}


def validate_fts5_query(query: str) -> QueryValidationResult:
    """
    Validate an FTS5 query for syntax errors.

    Returns a QueryValidationResult with:
    - valid: True if query is syntactically valid
    - error: Description of the error if invalid
    - suggestion: Suggested fix for the error
    - sanitized: Cleaned up version of the query
    """
    if not query or not query.strip():
        return QueryValidationResult(valid=True, sanitized="")

    q = query.strip()

    # Check for unbalanced quotes
    quote_error = _check_quotes(q)
    if quote_error:
        return quote_error

    # Check for unbalanced parentheses
    paren_error = _check_parentheses(q)
    if paren_error:
        return paren_error

    # Check for invalid operators
    operator_error = _check_operators(q)
    if operator_error:
        return operator_error

    # Check for invalid column prefixes
    column_error = _check_columns(q)
    if column_error:
        return column_error

    # Sanitize the query
    sanitized = sanitize_query(q)

    return QueryValidationResult(valid=True, sanitized=sanitized)


def _check_quotes(query: str) -> Optional[QueryValidationResult]:
    """Check for unbalanced quotes."""
    in_quote = False
    quote_char = None
    last_quote_pos = -1

    i = 0
    while i < len(query):
        c = query[i]
        if c in ('"', "'") and (i == 0 or query[i-1] != '\\'):
            if not in_quote:
                in_quote = True
                quote_char = c
                last_quote_pos = i
            elif c == quote_char:
                in_quote = False
                quote_char = None
        i += 1

    if in_quote:
        # Try to suggest a fix
        unclosed_text = query[last_quote_pos:]
        suggestion = query + quote_char
        return QueryValidationResult(
            valid=False,
            error=f"Unclosed quote starting at position {last_quote_pos}",
            suggestion=suggestion,
            sanitized=None
        )

    return None


def _check_parentheses(query: str) -> Optional[QueryValidationResult]:
    """Check for unbalanced parentheses."""
    depth = 0
    in_quote = False
    quote_char = None

    for i, c in enumerate(query):
        if c in ('"', "'") and (i == 0 or query[i-1] != '\\'):
            if not in_quote:
                in_quote = True
                quote_char = c
            elif c == quote_char:
                in_quote = False
                quote_char = None
        elif not in_quote:
            if c == '(':
                depth += 1
            elif c == ')':
                depth -= 1
                if depth < 0:
                    return QueryValidationResult(
                        valid=False,
                        error=f"Unexpected closing parenthesis at position {i}",
                        suggestion=query[:i] + query[i+1:],
                        sanitized=None
                    )

    if depth > 0:
        suggestion = query + ')' * depth
        return QueryValidationResult(
            valid=False,
            error=f"Unclosed parenthesis ({depth} missing)",
            suggestion=suggestion,
            sanitized=None
        )

    return None


def _check_operators(query: str) -> Optional[QueryValidationResult]:
    """Check for invalid operator usage."""
    # Remove quoted strings for operator checking
    without_quotes = re.sub(r'"[^"]*"', ' ', query)
    without_quotes = re.sub(r"'[^']*'", ' ', without_quotes)

    tokens = without_quotes.split()

    for i, token in enumerate(tokens):
        upper = token.upper()

        # Check for operators at start/end
        if upper in FTS5_OPERATORS:
            if i == 0 and upper in ("AND", "OR"):
                return QueryValidationResult(
                    valid=False,
                    error=f"Query cannot start with {upper}",
                    suggestion=' '.join(tokens[1:]) if len(tokens) > 1 else None,
                    sanitized=None
                )
            if i == len(tokens) - 1 and upper in ("AND", "OR", "NOT"):
                return QueryValidationResult(
                    valid=False,
                    error=f"Query cannot end with {upper}",
                    suggestion=' '.join(tokens[:-1]) if len(tokens) > 1 else None,
                    sanitized=None
                )

        # Check for consecutive operators
        if i > 0:
            prev_upper = tokens[i-1].upper()
            if upper in FTS5_OPERATORS and prev_upper in FTS5_OPERATORS:
                return QueryValidationResult(
                    valid=False,
                    error=f"Consecutive operators: {prev_upper} {upper}",
                    suggestion=None,
                    sanitized=None
                )

    return None


def _check_columns(query: str) -> Optional[QueryValidationResult]:
    """Check for invalid column prefixes."""
    # Match column:term patterns
    pattern = r'\b(\w+):'
    matches = re.finditer(pattern, query)

    for match in matches:
        col = match.group(1).lower()
        if col not in FTS5_COLUMNS:
            # Suggest valid columns
            valid_cols = ", ".join(sorted(FTS5_COLUMNS))
            return QueryValidationResult(
                valid=False,
                error=f"Unknown column '{col}'. Valid columns: {valid_cols}",
                suggestion=None,
                sanitized=None
            )

    return None


def sanitize_query(query: str) -> str:
    """
    Sanitize and normalize an FTS5 query.

    - Normalize whitespace
    - Fix common mistakes
    - Escape special characters where needed
    """
    if not query:
        return ""

    q = query.strip()

    # Normalize multiple spaces to single space
    q = re.sub(r'\s+', ' ', q)

    # Common typo fixes for operators
    replacements = [
        (r'\bund\b', 'AND'),  # German "und"
        (r'\boder\b', 'OR'),  # German "oder"
        (r'\bnicht\b', 'NOT'),  # German "nicht"
        (r'&&', 'AND'),
        (r'\|\|', 'OR'),
    ]

    for pattern, replacement in replacements:
        q = re.sub(pattern, replacement, q, flags=re.IGNORECASE)

    return q


def suggest_correction(query: str, error: str) -> Optional[str]:
    """
    Suggest a correction for a query based on the error.

    This is a fallback for when validate_fts5_query doesn't provide
    a suggestion directly.
    """
    if not query:
        return None

    q = query.strip()

    # Unclosed quote - add closing quote
    if "unclosed quote" in error.lower():
        if q.count('"') % 2 != 0:
            return q + '"'
        if q.count("'") % 2 != 0:
            return q + "'"

    # Unclosed parenthesis - add closing paren
    if "unclosed parenthesis" in error.lower() or "unmatched" in error.lower():
        open_count = q.count('(')
        close_count = q.count(')')
        if open_count > close_count:
            return q + ')' * (open_count - close_count)

    # Operator at end - remove it
    if "cannot end with" in error.lower():
        tokens = q.split()
        if tokens and tokens[-1].upper() in FTS5_OPERATORS:
            return ' '.join(tokens[:-1])

    return None


def extract_search_terms(query: str) -> list[str]:
    """
    Extract individual search terms from a query for fuzzy matching.

    Removes operators and column prefixes, returns plain terms.
    """
    if not query:
        return []

    # Remove quoted phrases (keep as single term)
    quoted = re.findall(r'"([^"]+)"', query)
    without_quotes = re.sub(r'"[^"]*"', ' ', query)

    # Remove column prefixes
    without_cols = re.sub(r'\b\w+:', '', without_quotes)

    # Remove operators and parentheses
    for op in FTS5_OPERATORS:
        without_cols = re.sub(rf'\b{op}\b', ' ', without_cols, flags=re.IGNORECASE)
    without_cols = without_cols.replace('(', ' ').replace(')', ' ')

    # Split into terms
    terms = without_cols.split()

    # Add back quoted phrases
    terms.extend(quoted)

    # Remove empty and very short terms
    terms = [t.strip() for t in terms if t.strip() and len(t.strip()) >= 2]

    return terms
