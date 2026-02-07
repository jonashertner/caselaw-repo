"""Parquet-based search service using DuckDB.

This module provides search functionality for parquet-based datasets,
using DuckDB for fast SQL queries and full-text search on parquet files
streamed from HuggingFace.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import duckdb

logger = logging.getLogger(__name__)

# HuggingFace dataset configuration
HF_REPO_ID = os.environ.get("HF_DATASET_REPO", "voilaj/swiss-caselaw")
CACHE_DIR = Path(os.environ.get("PARQUET_CACHE_DIR", "/tmp/parquet_cache"))


@dataclass
class SearchFilters:
    source_ids: Optional[list[str]] = None
    level: Optional[str] = None
    canton: Optional[str] = None
    language: Optional[str] = None
    date_from: Optional[dt.date] = None
    date_to: Optional[dt.date] = None


@dataclass
class DecisionMinimal:
    id: str
    source_id: str
    source_name: str
    level: str
    canton: Optional[str] = None
    court: Optional[str] = None
    chamber: Optional[str] = None
    docket: Optional[str] = None
    decision_date: Optional[dt.date] = None
    title: Optional[str] = None
    language: Optional[str] = None
    url: str = ""
    pdf_url: Optional[str] = None

    def model_dump(self) -> dict:
        return {
            "id": self.id,
            "source_id": self.source_id,
            "source_name": self.source_name,
            "level": self.level,
            "canton": self.canton,
            "court": self.court,
            "chamber": self.chamber,
            "docket": self.docket,
            "decision_date": self.decision_date,
            "title": self.title,
            "language": self.language,
            "url": self.url,
            "pdf_url": self.pdf_url,
        }


@dataclass
class SearchHit:
    decision: DecisionMinimal
    score: float
    snippet: str
    chunk_id: Optional[str] = None
    chunk_index: Optional[int] = None


# Swiss docket patterns
DOCKET_PATTERNS = [
    re.compile(r'\b\d[A-Z]_\d+/\d{4}\b', re.IGNORECASE),  # 5A_123/2024
    re.compile(r'\b\d[A-Z]_\d+-\d{4}\b', re.IGNORECASE),  # 5A_123-2024
    re.compile(r'\b\d[A-Z]\s+\d+/\d{4}\b', re.IGNORECASE),  # 5A 123/2024
    re.compile(r'\b[A-Z]-\d+/\d{4}\b', re.IGNORECASE),  # A-123/2024
    re.compile(r'\bBGE\s+\d+\s+[IVX]+\s+\d+\b', re.IGNORECASE),  # BGE 144 III 93
]

# Patterns to extract docket from title
TITLE_DOCKET_PATTERNS = [
    re.compile(r'(\d[A-Z]_\d+/\d{4})', re.IGNORECASE),
    re.compile(r'(\d[A-Z]_\d+-\d{4})', re.IGNORECASE),
    re.compile(r'(\d[A-Z]\s+\d+/\d{4})', re.IGNORECASE),
    re.compile(r'\((\d[A-Z]_\d+/\d{4})\)', re.IGNORECASE),  # In parentheses
]

# Court chamber patterns
CHAMBER_PATTERNS = [
    re.compile(r'(I+V?\.?\s*(?:Zivilrechtliche|Öffentlich-rechtliche|Strafrechtliche|Sozialrechtliche)\s*Abteilung)', re.IGNORECASE),
    re.compile(r'((?:Ire?|IIe?|IIIe?)\s*Cour\s*(?:de\s*droit\s*(?:civil|pénal|public|social)))', re.IGNORECASE),
    re.compile(r'((?:Prima|Seconda|Terza)\s*Corte\s*(?:di\s*diritto\s*(?:civile|penale|pubblico|sociale)))', re.IGNORECASE),
    re.compile(r'(Chambre\s*(?:des\s*)?(?:prud\'?hommes|curatelles|civile|pénale|administrative))', re.IGNORECASE),
]

# Citation patterns for tracking
CITATION_PATTERNS = [
    re.compile(r'BGE\s+(\d+)\s+([IVX]+)\s+(\d+)', re.IGNORECASE),  # BGE 144 III 93
    re.compile(r'ATF\s+(\d+)\s+([IVX]+)\s+(\d+)', re.IGNORECASE),  # ATF (French)
    re.compile(r'DTF\s+(\d+)\s+([IVX]+)\s+(\d+)', re.IGNORECASE),  # DTF (Italian)
    re.compile(r'(\d[A-Z]_\d+/\d{4})', re.IGNORECASE),  # Docket references
]


@dataclass
class SearchResult:
    """Search result with hits and metadata."""
    hits: list[SearchHit]
    total: int
    offset: int
    limit: int
    has_more: bool


class ParquetSearchService:
    """Search service using DuckDB on parquet files from HuggingFace."""

    def __init__(
        self,
        repo_id: str = HF_REPO_ID,
        cache_dir: Path = CACHE_DIR,
        use_httpfs: bool = True,
    ):
        import threading
        self.repo_id = repo_id
        self.cache_dir = cache_dir
        self.use_httpfs = use_httpfs
        self._conn: Optional[duckdb.DuckDBPyConnection] = None
        self._initialized = False
        self._lock = threading.Lock()  # Thread safety for DuckDB connection

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create DuckDB connection (thread-safe)."""
        with self._lock:
            if self._conn is None:
                logger.info("Creating new DuckDB connection...")
                self._conn = duckdb.connect(":memory:")
                self._setup_connection()
            return self._conn
    
    def _setup_connection(self) -> None:
        """Setup DuckDB extensions and configuration."""
        conn = self._conn
        if conn is None:
            return

        # Install and load required extensions
        conn.execute("INSTALL httpfs")
        conn.execute("LOAD httpfs")

        # Configure for HuggingFace access
        conn.execute("SET enable_progress_bar = false")
        # Set HTTP timeout for file downloads (default is 30s)
        conn.execute("SET http_timeout = 60000")  # 60 seconds in ms

        # Install FTS extension for full-text search
        conn.execute("INSTALL fts")
        conn.execute("LOAD fts")

        logger.info("DuckDB connection initialized with httpfs, fts extensions")
    
    def _download_parquet_files(self) -> list[str]:
        """Download parquet files from HuggingFace using huggingface_hub."""
        from huggingface_hub import snapshot_download

        logger.info(f"Downloading parquet files from {self.repo_id}...")

        try:
            # Use huggingface_hub's snapshot_download which handles:
            # - Parallel downloads
            # - Retries
            # - Caching
            # - Progress tracking
            local_dir = snapshot_download(
                repo_id=self.repo_id,
                repo_type="dataset",
                local_dir=str(self.cache_dir),
                allow_patterns=["data/*.parquet"],
            )

            # Find all downloaded parquet files
            import glob
            pattern = str(Path(local_dir) / "data" / "*.parquet")
            local_paths = glob.glob(pattern)

            logger.info(f"Downloaded {len(local_paths)} parquet files")
            return sorted(local_paths)

        except Exception as e:
            logger.error(f"Failed to download parquet files: {e}", exc_info=True)
            raise

    def _get_parquet_paths(self) -> list[str]:
        """Get list of local parquet file paths (downloading if needed).

        Only loads files from 2000+ to stay within memory limits.
        Older decisions (pre-2000) are rarely searched.
        """
        import glob
        import re

        # Check if files are already cached
        data_dir = self.cache_dir / "data"
        pattern = str(data_dir / "*.parquet")
        all_files = glob.glob(pattern)

        if not all_files:
            # Download files
            logger.info("No cached files found, downloading from HuggingFace...")
            all_files = self._download_parquet_files()

        # Filter to only include years 2000 and later (memory optimization)
        # File names are like "decisions-2020.parquet"
        year_pattern = re.compile(r'decisions-(\d{4})\.parquet$')
        recent_files = []
        for f in all_files:
            match = year_pattern.search(f)
            if match:
                year = int(match.group(1))
                if year >= 2020:  # Only 2020 and later (last 6 years)
                    recent_files.append(f)

        logger.info(f"Using {len(recent_files)} parquet files (2000+) out of {len(all_files)} total")
        return sorted(recent_files)
    
    def initialize(self) -> None:
        """Initialize the search service by loading local parquet files.

        Downloads files to local cache first (if not cached), then creates
        a VIEW from local files. Local file access is much faster than
        streaming from HuggingFace on every query.
        """
        if self._initialized:
            return

        import time
        start_time = time.time()

        logger.info("Step 1: Getting DuckDB connection...")
        conn = self._get_connection()
        logger.info(f"Step 1 done in {time.time() - start_time:.1f}s")

        # Download files to local cache (or use cached files)
        logger.info("Step 2: Getting parquet file paths...")
        step2_start = time.time()
        local_paths = self._get_parquet_paths()
        logger.info(f"Step 2 done in {time.time() - step2_start:.1f}s, found {len(local_paths)} files")

        if not local_paths:
            raise ValueError("No parquet files found")

        logger.info(f"Step 3: Creating VIEW from {len(local_paths)} local parquet files...")
        step3_start = time.time()

        try:
            # Create VIEW from local parquet files
            paths_list = ", ".join(f"'{path}'" for path in local_paths)
            conn.execute(f"""
                CREATE OR REPLACE VIEW decisions AS
                SELECT * FROM read_parquet([{paths_list}], union_by_name=true)
            """)
            logger.info(f"Step 3 done in {time.time() - step3_start:.1f}s")

            # Skip COUNT(*) - it's slow on 92 parquet files
            # Just verify the view works with a simple query
            logger.info("Step 4: Verifying view with sample query...")
            step4_start = time.time()
            result = conn.execute("SELECT id FROM decisions LIMIT 1").fetchone()
            logger.info(f"Step 4 done in {time.time() - step4_start:.1f}s, view works: {result is not None}")

            self._initialized = True
            logger.info(f"Parquet search service initialized successfully in {time.time() - start_time:.1f}s total")

        except Exception as e:
            logger.error(f"Failed to initialize parquet view: {e}", exc_info=True)
            raise
    
    def refresh(self) -> None:
        """Refresh the parquet view (re-read files for new shards)."""
        self._initialized = False
        self.initialize()
    
    def search(
        self,
        query: str,
        *,
        filters: Optional[SearchFilters] = None,
        limit: int = 20,
        offset: int = 0,
    ) -> SearchResult:
        """Search decisions using DuckDB full-text search.

        Args:
            query: Search query string
            filters: Optional search filters
            limit: Maximum results
            offset: Pagination offset

        Returns:
            SearchResult with hits, total count, and pagination info
        """
        with self._lock:  # Thread-safe search
            self.initialize()

            query = (query or "").strip()
            if not query:
                return SearchResult(hits=[], total=0, offset=offset, limit=limit, has_more=False)

            filters = filters or SearchFilters()

            try:
                # Check for docket pattern
                docket_pattern = self._detect_docket_pattern(query)
                if docket_pattern:
                    result = self._docket_search(docket_pattern, filters=filters, limit=limit, offset=offset)
                    if result.hits:
                        return result

                # Full-text search
                return self._fts_search(query, filters=filters, limit=limit, offset=offset)
            except Exception as e:
                logger.error("Search failed for query %r: %s", query[:100], e, exc_info=True)
                raise

    def _extract_docket_from_title(self, title: str) -> Optional[str]:
        """Extract docket number from title if not already set."""
        if not title:
            return None
        for pattern in TITLE_DOCKET_PATTERNS:
            match = pattern.search(title)
            if match:
                return match.group(1)
        return None

    def _extract_chamber(self, title: str, content: str = "") -> Optional[str]:
        """Extract court chamber from title or content."""
        for text in [title, content[:2000] if content else ""]:
            if not text:
                continue
            for pattern in CHAMBER_PATTERNS:
                match = pattern.search(text)
                if match:
                    return match.group(1).strip()
        return None

    def _extract_citations(self, content: str) -> list[str]:
        """Extract BGE/ATF/DTF citations from content."""
        if not content:
            return []
        citations = set()
        for pattern in CITATION_PATTERNS:
            for match in pattern.finditer(content):
                citations.add(match.group(0))
        return sorted(citations)[:20]  # Limit to 20 citations
    
    def _detect_docket_pattern(self, query: str) -> Optional[str]:
        """Detect if query contains a docket number pattern."""
        for pattern in DOCKET_PATTERNS:
            match = pattern.search(query)
            if match:
                return match.group()
        return None
    
    def _build_filter_clause(self, filters: SearchFilters) -> tuple[str, dict]:
        """Build WHERE clause for filters."""
        conditions = []
        params = {}

        if filters.source_ids:
            # Use named parameters that match the keys in params dict
            placeholders = ", ".join(f"$source_id_{i}" for i in range(len(filters.source_ids)))
            conditions.append(f"source_id IN ({placeholders})")
            for i, sid in enumerate(filters.source_ids):
                params[f"source_id_{i}"] = sid
        if filters.level:
            conditions.append("level = $level")
            params["level"] = filters.level
        if filters.canton:
            conditions.append("canton = $canton")
            params["canton"] = filters.canton
        if filters.language:
            conditions.append("language = $language")
            params["language"] = filters.language
        if filters.date_from:
            conditions.append("decision_date >= $date_from")
            params["date_from"] = str(filters.date_from)
        if filters.date_to:
            conditions.append("decision_date <= $date_to")
            params["date_to"] = str(filters.date_to)
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        return where_clause, params
    
    def _docket_search(
        self,
        docket: str,
        *,
        filters: SearchFilters,
        limit: int,
        offset: int = 0,
    ) -> SearchResult:
        """Search by docket number with total count and deduplication."""
        conn = self._get_connection()
        where_clause, params = self._build_filter_clause(filters)

        # Normalize docket format (handle both _ and - variants)
        docket_normalized = docket.replace("-", "_").replace(" ", "_")
        docket_alt = docket.replace("_", "-").replace(" ", "-")

        # Count total matches first (deduplicated)
        count_sql = f"""
            SELECT COUNT(DISTINCT id)
            FROM decisions
            WHERE (docket = $docket_exact OR docket = $docket_alt
                   OR docket ILIKE $docket_like OR title ILIKE $docket_like)
            AND {where_clause}
        """

        params["docket_exact"] = docket_normalized
        params["docket_alt"] = docket_alt
        params["docket_like"] = f"%{docket}%"

        try:
            total_result = conn.execute(count_sql, params).fetchone()
            total = total_result[0] if total_result else 0
        except Exception as e:
            logger.warning(f"Docket count failed: {e}")
            total = 0

        # Get deduplicated results with relevance scoring
        sql = f"""
            SELECT DISTINCT ON (id)
                id, source_id, source_name, level, canton, court,
                docket, decision_date, title, language, url, pdf_url,
                SUBSTRING(content_text, 1, 1000) as snippet_text,
                CASE
                    WHEN docket = $docket_exact OR docket = $docket_alt THEN 100.0
                    WHEN docket ILIKE $docket_like THEN 80.0
                    WHEN title ILIKE $docket_like THEN 60.0
                    ELSE 50.0
                END as score
            FROM decisions
            WHERE (docket = $docket_exact OR docket = $docket_alt
                   OR docket ILIKE $docket_like OR title ILIKE $docket_like)
            AND {where_clause}
            ORDER BY id, score DESC, decision_date DESC NULLS LAST
        """

        # Wrap for proper ordering after dedup
        outer_sql = f"""
            SELECT * FROM ({sql}) sub
            ORDER BY score DESC, decision_date DESC NULLS LAST
            LIMIT $limit OFFSET $offset
        """

        params["limit"] = limit
        params["offset"] = offset

        try:
            result = conn.execute(outer_sql, params).fetchall()
            hits = self._rows_to_hits(result, docket)
            return SearchResult(
                hits=hits,
                total=total,
                offset=offset,
                limit=limit,
                has_more=(offset + len(hits)) < total,
            )
        except Exception as e:
            logger.warning(f"Docket search failed: {e}")
            return SearchResult(hits=[], total=0, offset=offset, limit=limit, has_more=False)
    
    def _fts_search(
        self,
        query: str,
        *,
        filters: SearchFilters,
        limit: int,
        offset: int,
    ) -> SearchResult:
        """Search using ILIKE on title and docket.

        Since we use a VIEW (not TABLE), we can't create FTS indexes.
        This searches title and docket fields which is fast enough
        as those columns are small. Content search would require
        loading all data into memory which exceeds 16GB limit.
        """
        conn = self._get_connection()
        where_clause, params = self._build_filter_clause(filters)

        words = [w.strip() for w in query.split() if len(w.strip()) >= 2]
        if not words:
            return SearchResult(hits=[], total=0, offset=offset, limit=limit, has_more=False)

        # Build search conditions - search title and docket
        # Use contains_any approach: match if ANY word matches in title OR docket
        word_conditions = []
        for i, word in enumerate(words):
            param_name = f"word_{i}"
            word_conditions.append(f"(title ILIKE ${param_name} OR docket ILIKE ${param_name})")
            params[param_name] = f"%{word}%"

        # OR between words for broader matches
        word_clause = " OR ".join(word_conditions)
        params["limit"] = limit
        params["offset"] = offset

        sql = f"""
            SELECT
                id, source_id, source_name, level, canton, court,
                docket, decision_date, title, language, url, pdf_url,
                SUBSTRING(content_text, 1, 1000) as snippet_text,
                1.0 as score
            FROM decisions
            WHERE ({word_clause})
            AND {where_clause}
            ORDER BY decision_date DESC NULLS LAST
            LIMIT $limit OFFSET $offset
        """

        try:
            logger.info(f"Executing title/docket search for: {query[:50]}")
            result = conn.execute(sql, params).fetchall()
            hits = self._rows_to_hits(result, query)
            logger.info(f"Search returned {len(hits)} results")
            return SearchResult(
                hits=hits,
                total=len(hits),
                offset=offset,
                limit=limit,
                has_more=len(hits) == limit,
            )
        except Exception as e:
            logger.error(f"Search failed: {e}", exc_info=True)
            return SearchResult(hits=[], total=0, offset=offset, limit=limit, has_more=False)
    
    def _rows_to_hits(self, rows: list, query: str) -> list[SearchHit]:
        """Convert database rows to SearchHit objects."""
        hits: list[SearchHit] = []

        for row in rows:
            (id_, source_id, source_name, level, canton, court,
             docket, decision_date, title, language, url, pdf_url,
             snippet_text, score) = row

            # Parse decision_date if string
            if isinstance(decision_date, str) and decision_date:
                try:
                    decision_date = dt.date.fromisoformat(decision_date)
                except ValueError:
                    decision_date = None

            # Extract docket from title if not set
            if not docket and title:
                docket = self._extract_docket_from_title(title)

            # Extract chamber from title/content
            chamber = self._extract_chamber(title or "", snippet_text or "")

            snippet = self._extract_snippet(snippet_text or "", query)

            hits.append(
                SearchHit(
                    decision=DecisionMinimal(
                        id=id_,
                        source_id=source_id,
                        source_name=source_name,
                        level=level,
                        canton=canton,
                        court=court,
                        chamber=chamber,
                        docket=docket,
                        decision_date=decision_date,
                        title=title,
                        language=language,
                        url=url,
                        pdf_url=pdf_url,
                    ),
                    score=float(score) if score else 0.0,
                    snippet=snippet,
                )
            )

        return hits
    
    def _extract_snippet(self, text: str, query: str, max_len: int = 400) -> str:
        """Extract a relevant snippet around query terms."""
        if not text:
            return ""
        
        query_words = [w.lower() for w in query.split() if len(w) > 2]
        text_lower = text.lower()
        
        best_pos = 0
        for word in query_words:
            pos = text_lower.find(word)
            if pos != -1:
                best_pos = max(0, pos - 100)
                break
        
        snippet = text[best_pos:best_pos + max_len]
        if best_pos > 0:
            snippet = "..." + snippet
        if best_pos + max_len < len(text):
            snippet = snippet + "..."
        
        return snippet.strip()
    
    def get_statistics(self) -> dict:
        """Get dataset statistics."""
        self.initialize()
        conn = self._get_connection()

        stats = {}

        try:
            # Total count
            result = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()
            stats["total"] = result[0] if result else 0

            # By level
            result = conn.execute("""
                SELECT level, COUNT(*) as cnt
                FROM decisions
                GROUP BY level
            """).fetchall()
            stats["by_level"] = {row[0]: row[1] for row in result}

            # By canton
            result = conn.execute("""
                SELECT canton, COUNT(*) as cnt
                FROM decisions
                WHERE canton IS NOT NULL
                GROUP BY canton
                ORDER BY cnt DESC
            """).fetchall()
            stats["by_canton"] = {row[0]: row[1] for row in result}

            # By language
            result = conn.execute("""
                SELECT language, COUNT(*) as cnt
                FROM decisions
                WHERE language IS NOT NULL
                GROUP BY language
            """).fetchall()
            stats["by_language"] = {row[0]: row[1] for row in result}

            # By year (extract year from decision_date)
            result = conn.execute("""
                SELECT SUBSTR(CAST(decision_date AS VARCHAR), 1, 4) as year, COUNT(*) as cnt
                FROM decisions
                WHERE decision_date IS NOT NULL
                GROUP BY year
                ORDER BY year
            """).fetchall()
            stats["by_year"] = {row[0]: row[1] for row in result if row[0]}

            # Recent decisions (based on decision_date, not ingestion time)
            today = dt.date.today()
            result = conn.execute("""
                SELECT
                    SUM(CASE WHEN decision_date >= $today_minus_1 THEN 1 ELSE 0 END) as last_24h,
                    SUM(CASE WHEN decision_date >= $today_minus_7 THEN 1 ELSE 0 END) as last_7d,
                    SUM(CASE WHEN decision_date >= $today_minus_30 THEN 1 ELSE 0 END) as last_30d
                FROM decisions
                WHERE decision_date IS NOT NULL
            """, {
                "today_minus_1": str(today - dt.timedelta(days=1)),
                "today_minus_7": str(today - dt.timedelta(days=7)),
                "today_minus_30": str(today - dt.timedelta(days=30)),
            }).fetchone()
            stats["recent"] = {
                "last_24h": result[0] or 0 if result else 0,
                "last_7d": result[1] or 0 if result else 0,
                "last_30d": result[2] or 0 if result else 0,
            }

            # Source count
            result = conn.execute("""
                SELECT COUNT(DISTINCT source_id) FROM decisions
            """).fetchone()
            stats["source_count"] = result[0] if result else 0

        except Exception as e:
            logger.error(f"Failed to get statistics: {e}")
            stats["error"] = str(e)

        return stats

    def get_decision_by_id(self, decision_id: str) -> Optional[dict]:
        """Get a single decision by ID."""
        self.initialize()
        conn = self._get_connection()

        try:
            result = conn.execute("""
                SELECT
                    id, source_id, source_name, level, canton, court, chamber,
                    docket, decision_date, published_date, title, language,
                    url, pdf_url, content_text, content_hash, meta,
                    indexed_at, updated_at
                FROM decisions
                WHERE id = $id
                LIMIT 1
            """, {"id": decision_id}).fetchone()

            if not result:
                return None

            # Convert to dict
            columns = [
                "id", "source_id", "source_name", "level", "canton", "court", "chamber",
                "docket", "decision_date", "published_date", "title", "language",
                "url", "pdf_url", "content_text", "content_hash", "meta",
                "indexed_at", "updated_at"
            ]
            decision = dict(zip(columns, result))

            # Parse meta if it's a string
            if isinstance(decision.get("meta"), str):
                try:
                    decision["meta"] = json.loads(decision["meta"])
                except (json.JSONDecodeError, TypeError):
                    pass

            return decision

        except Exception as e:
            logger.error(f"Failed to get decision {decision_id}: {e}")
            return None
    
    def get_citations_for_decision(self, decision_id: str) -> dict:
        """Extract and return citations from a decision's content.

        Returns:
            dict with 'citations' list, 'bge_references', and 'docket_references'
        """
        decision = self.get_decision_by_id(decision_id)
        if not decision:
            return {"error": "not_found", "citations": []}

        content = decision.get("content_text", "") or ""
        all_citations = self._extract_citations(content)

        # Categorize citations
        bge_refs = []
        docket_refs = []
        for cit in all_citations:
            cit_upper = cit.upper()
            if cit_upper.startswith("BGE") or cit_upper.startswith("ATF") or cit_upper.startswith("DTF"):
                bge_refs.append(cit)
            else:
                docket_refs.append(cit)

        return {
            "decision_id": decision_id,
            "citations": all_citations,
            "bge_references": bge_refs,
            "docket_references": docket_refs,
            "total": len(all_citations),
        }

    def find_citing_decisions(
        self,
        reference: str,
        *,
        limit: int = 50,
        offset: int = 0,
    ) -> SearchResult:
        """Find decisions that cite a given reference (BGE, docket, etc).

        Args:
            reference: The reference to search for (e.g., 'BGE 144 III 93', '5A_123/2024')
            limit: Max results
            offset: Pagination offset

        Returns:
            SearchResult with citing decisions
        """
        self.initialize()
        conn = self._get_connection()

        # Normalize reference for search
        ref_pattern = f"%{reference}%"

        # Count total
        count_sql = """
            SELECT COUNT(DISTINCT id)
            FROM decisions
            WHERE content_text ILIKE $ref_pattern
        """
        try:
            total_result = conn.execute(count_sql, {"ref_pattern": ref_pattern}).fetchone()
            total = total_result[0] if total_result else 0
        except Exception as e:
            logger.warning(f"Citation count failed: {e}")
            total = 0

        # Get citing decisions
        sql = """
            SELECT DISTINCT ON (id)
                id, source_id, source_name, level, canton, court,
                docket, decision_date, title, language, url, pdf_url,
                SUBSTRING(content_text, 1, 1000) as snippet_text,
                50.0 as score
            FROM decisions
            WHERE content_text ILIKE $ref_pattern
            ORDER BY id, decision_date DESC NULLS LAST
        """

        outer_sql = f"""
            SELECT * FROM ({sql}) sub
            ORDER BY decision_date DESC NULLS LAST
            LIMIT $limit OFFSET $offset
        """

        try:
            result = conn.execute(outer_sql, {
                "ref_pattern": ref_pattern,
                "limit": limit,
                "offset": offset,
            }).fetchall()
            hits = self._rows_to_hits(result, reference)
            return SearchResult(
                hits=hits,
                total=total,
                offset=offset,
                limit=limit,
                has_more=(offset + len(hits)) < total,
            )
        except Exception as e:
            logger.error(f"Citation search failed: {e}")
            return SearchResult(hits=[], total=0, offset=offset, limit=limit, has_more=False)

    def get_bulk_decisions(
        self,
        *,
        filters: Optional[SearchFilters] = None,
        limit: int = 1000,
        offset: int = 0,
        fields: Optional[list[str]] = None,
    ) -> dict:
        """Get bulk decisions for research/export purposes.

        Args:
            filters: Optional filters (date range, canton, etc)
            limit: Max results (up to 10000)
            offset: Pagination offset
            fields: Optional list of fields to include (default: minimal)

        Returns:
            dict with 'decisions' list and pagination info
        """
        self.initialize()
        conn = self._get_connection()

        filters = filters or SearchFilters()
        where_clause, params = self._build_filter_clause(filters)

        # Default fields for bulk export
        default_fields = [
            "id", "source_id", "source_name", "level", "canton", "court",
            "docket", "decision_date", "title", "language", "url"
        ]
        select_fields = fields or default_fields

        # Validate and filter fields
        allowed_fields = {
            "id", "source_id", "source_name", "level", "canton", "court", "chamber",
            "docket", "decision_date", "published_date", "title", "language",
            "url", "pdf_url", "content_text", "content_hash"
        }
        select_fields = [f for f in select_fields if f in allowed_fields]
        if not select_fields:
            select_fields = default_fields

        # Limit max to 10000 for bulk requests
        limit = min(limit, 10000)

        # Count total
        count_sql = f"SELECT COUNT(*) FROM decisions WHERE {where_clause}"
        try:
            total_result = conn.execute(count_sql, params).fetchone()
            total = total_result[0] if total_result else 0
        except Exception:
            total = 0

        # Get decisions
        fields_str = ", ".join(select_fields)
        sql = f"""
            SELECT {fields_str}
            FROM decisions
            WHERE {where_clause}
            ORDER BY decision_date DESC NULLS LAST
            LIMIT $limit OFFSET $offset
        """
        params["limit"] = limit
        params["offset"] = offset

        try:
            result = conn.execute(sql, params).fetchall()
            decisions = [dict(zip(select_fields, row)) for row in result]

            # Convert dates to strings
            for dec in decisions:
                for key in ["decision_date", "published_date"]:
                    if key in dec and dec[key]:
                        dec[key] = str(dec[key])

            return {
                "decisions": decisions,
                "total": total,
                "offset": offset,
                "limit": limit,
                "has_more": (offset + len(decisions)) < total,
                "fields": select_fields,
            }
        except Exception as e:
            logger.error(f"Bulk query failed: {e}")
            return {
                "decisions": [],
                "total": 0,
                "offset": offset,
                "limit": limit,
                "has_more": False,
                "error": str(e),
            }

    def close(self) -> None:
        """Close the DuckDB connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
            self._initialized = False


# Global service instance (lazy initialization)
_service: Optional[ParquetSearchService] = None


def get_parquet_search_service() -> ParquetSearchService:
    """Get the global parquet search service instance."""
    global _service
    if _service is None:
        _service = ParquetSearchService()
    return _service


def search_parquet(
    query: str,
    *,
    filters: Optional[SearchFilters] = None,
    limit: int = 20,
    offset: int = 0,
) -> SearchResult:
    """Convenience function to search using the global service."""
    service = get_parquet_search_service()
    return service.search(query, filters=filters, limit=limit, offset=offset)
