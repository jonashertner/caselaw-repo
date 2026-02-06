"""Entscheidsuche.ch API connector.

This connector fetches Swiss court decisions from the entscheidsuche.ch API,
which aggregates 700K+ decisions from all cantons.

API: https://entscheidsuche.ch/_search.php (Elasticsearch-based)
"""
from __future__ import annotations

import asyncio
import datetime as dt
import logging
from typing import Optional

import httpx
from sqlmodel import Session

from app.ingest.common import IngestArgs
from app.models.decision import Decision
from app.services.indexer import Indexer, stable_uuid_url
from app.services.source_registry import Source

logger = logging.getLogger(__name__)

API_URL = "https://entscheidsuche.ch/_search.php"
BATCH_SIZE = 100
DEFAULT_HEADERS = {
    "User-Agent": "SwissCaselawBot/1.0 (+https://github.com/jonashertner/swiss-caselaw)",
    "Accept": "application/json",
}


def compute_hash(content: str) -> str:
    """Compute content hash for deduplication."""
    import hashlib
    return hashlib.sha256(content.encode()).hexdigest()[:32]


class EntscheidsucheConnector:
    """Connector for entscheidsuche.ch API."""

    def __init__(self, requests_per_second: float = 5.0):
        self.requests_per_second = requests_per_second
        self._last_request_time: float = 0

    async def _rate_limit(self) -> None:
        """Apply rate limiting."""
        import time
        now = time.time()
        elapsed = now - self._last_request_time
        min_interval = 1.0 / self.requests_per_second
        if elapsed < min_interval:
            await asyncio.sleep(min_interval - elapsed)
        self._last_request_time = time.time()

    async def _fetch_decisions(
        self,
        client: httpx.AsyncClient,
        canton: Optional[str],
        search_after: Optional[list] = None,
        date_from: Optional[dt.date] = None,
        date_to: Optional[dt.date] = None,
    ) -> dict:
        """Fetch decisions from API with pagination."""
        await self._rate_limit()

        # Build query
        must_clauses = []

        if canton:
            must_clauses.append({"term": {"canton": canton}})

        if date_from or date_to:
            date_range = {}
            if date_from:
                date_range["gte"] = date_from.isoformat()
            if date_to:
                date_range["lte"] = date_to.isoformat()
            must_clauses.append({"range": {"date": date_range}})

        if must_clauses:
            query = {"bool": {"must": must_clauses}}
        else:
            query = {"match_all": {}}

        body = {
            "query": query,
            "size": BATCH_SIZE,
            "sort": [{"date": "desc"}, {"_id": "asc"}],
            "_source": ["id", "date", "canton", "title", "abstract", "attachment", "hierarchy", "reference"],
        }

        if search_after:
            body["search_after"] = search_after

        for attempt in range(3):
            try:
                resp = await client.post(API_URL, json=body, timeout=60)
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                if attempt == 2:
                    raise
                logger.warning(f"Retry {attempt + 1}/3 after error: {e}")
                await asyncio.sleep(2 ** attempt)

        return {}

    def _map_canton_to_source(self, canton: str, hierarchy: Optional[list[str]]) -> tuple[str, str, str]:
        """Map canton code to source_id, source_name, level."""
        # Handle federal courts
        if canton == "CH" and hierarchy:
            hierarchy_str = " ".join(hierarchy)
            if "BVGE" in hierarchy_str or "BVGer" in hierarchy_str:
                return ("bvger", "Bundesverwaltungsgericht", "federal")
            if "BStGer" in hierarchy_str:
                return ("bstger", "Bundesstrafgericht", "federal")
            if "BPatGer" in hierarchy_str:
                return ("bpatger", "Bundespatentgericht", "federal")

        canton_names = {
            "CH": ("bger", "Bundesgericht", "federal"),
            "AG": ("ag_es", "Aargau (entscheidsuche.ch)", "cantonal"),
            "AI": ("ai_es", "Appenzell Innerrhoden (entscheidsuche.ch)", "cantonal"),
            "AR": ("ar_es", "Appenzell Ausserrhoden (entscheidsuche.ch)", "cantonal"),
            "BE": ("be_es", "Bern (entscheidsuche.ch)", "cantonal"),
            "BL": ("bl_es", "Basel-Landschaft (entscheidsuche.ch)", "cantonal"),
            "BS": ("bs_es", "Basel-Stadt (entscheidsuche.ch)", "cantonal"),
            "FR": ("fr_es", "Freiburg (entscheidsuche.ch)", "cantonal"),
            "GE": ("ge_es", "Genève (entscheidsuche.ch)", "cantonal"),
            "GL": ("gl_es", "Glarus (entscheidsuche.ch)", "cantonal"),
            "GR": ("gr_es", "Graubünden (entscheidsuche.ch)", "cantonal"),
            "JU": ("ju_es", "Jura (entscheidsuche.ch)", "cantonal"),
            "LU": ("lu_es", "Luzern (entscheidsuche.ch)", "cantonal"),
            "NE": ("ne_es", "Neuchâtel (entscheidsuche.ch)", "cantonal"),
            "NW": ("nw_es", "Nidwalden (entscheidsuche.ch)", "cantonal"),
            "OW": ("ow_es", "Obwalden (entscheidsuche.ch)", "cantonal"),
            "SG": ("sg_es", "St. Gallen (entscheidsuche.ch)", "cantonal"),
            "SH": ("sh_es", "Schaffhausen (entscheidsuche.ch)", "cantonal"),
            "SO": ("so_es", "Solothurn (entscheidsuche.ch)", "cantonal"),
            "SZ": ("sz_es", "Schwyz (entscheidsuche.ch)", "cantonal"),
            "TG": ("tg_es", "Thurgau (entscheidsuche.ch)", "cantonal"),
            "TI": ("ti_es", "Ticino (entscheidsuche.ch)", "cantonal"),
            "UR": ("ur_es", "Uri (entscheidsuche.ch)", "cantonal"),
            "VD": ("vd_es", "Vaud (entscheidsuche.ch)", "cantonal"),
            "VS": ("vs_es", "Valais (entscheidsuche.ch)", "cantonal"),
            "ZG": ("zg_es", "Zug (entscheidsuche.ch)", "cantonal"),
            "ZH": ("zh_es", "Zürich (entscheidsuche.ch)", "cantonal"),
        }
        return canton_names.get(canton, (f"{canton.lower()}_es", f"{canton} (entscheidsuche.ch)", "cantonal"))

    async def run(
        self,
        session: Session,
        source: Source,
        *,
        args: IngestArgs,
        indexer: Indexer,
    ) -> int:
        """Fetch decisions from entscheidsuche.ch API.

        The source configuration determines which canton to fetch:
        - source.canton: Filter by canton code (e.g., "ZH", "BE")
        - If source.canton is None, fetches all decisions
        """
        # Determine canton filter from source
        canton = source.canton if hasattr(source, "canton") else None

        # Use effective dates for historical mode support
        date_from = args.effective_since()
        date_to = args.effective_until()

        logger.info(
            f"EntscheidsucheConnector: canton={canton}, "
            f"date_from={date_from}, date_to={date_to}, "
            f"historical={args.historical}"
        )

        inserted = 0
        skipped = 0
        errors = 0
        search_after = None

        async with httpx.AsyncClient(headers=DEFAULT_HEADERS) as client:
            while True:
                try:
                    data = await self._fetch_decisions(
                        client, canton, search_after, date_from, date_to
                    )
                except Exception as e:
                    logger.error(f"Failed to fetch from entscheidsuche.ch: {e}")
                    errors += 1
                    break

                hits = data.get("hits", {}).get("hits", [])
                if not hits:
                    break

                # Get sort values for next page
                search_after = hits[-1].get("sort")

                for hit in hits:
                    src = hit.get("_source", {})
                    doc_id = src.get("id") or hit.get("_id")
                    doc_canton = src.get("canton", "")

                    # Generate stable ID
                    stable_id = stable_uuid_url(f"entscheidsuche:{doc_id}")

                    # Extract content
                    attachment = src.get("attachment", {})
                    content = attachment.get("content", "")
                    if not content or len(content) < 100:
                        skipped += 1
                        continue

                    # Parse date
                    date_str = src.get("date")
                    decision_date = None
                    if date_str:
                        try:
                            decision_date = dt.date.fromisoformat(date_str)
                        except ValueError:
                            pass

                    # Map to source
                    source_id, source_name, level = self._map_canton_to_source(
                        doc_canton, src.get("hierarchy")
                    )

                    # Get title
                    title_obj = src.get("title", {})
                    title = title_obj.get("de") or title_obj.get("fr") or title_obj.get("it") or doc_id

                    # Get URL
                    content_url = attachment.get("content_url", "")
                    url = content_url or f"https://entscheidsuche.ch/docs/{doc_id}"

                    # Get language
                    language = attachment.get("language")

                    try:
                        dec = Decision(
                            id=stable_id,
                            source_id=source_id,
                            source_name=source_name,
                            level=level,
                            canton=doc_canton if doc_canton != "CH" else None,
                            court=None,
                            chamber=None,
                            docket=None,
                            decision_date=decision_date,
                            published_date=None,
                            title=title[:500] if title else None,
                            language=language,
                            url=url,
                            pdf_url=content_url if content_url and content_url.endswith(".pdf") else None,
                            content_text=content,
                            content_hash=compute_hash(content),
                            meta={
                                "source": "entscheidsuche.ch",
                                "hierarchy": src.get("hierarchy"),
                                "reference": src.get("reference"),
                            },
                        )
                        session.merge(dec)
                        inserted += 1
                    except Exception as e:
                        logger.warning(f"Error processing decision {doc_id}: {e}")
                        errors += 1
                        continue

                    if inserted % 1000 == 0:
                        logger.info(f"Imported {inserted} (skipped {skipped})...")
                        session.commit()

                    # Check max_pages limit
                    if args.max_pages and inserted >= args.max_pages * BATCH_SIZE:
                        break

                if args.max_pages and inserted >= args.max_pages * BATCH_SIZE:
                    break

            session.commit()

        logger.info(
            f"EntscheidsucheConnector complete: "
            f"inserted={inserted}, skipped={skipped}, errors={errors}"
        )
        return inserted
