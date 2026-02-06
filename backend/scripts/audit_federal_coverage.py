"""Audit discovery coverage for federal court sources.

This script does *not* ingest anything. It only checks whether our discovery
mechanisms can enumerate URLs for each federal source.

Usage:
  python backend/scripts/audit_federal_coverage.py

Tip:
  If a source reports 0 discovered items, you likely need a different connector
  (e.g., the portal exposes no sitemap) or the site is blocking your IP/user-agent.
"""

from __future__ import annotations

import asyncio
import datetime as dt

import httpx


FEDERAL_IDS = {"bger", "bger_atf", "bvger", "bstger", "bpatger"}


async def main() -> int:
    from app.core.config import get_settings
    from app.ingest.connectors.bger_search import _extract_decision_links, _pick_base, _build_search_url
    from app.ingest.connectors.sitemap_connector import _discover_sitemaps, _load_sitemap_urls
    from app.services.source_registry import SourceRegistry
    from app.utils.http import fetch_bytes

    settings = get_settings()
    reg = SourceRegistry.load_default()

    sources = [s for s in reg.list() if s.id in FEDERAL_IDS]
    if not sources:
        print("No federal sources configured.")
        return 1

    async with httpx.AsyncClient(headers={"User-Agent": settings.ingest_user_agent}) as client:
        for s in sources:
            print(f"\n== {s.id} :: {s.name} :: connector={s.connector}")

            if s.connector == "bger_search":
                base = _pick_base(s.start_urls)
                url = _build_search_url(base, since=None, until=dt.date.today(), page=1)
                r = await fetch_bytes(client, url)
                if r.status_code >= 400:
                    print(f"BGer search fetch failed: status={r.status_code} url={url}")
                    continue
                html = r.content.decode("utf-8", errors="ignore")
                links = _extract_decision_links(base, html)
                print(f"search_page_1_links={len(links)} base={base}")
                if links:
                    print(f"sample={links[0]}")
                continue

            if s.connector in {"weblaw", "bpatger", "sitemap"}:
                sitemaps = await _discover_sitemaps(client, s.start_urls)
                print(f"sitemaps={len(sitemaps)}")
                if not sitemaps:
                    continue
                urls = await _load_sitemap_urls(client, sitemaps, max_urls=50_000)
                print(f"sitemap_urls_loaded={len(urls)}")
                if urls:
                    print(f"sample={urls[0].loc}")
                continue

            print("No audit for this connector.")

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
