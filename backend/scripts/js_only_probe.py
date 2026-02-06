"""Quick probe to flag JS-only source portals.

Usage:
  python backend/scripts/js_only_probe.py

Notes:
- Heuristic only. Use it to decide whether to use the crawler connector or a more structured connector
  (e.g., SitemapConnector).
"""

from __future__ import annotations

import asyncio
import re
import sys
import urllib.parse

import httpx


JS_HINTS = re.compile(
    r"(enable javascript|javascript enabled|you have to enable javascript|application built with vaadin)",
    re.IGNORECASE,
)


def _count_anchors(html: str) -> int:
    # Cheap, good-enough heuristic.
    return html.lower().count("<a ")


async def _probe_one(client: httpx.AsyncClient, url: str) -> tuple[bool, str]:
    try:
        r = await client.get(url, follow_redirects=True, timeout=20)
    except Exception as e:
        return (False, f"fetch_failed: {e}")

    ct = (r.headers.get("content-type") or "").lower()
    if "text/html" not in ct and not ct.startswith("text/"):
        return (False, f"not_html: {ct or 'unknown'}")

    html = r.text or ""
    if JS_HINTS.search(html):
        return (True, "js_hint")

    anchors = _count_anchors(html)
    # If there are essentially no links, but a lot of scripts, it's likely a SPA.
    scripts = html.lower().count("<script")
    if anchors < 5 and scripts >= 3:
        return (True, f"sparse_links: anchors={anchors} scripts={scripts}")

    return (False, f"ok: anchors={anchors} scripts={scripts}")


async def main() -> int:
    # Lazy import to avoid backend dependency wiring for this standalone script.
    from app.services.source_registry import SourceRegistry

    reg = SourceRegistry.load_default()
    sources = reg.list()

    async with httpx.AsyncClient(headers={"User-Agent": "SwissCaseLawAI/1.0"}) as client:
        for s in sources:
            url = s.start_urls[0] if s.start_urls else s.homepage
            if not url:
                continue

            js_only, reason = await _probe_one(client, url)
            tag = "JS_ONLY" if js_only else "OK"
            print(f"{tag}\t{s.id}\t{url}\t{reason}")

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
