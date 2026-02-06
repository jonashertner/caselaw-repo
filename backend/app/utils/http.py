from __future__ import annotations

import logging
import urllib.parse
import urllib.robotparser
from dataclasses import dataclass
from typing import Optional

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from app.core.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


@dataclass
class FetchResult:
    url: str
    status_code: int
    content: bytes
    content_type: str | None


class RobotsCache:
    def __init__(self) -> None:
        self._cache: dict[str, urllib.robotparser.RobotFileParser] = {}

    async def allowed(self, client: httpx.AsyncClient, url: str, user_agent: str) -> bool:
        parsed = urllib.parse.urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        rp = self._cache.get(base)
        if rp is None:
            rp = urllib.robotparser.RobotFileParser()
            rp.set_url(f"{base}/robots.txt")
            try:
                r = await client.get(rp.url, headers={"User-Agent": user_agent}, timeout=10)
                rp.parse(r.text.splitlines())
            except Exception:
                # If robots can't be fetched, default to allow (many gov sites block robots fetch).
                rp.parse([])
            self._cache[base] = rp
        try:
            return rp.can_fetch(user_agent, url)
        except Exception:
            return True


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=0.5, min=0.5, max=6))
async def fetch_bytes(
    client: httpx.AsyncClient,
    url: str,
    *,
    user_agent: Optional[str] = None,
    timeout_s: Optional[int] = None,
) -> FetchResult:
    headers = {"User-Agent": user_agent or settings.ingest_user_agent}
    timeout = timeout_s or settings.ingest_request_timeout_s
    r = await client.get(url, headers=headers, follow_redirects=True, timeout=timeout)
    ct = r.headers.get("content-type")
    return FetchResult(url=str(r.url), status_code=r.status_code, content=r.content, content_type=ct)
