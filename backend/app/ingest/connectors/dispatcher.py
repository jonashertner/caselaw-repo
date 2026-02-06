from __future__ import annotations

from app.ingest.connectors.bger_search import BGerSearchConnector
from app.ingest.connectors.bpatger_connector import BPatGerConnector
from app.ingest.connectors.crawler_connector import CrawlerConnector
from app.ingest.connectors.entscheidsuche_connector import EntscheidsucheConnector
from app.ingest.connectors.sitemap_connector import SitemapConnector
from app.ingest.connectors.weblaw_connector import WeblawConnector
from app.ingest.connectors.base import Connector


def get_connector(name: str) -> Connector:
    name = (name or "crawler").lower()
    if name == "bger_search":
        return BGerSearchConnector()
    if name == "weblaw":
        return WeblawConnector()
    if name == "bpatger":
        return BPatGerConnector()
    if name == "sitemap":
        return SitemapConnector()
    if name == "entscheidsuche":
        return EntscheidsucheConnector()
    return CrawlerConnector()
