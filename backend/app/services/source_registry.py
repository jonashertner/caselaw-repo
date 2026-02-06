from __future__ import annotations

import pathlib
from dataclasses import dataclass
from typing import Any, Iterable, Optional

import yaml


@dataclass(frozen=True)
class Source:
    id: str
    name: str
    level: str  # federal / cantonal
    canton: Optional[str]
    homepage: str
    start_urls: list[str]
    connector: str
    languages: list[str]
    notes: Optional[str] = None

    @staticmethod
    def from_dict(d: dict[str, Any]) -> "Source":
        return Source(
            id=str(d["id"]),
            name=str(d["name"]),
            level=str(d["level"]),
            canton=d.get("canton"),
            homepage=str(d["homepage"]),
            start_urls=list(d.get("start_urls") or []),
            connector=str(d.get("connector") or "crawler"),
            languages=list(d.get("languages") or []),
            notes=d.get("notes"),
        )


class SourceRegistry:
    def __init__(self, sources: list[Source]):
        self._sources = {s.id: s for s in sources}

    @classmethod
    def load_default(cls) -> "SourceRegistry":
        path = pathlib.Path(__file__).resolve().parent.parent / "data" / "sources.yaml"
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        sources = [Source.from_dict(x) for x in raw.get("sources", [])]
        return cls(sources)

    def get(self, source_id: str) -> Source:
        if source_id not in self._sources:
            raise KeyError(f"Unknown source_id: {source_id}")
        return self._sources[source_id]

    def list(self) -> list[Source]:
        return sorted(self._sources.values(), key=lambda s: (s.level, s.canton or "", s.name))

    def iter_selected(self, source_ids: list[str] | None) -> Iterable[Source]:
        if not source_ids or source_ids == ["all"]:
            yield from self.list()
            return
        # Convenience groups
        lowered = [s.lower() for s in source_ids]
        if "federal" in lowered:
            yield from [s for s in self.list() if s.level == "federal"]
            # Remove the group token so we don't try to resolve it as an id.
            source_ids = [s for s in source_ids if s.lower() != "federal"]
        if "cantonal" in lowered:
            yield from [s for s in self.list() if s.level == "cantonal"]
            source_ids = [s for s in source_ids if s.lower() != "cantonal"]

        for sid in source_ids:
            yield self.get(sid)
