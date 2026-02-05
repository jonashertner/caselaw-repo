from __future__ import annotations

import csv
import io
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.gzip import GZipMiddleware
from jinja2 import Environment, FileSystemLoader, select_autoescape

from .config import data_dir, manifest_url
from .db import connect, apply_pragmas, ensure_schema, stats, get_database_stats
from .search import validate_and_search, get_doc as get_doc_fn, suggest as suggest_fn, search_for_export
from .updater import update as update_fn, local_db_path


TEMPLATES_DIR = Path(__file__).parent / "templates"
STATIC_DIR = Path(__file__).parent / "static"


class UpdateManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._state: Dict[str, Any] = {"status": "idle", "error": None, "result": None}

    def start(self) -> Dict[str, Any]:
        with self._lock:
            if self._state["status"] == "running":
                return self._state
            self._state = {"status": "running", "error": None, "result": None}

        t = threading.Thread(target=self._run, daemon=True)
        t.start()
        return self._state

    def _run(self) -> None:
        try:
            res = update_fn()
            with self._lock:
                self._state = {"status": "done", "error": None, "result": res}
        except Exception as e:
            with self._lock:
                self._state = {"status": "error", "error": str(e), "result": None}

    def state(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self._state)


env = Environment(
    loader=FileSystemLoader(str(TEMPLATES_DIR)),
    autoescape=select_autoescape(["html", "xml"]),
)

updater = UpdateManager()

app = FastAPI(title="Swiss Caselaw Local Search", version="0.1.0")
app.add_middleware(GZipMiddleware, minimum_size=1024)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


def _get_ro_conn():
    db_path = local_db_path()
    if not db_path.exists():
        raise HTTPException(status_code=503, detail="Dataset not installed. Run update first.")
    conn = connect(db_path, read_only=True)
    apply_pragmas(conn, read_only=True)
    return conn


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    tpl = env.get_template("index.html")
    return tpl.render()


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard() -> str:
    tpl = env.get_template("dashboard.html")
    return tpl.render()


@app.post("/api/search")
async def api_search(payload: Dict[str, Any]) -> Dict[str, Any]:
    q = str(payload.get("q") or "")
    filters = payload.get("filters") or {}
    page = int(payload.get("page") or 1)
    page_size = int(payload.get("page_size") or 20)
    sort = str(payload.get("sort") or "relevance")

    conn = _get_ro_conn()
    try:
        return validate_and_search(conn, q=q, filters=filters, page=page, page_size=page_size, sort=sort)
    finally:
        conn.close()


@app.get("/api/doc/{doc_id}")
def api_doc(doc_id: str) -> Dict[str, Any]:
    conn = _get_ro_conn()
    try:
        doc = get_doc_fn(conn, doc_id)
        if not doc:
            raise HTTPException(status_code=404, detail="Not found")
        return doc
    finally:
        conn.close()


@app.get("/api/suggest")
def api_suggest(q: str, limit: int = 8) -> Dict[str, Any]:
    conn = _get_ro_conn()
    try:
        return {"items": suggest_fn(conn, q, limit=limit)}
    finally:
        conn.close()


@app.get("/api/status")
def api_status() -> Dict[str, Any]:
    dd = data_dir()
    db_path = local_db_path(dd)
    local = None
    if db_path.exists():
        local = stats(db_path)
    return {
        "data_dir": str(dd),
        "db_path": str(db_path),
        "local": local,
        "manifest_url": manifest_url(),
    }


@app.post("/api/update")
def api_update() -> Dict[str, Any]:
    return updater.start()


@app.get("/api/update/status")
def api_update_status() -> Dict[str, Any]:
    return updater.state()


@app.get("/api/health")
def api_health() -> Dict[str, Any]:
    return {"ok": True}


@app.get("/api/stats")
def api_stats(detailed: bool = False) -> Dict[str, Any]:
    """Get comprehensive database statistics for the dashboard."""
    conn = _get_ro_conn()
    try:
        return get_database_stats(conn, detailed=detailed)
    finally:
        conn.close()


@app.post("/api/export/csv")
async def api_export_csv(payload: Dict[str, Any]) -> StreamingResponse:
    """Export search results to CSV."""
    q = str(payload.get("q") or "")
    filters = payload.get("filters") or {}
    max_results = min(int(payload.get("max_results") or 1000), 10000)

    conn = _get_ro_conn()
    try:
        results = search_for_export(conn, q=q, filters=filters, max_results=max_results)

        # Create CSV in memory
        output = io.StringIO()
        writer = csv.writer(output)

        # Header row
        writer.writerow([
            "id", "docket", "title", "decision_date", "court", "canton",
            "language", "level", "source_name", "url", "pdf_url"
        ])

        # Data rows
        for r in results:
            writer.writerow([
                r.get("id", ""),
                r.get("docket", ""),
                r.get("title", ""),
                r.get("decision_date", ""),
                r.get("court", ""),
                r.get("canton", ""),
                r.get("language", ""),
                r.get("level", ""),
                r.get("source_name", ""),
                r.get("url", ""),
                r.get("pdf_url", ""),
            ])

        output.seek(0)

        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=swiss_caselaw_export.csv"}
        )
    finally:
        conn.close()


@app.post("/api/cite")
async def api_cite(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Generate citation for a decision."""
    doc_id = payload.get("id")
    format = payload.get("format", "standard")  # standard, bibtex, apa

    conn = _get_ro_conn()
    try:
        doc = get_doc_fn(conn, doc_id)
        if not doc:
            raise HTTPException(status_code=404, detail="Not found")

        # Build citation based on format
        docket = doc.get("docket", "")
        title = doc.get("title", "")
        date = doc.get("decision_date", "")
        court = doc.get("court", "") or doc.get("source_name", "")
        url = doc.get("url", "")

        if format == "bibtex":
            cite_key = docket.replace("/", "_").replace(" ", "_") if docket else doc_id[:8]
            citation = f"""@misc{{{cite_key},
  title = {{{title}}},
  author = {{{court}}},
  year = {{{date[:4] if date else ""}}},
  howpublished = {{\\url{{{url}}}}},
  note = {{{docket}}}
}}"""
        elif format == "apa":
            year = date[:4] if date else "n.d."
            citation = f"{court} ({year}). {title}. {docket}. {url}"
        else:
            # Standard Swiss legal citation
            citation = f"{court}, {docket}, {date}" if docket else f"{court}, {date}: {title[:50]}..."

        return {"citation": citation, "format": format}
    finally:
        conn.close()
