from __future__ import annotations

import asyncio
import datetime as dt
import logging
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from app.core.logging import configure_logging
from app.db.init_db import init_db
from app.db.session import get_session
from app.ingest.common import IngestArgs
from app.ingest.connectors.dispatcher import get_connector
from app.services.indexer import Indexer
from app.services.source_registry import SourceRegistry

app = typer.Typer(no_args_is_help=True)
console = Console()
logger = logging.getLogger(__name__)


@app.command()
def sources() -> None:
    """List configured sources."""
    reg = SourceRegistry.load_default()
    table = Table(title="Sources")
    table.add_column("id")
    table.add_column("level")
    table.add_column("canton")
    table.add_column("name")
    table.add_column("connector")
    for s in reg.list():
        table.add_row(s.id, s.level, s.canton or "", s.name, s.connector)
    console.print(table)


db = typer.Typer(no_args_is_help=True)
app.add_typer(db, name="db")


@db.command("init")
def db_init() -> None:
    """Create extensions and tables."""
    configure_logging()
    init_db()
    console.print("[green]DB initialized.[/green]")

@db.command("tune")
def db_tune() -> None:
    """Create optional indexes (vector + full-text)."""
    configure_logging()
    from sqlalchemy import text as sql_text
    from app.db.session import engine

    stmts = [
        # Full-text search expression index
        "CREATE INDEX IF NOT EXISTS idx_chunks_fts ON chunks USING gin (to_tsvector('simple', text));",
        # Vector index (HNSW) for cosine distance; requires pgvector >= 0.5 and fixed dim
        "CREATE INDEX IF NOT EXISTS idx_chunks_embedding_hnsw ON chunks USING hnsw (embedding vector_cosine_ops);",
    ]
    with engine.begin() as conn:
        for s in stmts:
            try:
                conn.execute(sql_text(s))
            except Exception as e:
                console.print(f"[yellow]Skip[/yellow] {s} ({e})")
    console.print("[green]DB tuned (best-effort).[/green]")


ingest = typer.Typer(no_args_is_help=True)
app.add_typer(ingest, name="ingest")


@ingest.command("run")
def ingest_run(
    source: list[str] = typer.Option(["all"], "--source", "-s", help="Source id(s) or 'all'"),
    since: Optional[str] = typer.Option(None, help="YYYY-MM-DD (inclusive)"),
    until: Optional[str] = typer.Option(None, help="YYYY-MM-DD (inclusive)"),
    max_pages: Optional[int] = typer.Option(None, help="Max listing pages (connector-specific)"),
    max_depth: Optional[int] = typer.Option(None, help="Crawler max link depth"),
) -> None:
    """Ingest configured sources."""
    configure_logging()
    reg = SourceRegistry.load_default()
    args = IngestArgs(
        since=dt.date.fromisoformat(since) if since else None,
        until=dt.date.fromisoformat(until) if until else None,
        max_pages=max_pages,
        max_depth=max_depth,
    )
    indexer = Indexer()

    async def _run() -> None:
        total = 0
        with get_session() as session:
            for s in reg.iter_selected(source):
                console.print(f"[bold]{s.id}[/bold] {s.name}")
                connector = get_connector(s.connector)
                inserted = await connector.run(session, s, args=args, indexer=indexer)
                console.print(f"  inserted: {inserted}")
                total += inserted
        console.print(f"[bold]Total inserted: {total}[/bold]")

    asyncio.run(_run())


if __name__ == "__main__":
    app()
