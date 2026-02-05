from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Optional, List

from .updater import update as update_fn, local_db_path
from .config import data_dir, manifest_url
from .db import connect, apply_pragmas, ensure_schema


def cmd_update(_: argparse.Namespace) -> None:
    res = update_fn()
    print(res)


def cmd_serve(args: argparse.Namespace) -> None:
    # Import here so update-only installs don't require uvicorn extras at import time
    import uvicorn
    from .server import app

    uvicorn.run(app, host=args.host, port=args.port, reload=args.reload)


def cmd_doctor(_: argparse.Namespace) -> None:
    dd = data_dir()
    print(f"Data dir: {dd}")
    print(f"Manifest: {manifest_url()}")
    dbp = local_db_path(dd)
    print(f"DB: {dbp}")
    if not dbp.exists():
        print("DB missing. Run: caselaw-local update")
        return

    conn = connect(dbp, read_only=False)
    try:
        apply_pragmas(conn, read_only=False)
        ensure_schema(conn)
        # Check FTS5
        row = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='decisions_fts';").fetchone()
        print(f"FTS present: {bool(row)}")
        # Quick query
        n = conn.execute("SELECT COUNT(*) AS n FROM decisions;").fetchone()["n"]
        print(f"Decisions: {n}")
    finally:
        conn.close()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="caselaw-local")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("update", help="Download snapshot + apply deltas")
    s.set_defaults(fn=cmd_update)

    s = sub.add_parser("serve", help="Run local search web app")
    s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=8787)
    s.add_argument("--reload", action="store_true")
    s.set_defaults(fn=cmd_serve)

    s = sub.add_parser("doctor", help="Verify local install")
    s.set_defaults(fn=cmd_doctor)

    return p


def main(argv: Optional[List[str]] = None) -> None:
    args = build_parser().parse_args(argv)
    args.fn(args)


if __name__ == "__main__":
    main()
