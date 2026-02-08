#!/usr/bin/env python3
"""Daily/weekly incremental update script.

Fetches only NEW decisions since last run, then optionally pushes to HuggingFace.

Cost-effective approach:
- Only fetches decisions from the last N days (default: 7)
- Uses date filters on APIs to minimize requests
- Skips already-imported decisions via content hash

Usage:
    python scripts/daily_update.py [--days 7] [--push]
    python scripts/daily_update.py --push  # Update and push to HuggingFace
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import signal
import sys
import time
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlmodel import select, func
from app.db.session import get_session
from app.models.decision import Decision

logger = logging.getLogger(__name__)


def record_ingestion_run(
    scraper_name: str,
    status: str,
    started_at: dt.datetime,
    from_date: date | None = None,
    to_date: date | None = None,
    decisions_imported: int = 0,
    decisions_skipped: int = 0,
    errors: int = 0,
    error_message: str | None = None,
    details: dict | None = None,
) -> None:
    """Record an ingestion run to the database."""
    try:
        from app.models.ingestion import IngestionRun

        completed_at = dt.datetime.now(dt.timezone.utc)
        duration = (completed_at - started_at).total_seconds()

        with get_session() as session:
            run = IngestionRun(
                scraper_name=scraper_name,
                started_at=started_at,
                completed_at=completed_at,
                duration_seconds=duration,
                status=status,
                decisions_imported=decisions_imported,
                decisions_skipped=decisions_skipped,
                errors=errors,
                from_date=from_date,
                to_date=to_date,
                error_message=error_message,
                details=details or {},
            )
            session.add(run)
            session.commit()
    except Exception as e:
        # Don't fail the whole run if we can't record metrics
        logger.warning(f"Failed to record ingestion run: {e}")


def get_stats() -> dict:
    """Get current database statistics."""
    with get_session() as session:
        total = session.exec(select(func.count(Decision.id))).one()
        federal = session.exec(
            select(func.count(Decision.id)).where(Decision.level == "federal")
        ).one()
        cantonal = session.exec(
            select(func.count(Decision.id)).where(Decision.level == "cantonal")
        ).one()
        return {"total": total, "federal": federal, "cantonal": cantonal}


SCRAPER_TIMEOUT_SECONDS = 600  # 10 minutes per scraper


class ScraperTimeout(Exception):
    pass


def _timeout_handler(signum, frame):
    raise ScraperTimeout("Scraper timed out")


def run_scraper(
    name: str,
    scraper_func,
    from_date: date | None,
    to_date: date | None = None,
    timeout: int = SCRAPER_TIMEOUT_SECONDS,
    **kwargs
) -> dict:
    """Run a single scraper with error handling, timeout, and metrics tracking.

    Returns:
        Dict with status, count, and optionally error
    """
    started_at = dt.datetime.now(dt.timezone.utc)

    # Set per-scraper timeout (signal.alarm works on Linux/macOS main thread)
    old_handler = signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(timeout)

    try:
        count = scraper_func(from_date=from_date, **kwargs)
        signal.alarm(0)  # cancel alarm on success
        result = {"status": "success", "count": count or 0}

        # Record successful run
        record_ingestion_run(
            scraper_name=name,
            status="completed",
            started_at=started_at,
            from_date=from_date,
            to_date=to_date or date.today(),
            decisions_imported=count or 0,
        )

        return result
    except ScraperTimeout:
        logger.warning(f"Scraper {name} timed out after {timeout}s — skipping")
        result = {"status": "timeout", "count": 0, "error": f"Timed out after {timeout}s"}

        record_ingestion_run(
            scraper_name=name,
            status="timeout",
            started_at=started_at,
            from_date=from_date,
            to_date=to_date or date.today(),
            errors=1,
            error_message=f"Timed out after {timeout}s",
        )

        return result
    except Exception as e:
        logger.exception(f"Error running {name}")
        result = {"status": "error", "count": 0, "error": str(e)}

        record_ingestion_run(
            scraper_name=name,
            status="failed",
            started_at=started_at,
            from_date=from_date,
            to_date=to_date or date.today(),
            errors=1,
            error_message=str(e),
        )

        return result
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)


def run_incremental_scrapers(days: int = 7, skip_entscheidsuche: bool = True) -> dict[str, dict]:
    """Run all scrapers with date filter for incremental updates.

    All cantonal courts are now scraped directly from official portals.
    No third-party aggregator (entscheidsuche.ch) dependency.

    Args:
        days: Number of days to look back
        skip_entscheidsuche: Skip legacy entscheidsuche.ch import (deprecated, kept for gap verification only)

    Returns:
        Dict mapping scraper name to result dict with status and count
    """
    from_date = date.today() - timedelta(days=days)
    to_date = date.today()
    results = {}
    step = 0
    total_steps = 14 if skip_entscheidsuche else 15

    print(f"\n{'='*60}")
    print(f"INCREMENTAL UPDATE - Last {days} days (from {from_date})")
    print(f"{'='*60}\n")

    # =========================================================================
    # Federal Courts
    # =========================================================================

    # 1. BGer (Federal Supreme Court) - direct scraping
    step += 1
    print(f"\n[{step}/{total_steps}] Bundesgericht (BGer)...")
    try:
        from scripts.scrape_bger import scrape_bger
        results["bger"] = run_scraper("bger", scrape_bger, from_date, to_date=to_date)
    except ImportError as e:
        results["bger"] = {"status": "error", "count": 0, "error": str(e)}

    # 2. BVGer (Federal Administrative Court) - direct scraper
    step += 1
    print(f"\n[{step}/{total_steps}] Bundesverwaltungsgericht (BVGer)...")
    try:
        from scripts.scrape_bvger_direct import scrape_bvger_direct
        results["bvger"] = run_scraper("bvger", scrape_bvger_direct, from_date, to_date=to_date)
    except ImportError as e:
        results["bvger"] = {"status": "error", "count": 0, "error": str(e)}

    # 3. BStGer (Federal Criminal Court) - direct scraper
    step += 1
    print(f"\n[{step}/{total_steps}] Bundesstrafgericht (BStGer)...")
    try:
        from scripts.scrape_bstger_direct import scrape_bstger_direct
        results["bstger"] = run_scraper("bstger", scrape_bstger_direct, from_date, to_date=to_date)
    except ImportError as e:
        results["bstger"] = {"status": "error", "count": 0, "error": str(e)}

    # 4. BPatGer (Federal Patent Court)
    step += 1
    print(f"\n[{step}/{total_steps}] Bundespatentgericht (BPatGer)...")
    try:
        from scripts.scrape_bpatger import scrape_bpatger
        results["bpatger"] = run_scraper("bpatger", scrape_bpatger, from_date, to_date=to_date)
    except ImportError as e:
        results["bpatger"] = {"status": "error", "count": 0, "error": str(e)}

    # 5. WEKO (Competition Commission)
    step += 1
    print(f"\n[{step}/{total_steps}] Wettbewerbskommission (WEKO)...")
    try:
        from scripts.scrape_weko import scrape_weko
        results["weko"] = run_scraper("weko", scrape_weko, from_date, to_date=to_date)
    except ImportError as e:
        results["weko"] = {"status": "error", "count": 0, "error": str(e)}

    # 6. EDÖB (Data Protection Commissioner)
    step += 1
    print(f"\n[{step}/{total_steps}] EDÖB (Datenschutzbeauftragter)...")
    try:
        from scripts.scrape_edoeb import scrape_edoeb
        results["edoeb"] = run_scraper("edoeb", scrape_edoeb, from_date, to_date=to_date)
    except ImportError as e:
        results["edoeb"] = {"status": "error", "count": 0, "error": str(e)}

    # =========================================================================
    # Cantonal Courts - Direct Scrapers
    # =========================================================================

    # 7. Zürich Courts (direct scraper)
    step += 1
    print(f"\n[{step}/{total_steps}] Zürich Courts (Obergericht)...")
    try:
        from scripts.scrape_zh_courts import scrape_zh_courts
        results["zh"] = run_scraper("zh", scrape_zh_courts, from_date, to_date=to_date)
    except ImportError as e:
        results["zh"] = {"status": "error", "count": 0, "error": str(e)}

    # 8. Zürich Steuerrekurs (Tax Appeals)
    step += 1
    print(f"\n[{step}/{total_steps}] Zürich Steuerrekursgericht...")
    try:
        from scripts.scrape_zh_steuerrekurs import scrape_zh_steuerrekurs
        results["zh_steuerrekurs"] = run_scraper("zh_steuerrekurs", scrape_zh_steuerrekurs, from_date, to_date=to_date)
    except ImportError as e:
        results["zh_steuerrekurs"] = {"status": "error", "count": 0, "error": str(e)}

    # 9. Zürich Baurekurs (Construction Appeals)
    step += 1
    print(f"\n[{step}/{total_steps}] Zürich Baurekursgericht...")
    try:
        from scripts.scrape_zh_baurekurs import scrape_zh_baurekurs
        results["zh_baurekurs"] = run_scraper("zh_baurekurs", scrape_zh_baurekurs, from_date, to_date=to_date)
    except ImportError as e:
        results["zh_baurekurs"] = {"status": "error", "count": 0, "error": str(e)}

    # 10. Zürich Sozialversicherung (Social Insurance)
    step += 1
    print(f"\n[{step}/{total_steps}] Zürich Sozialversicherungsgericht...")
    try:
        from scripts.scrape_zh_sozialversicherung import scrape_zh_sozialversicherung
        results["zh_sozialversicherung"] = run_scraper("zh_sozialversicherung", scrape_zh_sozialversicherung, from_date, to_date=to_date)
    except ImportError as e:
        results["zh_sozialversicherung"] = {"status": "error", "count": 0, "error": str(e)}

    # 11. Geneva Courts (direct scraper from official portal)
    step += 1
    print(f"\n[{step}/{total_steps}] Geneva Courts...")
    try:
        from scripts.scrape_cantons import scrape_ge_crawler
        results["ge"] = run_scraper("ge", lambda from_date, **kw: scrape_ge_crawler(from_date=from_date), from_date, to_date=to_date)
    except ImportError as e:
        results["ge"] = {"status": "error", "count": 0, "error": str(e)}

    # 12. Vaud Courts (direct scraper from official portal)
    step += 1
    print(f"\n[{step}/{total_steps}] Vaud Courts...")
    try:
        from scripts.scrape_cantons import scrape_vd_crawler
        results["vd"] = run_scraper("vd", lambda from_date, **kw: scrape_vd_crawler(from_date=from_date), from_date, to_date=to_date)
    except ImportError as e:
        results["vd"] = {"status": "error", "count": 0, "error": str(e)}

    # 13. Ticino Courts (direct scraper from official portal)
    step += 1
    print(f"\n[{step}/{total_steps}] Ticino Courts...")
    try:
        from scripts.scrape_cantons import scrape_ti_crawler
        results["ti"] = run_scraper("ti", lambda from_date, **kw: scrape_ti_crawler(from_date=from_date), from_date, to_date=to_date)
    except ImportError as e:
        results["ti"] = {"status": "error", "count": 0, "error": str(e)}

    # 14. Other Cantons (scrape_cantons.py)
    step += 1
    print(f"\n[{step}/{total_steps}] Other Cantonal Courts...")
    try:
        from scripts.scrape_cantons import scrape_all_cantons
        results["cantons"] = run_scraper("cantons", scrape_all_cantons, from_date, to_date=to_date)
    except ImportError as e:
        results["cantons"] = {"status": "error", "count": 0, "error": str(e)}

    # =========================================================================
    # Legacy: entscheidsuche.ch (no longer used - all cantons have direct scrapers)
    # =========================================================================

    # Note: entscheidsuche.ch import is deprecated. All cantons now have direct
    # scrapers accessing official court portals. Keep this code path for potential
    # gap verification only.
    if not skip_entscheidsuche:
        step += 1
        print(f"\n[{step}/{total_steps}] [DEPRECATED] entscheidsuche.ch gap verification...")
        try:
            from scripts.import_entscheidsuche import import_entscheidsuche
            # Import with skip_federal since we already did those
            results["entscheidsuche"] = run_scraper(
                "entscheidsuche",
                lambda from_date: import_entscheidsuche(skip_federal=True),
                from_date
            )
        except ImportError as e:
            results["entscheidsuche"] = {"status": "error", "count": 0, "error": str(e)}
    else:
        # Default: skip entscheidsuche.ch since we have direct scrapers for all cantons
        results["entscheidsuche"] = {"status": "skipped", "count": 0, "note": "direct scrapers used"}

    return results


def push_to_huggingface(repo_id: str = "voilaj/swiss-caselaw") -> bool:
    """Push updated dataset to HuggingFace."""
    token = os.environ.get("HF_TOKEN")
    if not token:
        print("Warning: HF_TOKEN not set, skipping push")
        return False

    try:
        from scripts.push_to_huggingface import push_to_huggingface as push_hf
        push_hf(repo_id)
        return True
    except Exception as e:
        print(f"Push failed: {e}")
        return False


def print_summary(results: dict[str, dict], before_stats: dict, after_stats: dict) -> None:
    """Print a formatted summary of the update."""
    total_added = after_stats["total"] - before_stats["total"]
    total_imported = sum(r.get("count", 0) for r in results.values())
    errors = [name for name, r in results.items() if r.get("status") in ("error", "timeout")]

    print(f"\n{'='*60}")
    print("UPDATE SUMMARY")
    print("="*60)

    # Federal courts and bodies
    print("\nFederal Courts & Bodies:")
    for name in ["bger", "bvger", "bstger", "bpatger", "weko", "edoeb"]:
        if name in results:
            r = results[name]
            status = "OK" if r["status"] == "success" else ("TIMEOUT" if r["status"] == "timeout" else "FAILED")
            count = r.get("count", 0)
            print(f"  {name.upper():12} [{status}]: {count:,} imported")

    # Zürich specialized courts
    print("\nZürich Courts:")
    for name in ["zh", "zh_steuerrekurs", "zh_baurekurs", "zh_sozialversicherung"]:
        if name in results:
            r = results[name]
            status = "OK" if r["status"] == "success" else ("TIMEOUT" if r["status"] == "timeout" else "FAILED")
            count = r.get("count", 0)
            print(f"  {name:20} [{status}]: {count:,} imported")

    # Other cantonal courts
    print("\nOther Cantonal Courts:")
    for name in ["ge", "vd", "ti", "cantons", "entscheidsuche"]:
        if name in results:
            r = results[name]
            status = "OK" if r["status"] == "success" else "FAILED"
            count = r.get("count", 0)
            print(f"  {name:12} [{status}]: {count:,} imported")

    # Totals
    print(f"\n{'-'*60}")
    print(f"Total imported this run: {total_imported:,}")
    print(f"Net change in DB:        {total_added:,}")
    print(f"\nDatabase total: {after_stats['total']:,} decisions")
    print(f"  - Federal:  {after_stats['federal']:,}")
    print(f"  - Cantonal: {after_stats['cantonal']:,}")

    if errors:
        print(f"\nErrors occurred in: {', '.join(errors)}")


def main():
    parser = argparse.ArgumentParser(description="Daily incremental update")
    parser.add_argument("--days", type=int, default=7, help="Days to look back (default: 7)")
    parser.add_argument("--push", action="store_true", help="Push to HuggingFace after update")
    parser.add_argument("--repo", default="voilaj/swiss-caselaw", help="HuggingFace repo ID")
    parser.add_argument("--full", action="store_true", help="Include slow entscheidsuche.ch import (700K+ decisions)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    # Get initial stats
    before_stats = get_stats()
    print(f"Before: {before_stats['total']:,} decisions ({before_stats['federal']:,} federal, {before_stats['cantonal']:,} cantonal)")

    # Run incremental scrapers
    results = run_incremental_scrapers(days=args.days, skip_entscheidsuche=not args.full)

    # Get final stats
    after_stats = get_stats()

    # Print summary
    print_summary(results, before_stats, after_stats)

    # Push to HuggingFace if requested
    if args.push:
        print(f"\n{'='*60}")
        print("PUSHING TO HUGGINGFACE")
        print("="*60)
        if push_to_huggingface(args.repo):
            print(f"Successfully pushed to {args.repo}")
        else:
            print("Push failed or skipped")


if __name__ == "__main__":
    main()
