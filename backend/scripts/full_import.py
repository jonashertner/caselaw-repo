#!/usr/bin/env python3
"""Full historical import - gather ALL decisions from all sources.

This script runs all scrapers in HISTORICAL MODE to build a complete database.
Historical mode disables date filtering, enabling complete archive capture.

Run this once to initialize the database, then use daily_update.py for incremental updates.

Usage:
    python scripts/full_import.py [--source SOURCE] [--skip-entscheidsuche]
    python scripts/full_import.py --list  # List available sources

Historical Mode:
    All scrapers are run with historical=True, which:
    - Disables date filtering (from_date=None, to_date=None)
    - Fetches the complete archive from each source
    - Suitable for initial database population or archive refresh
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlmodel import select, func
from app.db.session import get_session
from app.models.decision import Decision

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


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

        # By canton
        canton_query = (
            select(Decision.canton, func.count(Decision.id))
            .where(Decision.canton.isnot(None))
            .group_by(Decision.canton)
        )
        cantons = dict(session.exec(canton_query).all())

        return {
            "total": total,
            "federal": federal,
            "cantonal": cantonal,
            "by_canton": cantons,
        }


def print_stats(label: str, stats: dict) -> None:
    """Print statistics."""
    print(f"\n{'='*60}")
    print(f"{label}")
    print("="*60)
    print(f"Total:    {stats['total']:,}")
    print(f"Federal:  {stats['federal']:,}")
    print(f"Cantonal: {stats['cantonal']:,}")
    if stats.get('by_canton'):
        print(f"\nBy Canton ({len(stats['by_canton'])} cantons):")
        for canton, count in sorted(stats['by_canton'].items(), key=lambda x: -x[1])[:10]:
            print(f"  {canton}: {count:,}")
        if len(stats['by_canton']) > 10:
            print(f"  ... and {len(stats['by_canton']) - 10} more")


def run_source(name: str, func, **kwargs) -> int:
    """Run a single source importer."""
    print(f"\n{'-'*60}")
    print(f"IMPORTING: {name}")
    print("-"*60)

    start = time.time()
    try:
        count = func(**kwargs) or 0
        elapsed = time.time() - start
        print(f"{name}: Imported {count:,} decisions in {elapsed:.1f}s")
        return count
    except Exception as e:
        elapsed = time.time() - start
        logger.exception(f"{name} failed after {elapsed:.1f}s")
        print(f"{name}: FAILED - {e}")
        return 0


def import_federal_courts() -> dict[str, int]:
    """Import from all federal court scrapers in historical mode."""
    results = {}

    print("\n" + "="*60)
    print("FEDERAL COURTS - Historical Import")
    print("="*60)

    # 1. BGer - Federal Supreme Court
    try:
        from scripts.scrape_bger import scrape_bger
        results["bger"] = run_source(
            "Bundesgericht (BGer)",
            scrape_bger,
            from_date=None,
        )
    except ImportError as e:
        logger.error(f"BGer scraper not available: {e}")
        results["bger"] = 0

    # 2. BVGer - Federal Administrative Court (direct scraper)
    try:
        from scripts.scrape_bvger_direct import scrape_bvger_direct
        results["bvger"] = run_source(
            "Bundesverwaltungsgericht (BVGer)",
            scrape_bvger_direct,
            from_date=None,
        )
    except ImportError as e:
        logger.error(f"BVGer scraper not available: {e}")
        results["bvger"] = 0

    # 3. BStGer - Federal Criminal Court (direct scraper)
    try:
        from scripts.scrape_bstger_direct import scrape_bstger_direct
        results["bstger"] = run_source(
            "Bundesstrafgericht (BStGer)",
            scrape_bstger_direct,
            from_date=None,
        )
    except ImportError as e:
        logger.error(f"BStGer scraper not available: {e}")
        results["bstger"] = 0

    # 4. BPatGer - Federal Patent Court
    try:
        from scripts.scrape_bpatger import scrape_bpatger
        results["bpatger"] = run_source(
            "Bundespatentgericht (BPatGer)",
            scrape_bpatger,
            from_date=None,
        )
    except ImportError as e:
        logger.error(f"BPatGer scraper not available: {e}")
        results["bpatger"] = 0

    # 5. WEKO - Competition Commission
    try:
        from scripts.scrape_weko import scrape_weko
        results["weko"] = run_source(
            "Wettbewerbskommission (WEKO)",
            scrape_weko,
            from_date=None,
        )
    except ImportError as e:
        logger.error(f"WEKO scraper not available: {e}")
        results["weko"] = 0

    # 6. EDÖB - Data Protection Commissioner
    try:
        from scripts.scrape_edoeb import scrape_edoeb
        results["edoeb"] = run_source(
            "EDÖB (Datenschutzbeauftragter)",
            scrape_edoeb,
            from_date=None,
        )
    except ImportError as e:
        logger.error(f"EDÖB scraper not available: {e}")
        results["edoeb"] = 0

    return results


def import_cantonal_courts() -> dict[str, int]:
    """Import from all cantonal court scrapers in historical mode."""
    results = {}

    print("\n" + "="*60)
    print("CANTONAL COURTS - Historical Import")
    print("="*60)

    # 1. Zürich Courts (main)
    try:
        from scripts.scrape_zh_courts import scrape_zh_courts
        results["zh"] = run_source(
            "Zürich Courts (Obergericht)",
            scrape_zh_courts,
            from_date=None,
        )
    except ImportError as e:
        logger.error(f"ZH scraper not available: {e}")
        results["zh"] = 0

    # 2. Zürich Steuerrekurs (Tax Appeals)
    try:
        from scripts.scrape_zh_steuerrekurs import scrape_zh_steuerrekurs
        results["zh_steuerrekurs"] = run_source(
            "Zürich Steuerrekursgericht",
            scrape_zh_steuerrekurs,
            from_date=None,
        )
    except ImportError as e:
        logger.error(f"ZH Steuerrekurs scraper not available: {e}")
        results["zh_steuerrekurs"] = 0

    # 3. Zürich Baurekurs (Construction Appeals)
    try:
        from scripts.scrape_zh_baurekurs import scrape_zh_baurekurs
        results["zh_baurekurs"] = run_source(
            "Zürich Baurekursgericht",
            scrape_zh_baurekurs,
            from_date=None,
        )
    except ImportError as e:
        logger.error(f"ZH Baurekurs scraper not available: {e}")
        results["zh_baurekurs"] = 0

    # 4. Zürich Sozialversicherung (Social Insurance)
    try:
        from scripts.scrape_zh_sozialversicherung import scrape_zh_sozialversicherung
        results["zh_sozialversicherung"] = run_source(
            "Zürich Sozialversicherungsgericht",
            scrape_zh_sozialversicherung,
            from_date=None,
        )
    except ImportError as e:
        logger.error(f"ZH Sozialversicherung scraper not available: {e}")
        results["zh_sozialversicherung"] = 0

    # 5. Geneva Courts (direct scraper)
    try:
        from scripts.scrape_ge_direct import scrape_ge_direct
        results["ge"] = run_source(
            "Geneva Courts",
            scrape_ge_direct,
            from_date=None,
        )
    except ImportError as e:
        logger.error(f"GE scraper not available: {e}")
        results["ge"] = 0

    # 6. Vaud Courts
    try:
        from scripts.scrape_vd import scrape_vd
        results["vd"] = run_source(
            "Vaud Courts",
            scrape_vd,
            from_date=None,
        )
    except ImportError as e:
        logger.error(f"VD scraper not available: {e}")
        results["vd"] = 0

    # 7. Ticino Courts
    try:
        from scripts.scrape_ti import scrape_ti
        results["ti"] = run_source(
            "Ticino Courts",
            scrape_ti,
            from_date=None,
        )
    except ImportError as e:
        logger.error(f"TI scraper not available: {e}")
        results["ti"] = 0

    # 8. All other cantons (scrape_cantons.py)
    try:
        from scripts.scrape_cantons import scrape_all_cantons
        results["cantons"] = run_source(
            "Other Cantonal Courts (BE, LU, AG, SG, etc.)",
            scrape_all_cantons,
            from_date=None,
        )
    except ImportError as e:
        logger.error(f"Cantons scraper not available: {e}")
        results["cantons"] = 0

    return results


def import_entscheidsuche_full() -> int:
    """Import ALL decisions from entscheidsuche.ch (700K+)."""
    from scripts.import_entscheidsuche import import_entscheidsuche
    return import_entscheidsuche(canton=None, limit=None, skip_federal=False)


SOURCES = {
    "federal": ("All Federal Courts", import_federal_courts),
    "cantonal": ("All Cantonal Courts", import_cantonal_courts),
    "entscheidsuche": ("entscheidsuche.ch (700K+ decisions)", import_entscheidsuche_full),
}


def list_sources():
    """List available import sources."""
    print("Available import sources:")
    print("-" * 60)
    for key, (name, _) in SOURCES.items():
        print(f"  {key:20} - {name}")
    print()
    print("Run with --source <name> to import a specific source")
    print("Run without --source to import ALL sources")


def main():
    parser = argparse.ArgumentParser(
        description="Full historical import - fetch ALL decisions from all sources",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Historical Mode:
  This script runs in HISTORICAL MODE by default, which:
  - Disables date filtering (fetches all decisions regardless of date)
  - Enables complete archive capture from each source
  - Suitable for initial database population or archive refresh

  Use daily_update.py for incremental updates with date filtering.
        """,
    )
    parser.add_argument("--source", help="Specific source to import (federal, cantonal, entscheidsuche)")
    parser.add_argument("--skip-entscheidsuche", action="store_true",
                       help="Skip entscheidsuche.ch (700K+ records, very slow)")
    parser.add_argument("--list", action="store_true", help="List available sources")
    args = parser.parse_args()

    if args.list:
        list_sources()
        return

    # Get initial stats
    before_stats = get_stats()
    print_stats("BEFORE IMPORT", before_stats)

    start_time = time.time()
    all_results = {}

    if args.source:
        # Run specific source
        if args.source not in SOURCES:
            print(f"Unknown source: {args.source}")
            list_sources()
            return

        name, func = SOURCES[args.source]
        result = func()
        if isinstance(result, dict):
            all_results.update(result)
        else:
            all_results[args.source] = result
    else:
        # Run ALL sources (except entscheidsuche if skipped)
        print("\n" + "="*60)
        print("FULL HISTORICAL IMPORT")
        print("="*60)
        print("\nThis will import ALL available decisions from all scrapers.")
        print("This may take several hours.")
        print()

        # 1. Federal courts
        print("\n[1/3] Importing from Federal Court scrapers...")
        federal_results = import_federal_courts()
        all_results.update(federal_results)

        # 2. Cantonal courts
        print("\n[2/3] Importing from Cantonal Court scrapers...")
        cantonal_results = import_cantonal_courts()
        all_results.update(cantonal_results)

        # 3. entscheidsuche.ch (optional, very slow)
        if not args.skip_entscheidsuche:
            print("\n[3/3] Importing from entscheidsuche.ch (main source - 700K+ decisions)...")
            all_results["entscheidsuche"] = run_source(
                "entscheidsuche.ch",
                import_entscheidsuche_full,
            )
        else:
            print("\n[3/3] Skipping entscheidsuche.ch (--skip-entscheidsuche)")
            all_results["entscheidsuche"] = 0

    # Get final stats
    after_stats = get_stats()
    elapsed = time.time() - start_time

    # Print summary
    print("\n" + "="*60)
    print("IMPORT COMPLETE")
    print("="*60)
    print(f"\nTotal time: {elapsed/60:.1f} minutes ({elapsed/3600:.1f} hours)")
    print(f"\nResults by source:")
    for source, count in sorted(all_results.items(), key=lambda x: -x[1]):
        status = "OK" if count > 0 else "NONE"
        print(f"  {source:24} [{status}]: {count:,}")

    total_imported = sum(all_results.values())
    net_added = after_stats["total"] - before_stats["total"]

    print(f"\nTotal imported this run: {total_imported:,}")
    print(f"Net new decisions added: {net_added:,}")

    print_stats("AFTER IMPORT", after_stats)

    # Coverage summary
    print("\n" + "="*60)
    print("COVERAGE SUMMARY")
    print("="*60)
    print(f"Federal courts: {after_stats['federal']:,} decisions")
    print(f"Cantonal courts: {after_stats['cantonal']:,} decisions")
    print(f"Cantons covered: {len(after_stats['by_canton'])}/26")

    missing_cantons = set([
        "AG", "AI", "AR", "BE", "BL", "BS", "FR", "GE", "GL", "GR",
        "JU", "LU", "NE", "NW", "OW", "SG", "SH", "SO", "SZ", "TG",
        "TI", "UR", "VD", "VS", "ZG", "ZH"
    ]) - set(after_stats['by_canton'].keys())

    if missing_cantons:
        print(f"Missing cantons: {', '.join(sorted(missing_cantons))}")
    else:
        print("All 26 cantons covered!")


if __name__ == "__main__":
    main()
