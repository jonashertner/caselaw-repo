"""Shared fixtures for the Swiss caselaw test suite."""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, List

import pytest

import sys
from pathlib import Path

# Ensure project packages are importable
ROOT = Path(__file__).resolve().parent.parent
for sub in ("pipeline", "local_app", "mcp_server"):
    p = ROOT / sub
    if p.exists() and str(p) not in sys.path:
        sys.path.insert(0, str(p))


SAMPLE_DECISIONS: List[Dict[str, Any]] = [
    {
        "id": "bge-140-iii-264",
        "source_id": "bger-bge-140-iii-264",
        "source_name": "Bundesgericht",
        "level": "federal",
        "canton": None,
        "court": "Bundesgericht",
        "chamber": "I. zivilrechtliche Abteilung",
        "language": "de",
        "docket": "4A_541/2013",
        "decision_date": "2024-03-15",
        "published_date": "2024-04-01",
        "title": "Arbeitsrecht: Fristlose Kündigung wegen Datenschutzverletzung",
        "url": "https://example.com/bge-140-iii-264",
        "pdf_url": "https://example.com/bge-140-iii-264.pdf",
        "content_text": "Das Bundesgericht hat entschieden, dass die fristlose Kündigung "
                        "wegen schwerer Verletzung der Datenschutzpflichten gerechtfertigt war. "
                        "Der Arbeitnehmer hatte vertrauliche Kundendaten an Dritte weitergegeben. "
                        "BGE 140 III 264 bestätigt die strenge Haltung des Bundesgerichts.",
        "content_sha256": None,
        "fetched_at": "2024-03-20T10:00:00+00:00",
        "updated_at": "2024-03-20T10:00:00+00:00",
    },
    {
        "id": "zh-obergericht-2024-001",
        "source_id": "zh-oger-2024-001",
        "source_name": "Obergericht Zürich",
        "level": "cantonal",
        "canton": "ZH",
        "court": "Obergericht",
        "chamber": "II. Zivilkammer",
        "language": "de",
        "docket": "LB230045",
        "decision_date": "2024-01-20",
        "published_date": "2024-02-15",
        "title": "Mietrecht: Kündigung wegen Eigenbedarf",
        "url": "https://example.com/zh-oger-2024-001",
        "pdf_url": None,
        "content_text": "Das Obergericht Zürich bestätigte die Kündigung wegen Eigenbedarf. "
                        "Die Vermieterin konnte glaubhaft darlegen, dass sie die Wohnung "
                        "für ihren Sohn benötigt. Steuerrecht spielt keine Rolle.",
        "content_sha256": None,
        "fetched_at": "2024-02-20T10:00:00+00:00",
        "updated_at": "2024-02-20T10:00:00+00:00",
    },
    {
        "id": "vd-tribunal-cantonal-2023-042",
        "source_id": "vd-tc-2023-042",
        "source_name": "Tribunal cantonal vaudois",
        "level": "cantonal",
        "canton": "VD",
        "court": "Tribunal cantonal",
        "chamber": "Cour d'appel civile",
        "language": "fr",
        "docket": "CALC/2023/42",
        "decision_date": "2023-11-10",
        "published_date": "2023-12-01",
        "title": "Droit du travail: licenciement immédiat pour violation du devoir de fidélité",
        "url": "https://example.com/vd-tc-2023-042",
        "pdf_url": "https://example.com/vd-tc-2023-042.pdf",
        "content_text": "Le Tribunal cantonal a confirmé le licenciement immédiat de l'employé "
                        "pour violation grave du devoir de fidélité. L'employé avait divulgué "
                        "des informations confidentielles à un concurrent. "
                        "La protection des données est un enjeu majeur.",
        "content_sha256": None,
        "fetched_at": "2023-12-05T10:00:00+00:00",
        "updated_at": "2023-12-05T10:00:00+00:00",
    },
    {
        "id": "ti-tribunale-2023-099",
        "source_id": "ti-tca-2023-099",
        "source_name": "Tribunale d'appello del Cantone Ticino",
        "level": "cantonal",
        "canton": "TI",
        "court": "Tribunale d'appello",
        "chamber": "II Camera civile",
        "language": "it",
        "docket": "TCA/2023/99",
        "decision_date": "2023-09-05",
        "published_date": "2023-10-01",
        "title": "Diritto del lavoro: licenziamento immediato per furto",
        "url": "https://example.com/ti-tca-2023-099",
        "pdf_url": None,
        "content_text": "Il Tribunale d'appello ha confermato il licenziamento immediato "
                        "per furto nel luogo di lavoro. Il dipendente aveva sottratto "
                        "materiale dell'azienda per un valore considerevole.",
        "content_sha256": None,
        "fetched_at": "2023-10-10T10:00:00+00:00",
        "updated_at": "2023-10-10T10:00:00+00:00",
    },
    {
        "id": "bger-5a-200-2024",
        "source_id": "bger-5a-200-2024",
        "source_name": "Bundesgericht",
        "level": "federal",
        "canton": "BE",
        "court": "Bundesgericht",
        "chamber": "II. zivilrechtliche Abteilung",
        "language": "de",
        "docket": "5A_200/2024",
        "decision_date": "2024-06-01",
        "published_date": "2024-06-20",
        "title": "Familienrecht: Scheidung und Unterhaltsbeiträge",
        "url": "https://example.com/bger-5a-200-2024",
        "pdf_url": "https://example.com/bger-5a-200-2024.pdf",
        "content_text": "Das Bundesgericht hat die Beschwerde betreffend Unterhaltsbeiträge "
                        "nach Scheidung teilweise gutgeheissen. Der Unterhalt wurde neu "
                        "auf Basis des tatsächlichen Einkommens berechnet. Datenschutz "
                        "spielt auch bei Familienrechtsfällen eine zunehmende Rolle.",
        "content_sha256": None,
        "fetched_at": "2024-06-25T10:00:00+00:00",
        "updated_at": "2024-06-25T10:00:00+00:00",
    },
]


def _insert_decisions(conn: sqlite3.Connection, decisions: List[Dict[str, Any]]) -> None:
    """Insert decisions into a database with schema already created."""
    from caselaw_local.db import DECISION_COLS

    cols = DECISION_COLS
    placeholders = ",".join(["?"] * len(cols))
    sql = f"INSERT OR REPLACE INTO decisions ({','.join(cols)}) VALUES ({placeholders})"

    for d in decisions:
        row = [d.get(c) for c in cols]
        conn.execute(sql, row)
    conn.commit()

    # Rebuild FTS
    conn.execute("INSERT INTO decisions_fts(decisions_fts) VALUES('rebuild');")
    conn.commit()


@pytest.fixture
def db_with_decisions() -> sqlite3.Connection:
    """In-memory SQLite DB with schema + 5 sample decisions + FTS index."""
    from caselaw_local.db import ensure_schema

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    ensure_schema(conn)
    _insert_decisions(conn, SAMPLE_DECISIONS)
    yield conn
    conn.close()


@pytest.fixture
def empty_db() -> sqlite3.Connection:
    """In-memory SQLite DB with schema but no data."""
    from caselaw_local.db import ensure_schema

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    ensure_schema(conn)
    yield conn
    conn.close()


@pytest.fixture
def sample_decision() -> Dict[str, Any]:
    """Return a single sample decision dict."""
    return dict(SAMPLE_DECISIONS[0])
