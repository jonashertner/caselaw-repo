from __future__ import annotations

import datetime as dt


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def parse_date(date_str: str) -> dt.date:
    return dt.date.fromisoformat(date_str)


def iso_week(date: dt.date | None = None) -> str:
    if date is None:
        date = dt.date.today()
    year, week, _ = date.isocalendar()
    return f"{year}-W{week:02d}"
