from __future__ import annotations

import datetime as dt
import uuid as uuid_pkg
from typing import Optional

from sqlmodel import Field, SQLModel


class TimestampedModel(SQLModel):
    created_at: dt.datetime = Field(default_factory=lambda: dt.datetime.now(dt.timezone.utc), nullable=False)
    updated_at: dt.datetime = Field(default_factory=lambda: dt.datetime.now(dt.timezone.utc), nullable=False)


def uuid4_str() -> str:
    return str(uuid_pkg.uuid4())
