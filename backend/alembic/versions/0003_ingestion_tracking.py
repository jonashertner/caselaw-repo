"""Add ingestion tracking: timestamps on decisions + ingestion_runs table.

Revision ID: 0003
Revises: 0002
Create Date: 2025-01-28
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add timestamps to decisions table
    op.add_column(
        "decisions",
        sa.Column(
            "indexed_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=True,
        ),
    )
    op.add_column(
        "decisions",
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=True),
    )
    op.create_index("idx_decisions_indexed_at", "decisions", ["indexed_at"])

    # Create ingestion_runs table
    op.create_table(
        "ingestion_runs",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("scraper_name", sa.String(), nullable=False, index=True),
        sa.Column("source_id", sa.String(), nullable=True, index=True),
        sa.Column(
            "started_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("completed_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("duration_seconds", sa.Float(), nullable=True),
        sa.Column("status", sa.String(), nullable=False, default="running", index=True),
        sa.Column("decisions_found", sa.Integer(), default=0),
        sa.Column("decisions_imported", sa.Integer(), default=0),
        sa.Column("decisions_skipped", sa.Integer(), default=0),
        sa.Column("decisions_updated", sa.Integer(), default=0),
        sa.Column("errors", sa.Integer(), default=0),
        sa.Column("from_date", sa.Date(), nullable=True),
        sa.Column("to_date", sa.Date(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("details", postgresql.JSONB(), nullable=False, server_default="{}"),
    )


def downgrade() -> None:
    op.drop_table("ingestion_runs")
    op.drop_index("idx_decisions_indexed_at", table_name="decisions")
    op.drop_column("decisions", "updated_at")
    op.drop_column("decisions", "indexed_at")
