"""init

Revision ID: 0001
Revises: 
Create Date: 2026-01-27

"""

from alembic import op
import sqlalchemy as sa
from pgvector.sqlalchemy import Vector

# revision identifiers, used by Alembic.
revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    op.create_table(
        "decisions",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("source_id", sa.String(), nullable=False, index=True),
        sa.Column("source_name", sa.String(), nullable=False, index=True),
        sa.Column("level", sa.String(), nullable=False, index=True),
        sa.Column("canton", sa.String(), nullable=True, index=True),
        sa.Column("court", sa.String(), nullable=True, index=True),
        sa.Column("chamber", sa.String(), nullable=True, index=True),
        sa.Column("docket", sa.String(), nullable=True, index=True),
        sa.Column("decision_date", sa.Date(), nullable=True, index=True),
        sa.Column("published_date", sa.Date(), nullable=True, index=True),
        sa.Column("title", sa.String(), nullable=True),
        sa.Column("language", sa.String(), nullable=True, index=True),
        sa.Column("url", sa.String(), nullable=False, unique=True, index=True),
        sa.Column("pdf_url", sa.String(), nullable=True),
        sa.Column("content_text", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.String(), nullable=False, index=True),
        sa.Column("meta", sa.dialects.postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    )

    op.create_table(
        "chunks",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("decision_id", sa.String(), sa.ForeignKey("decisions.id"), nullable=False, index=True),
        sa.Column("chunk_index", sa.Integer(), nullable=False, index=True),
        sa.Column("text", sa.Text(), nullable=False),
        sa.Column("embedding", Vector(3072), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("chunks")
    op.drop_table("decisions")
