"""Add GIN index for full-text search

Revision ID: 0002
Revises: 0001
Create Date: 2026-01-28

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create GIN index for full-text search on decisions
    # This significantly speeds up FTS queries on title, docket, and content
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_decisions_fts ON decisions USING gin (
            (
                setweight(to_tsvector('simple', coalesce(title, '')), 'A') ||
                setweight(to_tsvector('simple', coalesce(docket, '')), 'A') ||
                setweight(to_tsvector('simple', substr(content_text, 1, 50000)), 'D')
            )
        )
    """)

    # Add index for faster canton filtering with FTS
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_decisions_canton_date ON decisions (canton, decision_date DESC NULLS LAST)
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_decisions_fts")
    op.execute("DROP INDEX IF EXISTS idx_decisions_canton_date")
