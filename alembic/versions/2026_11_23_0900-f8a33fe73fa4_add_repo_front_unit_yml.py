"""Store repository search metadata from the front Unit manifest."""

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from alembic import op

revision = "f8a33fe73fa4"
down_revision = "e4b7a29c610f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "repo",
        sa.Column(
            "is_have_front_unit_yml",
            sa.Boolean(),
            server_default=sa.false(),
            nullable=False,
        ),
    )
    op.add_column(
        "repo", sa.Column("front_unit_yml_search", JSONB(), nullable=True)
    )


def downgrade() -> None:
    op.drop_column("repo", "front_unit_yml_search")
    op.drop_column("repo", "is_have_front_unit_yml")
