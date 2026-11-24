import sqlalchemy as sa

from alembic import op

revision = "bdc9173e69d4"
down_revision = "f8a33fe73fa4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "units_io",
        sa.Column("last_message_error", sa.String(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("units_io", "last_message_error")
