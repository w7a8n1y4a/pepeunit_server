"""Require unique IO names within a Unit."""

from alembic import op

revision = "e4b7a29c610f"
down_revision = "c3e8a14b70f2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_unique_constraint(
        "units_io_unit_topic_key", "units_io", ["unit_uuid", "topic_name"]
    )


def downgrade() -> None:
    op.drop_constraint("units_io_unit_topic_key", "units_io", type_="unique")
