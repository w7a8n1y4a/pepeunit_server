from alembic import op

revision = "d46b814027af"
down_revision = "a269c5f2e814"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_notifications_type_create_datetime",
        "notifications",
        ["type", "create_datetime"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_notifications_type_create_datetime",
        table_name="notifications",
    )
