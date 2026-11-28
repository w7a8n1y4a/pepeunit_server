from alembic import op

revision = "b8ea72ce41c6"
down_revision = "d46b814027af"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE units_io
        SET topic_name = 'io_topics_update'
        WHERE type = 'Input' AND topic_name = 'schema_update'
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE units_io
        SET topic_name = 'schema_update'
        WHERE type = 'Input' AND topic_name = 'io_topics_update'
        """
    )
