"""Add instance resource limits."""

import sqlalchemy as sa

from alembic import op

revision = "a269c5f2e814"
down_revision = "bdc9173e69d4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for name, default in (
        ("pu_instance_pg_size_limit", 2**30),
        ("pu_instance_cc_size_limit", 2**30),
        ("pu_instance_network_speed_limit", 2**24),
        ("pu_instance_network_monthly_traffic_limit", 3 * 2**40),
    ):
        op.add_column(
            "instance_settings",
            sa.Column(
                name,
                sa.BigInteger(),
                nullable=False,
                server_default=sa.text(str(default)),
            ),
        )
    op.add_column(
        "instance_settings",
        sa.Column(
            "pu_instance_network_traffic_reset_day",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("1"),
        ),
    )
    op.create_check_constraint(
        "instance_settings_resource_limits_check",
        "instance_settings",
        "pu_instance_pg_size_limit > 0"
        " AND pu_instance_cc_size_limit > 0"
        " AND pu_instance_network_speed_limit > 0"
        " AND pu_instance_network_monthly_traffic_limit > 0"
        " AND pu_instance_network_traffic_reset_day BETWEEN 1 AND 31",
    )


def downgrade() -> None:
    op.drop_constraint(
        "instance_settings_resource_limits_check",
        "instance_settings",
        type_="check",
    )
    for name in (
        "pu_instance_network_traffic_reset_day",
        "pu_instance_network_monthly_traffic_limit",
        "pu_instance_network_speed_limit",
        "pu_instance_cc_size_limit",
        "pu_instance_pg_size_limit",
    ):
        op.drop_column("instance_settings", name)
