from calendar import monthrange
from contextlib import ExitStack
from datetime import UTC, datetime, timedelta
from math import isfinite

import pytest

from app.configs.error_codes import ErrorCode
from app.configs.errors import InstanceError, NoAccessError
from app.dto.agent.abc import AgentUser
from app.dto.enum import InstanceResourceMetric, NotificationType
from app.schemas.pydantic.notification import NotificationFilter
from app.utils.utils import ensure_timezone_aware
from tests.integration.helpers.instance_settings import owner_setting
from tests.integration.helpers.notifications import (
    as_recipient,
    notification_was_delivered,
    use_configured_test_telegram_chat,
    wait_for_resource_alert_window,
)
from tests.integration.helpers.owner import as_owner
from tests.integration.helpers.services import (
    instance_resources_service,
    notification_service,
)
from tests.integration.helpers.wait import wait_until


def test_resource_endpoints_require_owner(
    regular_user_token, admin_user_token, database, cc
) -> None:
    now = datetime.now(UTC)
    start = now - timedelta(minutes=5)

    for token in (None, regular_user_token, admin_user_token):
        service = instance_resources_service(database, cc, token)
        with pytest.raises(NoAccessError):
            service.history(start, now)
        with pytest.raises(NoAccessError):
            service.network_traffic()


def test_resource_history_rejects_invalid_period(
    extra_user, extra_user_token, database, cc
) -> None:
    now = datetime.now(UTC)
    with as_owner(database, extra_user):
        service = instance_resources_service(database, cc, extra_user_token)

        with pytest.raises(InstanceError) as error:
            service.history(now, now)

        assert (
            error.value.error_code
            == ErrorCode.INSTANCE_RESOURCE_PERIOD_INVALID
        )


def test_resource_history_uses_live_samples(
    extra_user, extra_user_token, database, cc
) -> None:
    with as_owner(database, extra_user):
        service = instance_resources_service(database, cc, extra_user_token)
        started = datetime.now(UTC).replace(second=0, microsecond=0)
        outside_retention = started - timedelta(days=91)
        assert (
            service.history(
                outside_retention - timedelta(days=1), outside_retention
            )
            == []
        )

        wait_until(
            lambda: bool(service.history(started, datetime.now(UTC))),
            timeout=150,
            interval=2,
            message="no live Logporter resource sample reached ClickHouse",
        )

        sample = service.history(started, datetime.now(UTC))[-1]
        timestamp = sample.create_datetime
        assert started <= timestamp <= datetime.now(UTC)
        assert timestamp.second == 0
        assert sample.pu_instance_network_speed_limit > 0
        assert sample.pg_disk_percent > 0
        for percent in (
            sample.cpu_percent,
            sample.ram_percent,
            sample.network_width_percent,
            sample.pg_disk_percent,
            sample.cc_disk_percent,
        ):
            assert isfinite(percent)
            assert percent >= 0

        second = timedelta(seconds=1)
        assert service.history(timestamp, timestamp + second) == [sample]
        assert service.history(timestamp - second, timestamp) == []


def test_monthly_traffic_respects_instance_limits(
    extra_user, extra_user_token, database, cc
) -> None:
    with (
        owner_setting(
            database,
            extra_user,
            extra_user_token,
            "pu_instance_network_monthly_traffic_limit",
            1024,
        ),
        owner_setting(
            database,
            extra_user,
            extra_user_token,
            "pu_instance_network_traffic_reset_day",
            31,
        ),
    ):
        service = instance_resources_service(database, cc, extra_user_token)

        traffic = service.network_traffic()

    assert traffic.period_start <= datetime.now(UTC) < traffic.period_end
    for boundary in (traffic.period_start, traffic.period_end):
        assert boundary.hour == boundary.minute == 0
        assert boundary.day == monthrange(boundary.year, boundary.month)[1]
    assert traffic.limit_bytes == 1024
    assert 0 <= traffic.sampled_minutes <= traffic.elapsed_minutes
    assert traffic.estimated_bytes >= 0
    assert traffic.limit_percent == pytest.approx(
        traffic.estimated_bytes * 100 / traffic.limit_bytes
    )


@pytest.mark.notification
def test_resource_limit_alert_reaches_owner_once(
    resource_alert_owner, database, cc
) -> None:
    wait_for_resource_alert_window(database)
    owner = resource_alert_owner
    token = AgentUser(**owner.model_dump()).generate_agent_token()
    notifications = notification_service(database, cc, token)
    repository = notifications.notification_repository
    filters = NotificationFilter.unlimited(
        type=[NotificationType.INSTANCE_RESOURCE_LIMIT]
    )
    _, prior_alerts = repository.list(owner.uuid, filters)
    prior_ids = {alert.uuid for alert in prior_alerts}

    def new_alerts():
        _, alerts = notifications.list(filters)
        return [alert for alert in alerts if alert.uuid not in prior_ids]

    resources = instance_resources_service(database, cc, token)
    target_pg_percent = 95
    pg_bytes, _ = resources.repository.database_sizes()
    pg_size_limit = round(pg_bytes * 100 / target_pg_percent)
    expected_pg_percent = pytest.approx(target_pg_percent, abs=3)

    try:
        with ExitStack() as resource_limit:
            with (
                use_configured_test_telegram_chat(
                    database, owner
                ) as telegram_enabled,
                as_recipient(
                    database,
                    cc,
                    owner,
                    token,
                    telegram_enabled=telegram_enabled,
                ),
            ):
                resource_limit.enter_context(
                    owner_setting(
                        database,
                        owner,
                        token,
                        "pu_instance_pg_size_limit",
                        pg_size_limit,
                    )
                )
                wait_until(
                    lambda: bool(new_alerts()),
                    timeout=150,
                    interval=2,
                    session=database,
                    message="PostgreSQL limit alert did not reach the owner",
                )
                (alert,) = new_alerts()
                assert alert.user_uuid == owner.uuid
                pg_percent = alert.data["metrics"][
                    InstanceResourceMetric.POSTGRESQL
                ]
                assert pg_percent == expected_pg_percent
                wait_until(
                    lambda: notification_was_delivered(owner.uuid, alert.uuid),
                    message="resource alert was not published to the owner's stream",
                )

            next_sample = ensure_timezone_aware(
                alert.create_datetime
            ) + timedelta(seconds=1)
            wait_until(
                lambda: (
                    datetime.now(UTC) > next_sample
                    and bool(resources.history(next_sample, datetime.now(UTC)))
                ),
                timeout=80,
                interval=2,
                message="resource collector did not take the next sample",
            )
            samples = resources.history(next_sample, datetime.now(UTC))
            assert samples[-1].pg_disk_percent == expected_pg_percent
            _, alerts = repository.list(owner.uuid, filters)
            assert [
                item.uuid for item in alerts if item.uuid not in prior_ids
            ] == [alert.uuid]
    finally:
        _, alerts = repository.list(owner.uuid, filters)
        for item in alerts:
            if (
                item.uuid not in prior_ids
                and InstanceResourceMetric.POSTGRESQL
                in item.data.get("metrics", {})
            ):
                repository.delete(item)
