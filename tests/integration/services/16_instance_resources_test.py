from calendar import monthrange
from datetime import UTC, datetime, timedelta
from math import isfinite

import pytest

from app.configs.error_codes import ErrorCode
from app.configs.errors import InstanceError, NoAccessError
from tests.integration.helpers.instance_settings import owner_setting
from tests.integration.helpers.owner import as_owner
from tests.integration.helpers.services import instance_resources_service
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
    integration_started_at: datetime,
    extra_user,
    extra_user_token,
    database,
    cc,
) -> None:
    with as_owner(database, extra_user):
        service = instance_resources_service(database, cc, extra_user_token)
        started = integration_started_at
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
