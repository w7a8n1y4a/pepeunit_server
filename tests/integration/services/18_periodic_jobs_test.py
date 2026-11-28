from collections import Counter
from datetime import UTC, datetime, timedelta

import pytest
from clickhouse_driver import Client
from pepeunit_client import PepeunitClient
from sqlmodel import Session

from app.domain.notification_model import Notification
from app.domain.unit_model import Unit
from app.domain.user_model import User
from app.dto.agent.abc import AgentUser
from app.dto.enum import InstanceResourceMetric, NotificationType
from app.schemas.pydantic.notification import (
    NotificationFilter,
    NotificationSettingsUpdate,
)
from app.schemas.pydantic.unit import UnitLogFilter
from app.services.notification_service import NotificationService
from app.services.unit_service import UnitService
from app.utils.utils import ensure_timezone_aware
from tests.integration.helpers.instance_settings import owner_setting
from tests.integration.helpers.mqtt import wait_for_live_log
from tests.integration.helpers.notifications import (
    as_recipient,
    drop_notifications,
    notification_was_delivered,
    use_configured_test_telegram_chat,
    wait_for_resource_alert_window,
)
from tests.integration.helpers.services import (
    instance_resources_service,
    unit_service,
)
from tests.integration.helpers.wait import wait_until

pytestmark = pytest.mark.notification


def publish_summary_logs(
    client: PepeunitClient, service: UnitService, unit: Unit
) -> None:
    client.connect()
    wait_for_live_log(client, service, unit.uuid)
    error = f"Scheduled summary error {unit.uuid}"
    critical = f"Scheduled summary critical {unit.uuid}"
    info = f"Scheduled summary info {unit.uuid}"
    client.logger.error(error)
    client.logger.error(error)
    client.logger.critical(critical)
    client.logger.info(info)
    expected = Counter({error: 2, critical: 1, info: 1})
    filters = UnitLogFilter.unlimited(uuid=unit.uuid)

    def stored() -> bool:
        return (
            Counter(
                entry.text
                for entry in service.log_list(filters)[1]
                if entry.text in expected
            )
            == expected
        )

    wait_until(stored, message="The summary Unit's MQTT logs were not stored")


def alerts_since(
    service: NotificationService,
    kind: NotificationType,
    start: datetime,
    *,
    visible: bool = True,
) -> list[Notification]:
    user_uuid = service.access_service.current_agent.uuid
    _, alerts = service.notification_repository.list(
        user_uuid,
        NotificationFilter.unlimited(type=[kind]),
        is_visible=visible,
    )
    return [
        alert
        for alert in alerts
        if ensure_timezone_aware(alert.create_datetime) >= start
    ]


def wait_for_delivered_alert(
    service: NotificationService, kind: NotificationType, start: datetime
) -> Notification:
    alert = None

    def delivered() -> bool:
        nonlocal alert
        alerts = alerts_since(service, kind, start)
        if not alerts:
            return False
        (alert,) = alerts
        return notification_was_delivered(alert.user_uuid, alert.uuid)

    wait_until(
        delivered,
        timeout=150,
        interval=2,
        message=f"{kind.value} did not reach the recipient's stream",
        session=service.notification_repository.db,
    )
    assert alert is not None
    assert alert.is_processed is True
    return alert


def test_periodic_jobs_deliver_alerts(
    sdk_unit: PepeunitClient,
    crud_unit: Unit,
    regular_user: User,
    regular_user_token: str,
    admin_user: User,
    admin_user_token: str,
    resource_alert_owner: User,
    database: Session,
    cc: Client,
) -> None:
    publish_summary_logs(
        sdk_unit, unit_service(database, cc, regular_user_token), crud_unit
    )
    wait_for_resource_alert_window(database)
    owner = resource_alert_owner
    owner_token = AgentUser(**owner.model_dump()).generate_agent_token()
    resources = instance_resources_service(database, cc, owner_token)
    pg_bytes, _ = resources.repository.database_sizes()
    target_percent = 95
    limit = round(pg_bytes * 100 / target_percent)
    expected_percent = pytest.approx(target_percent, abs=3)

    with (
        as_recipient(database, cc, admin_user, admin_user_token) as admin,
        as_recipient(
            database, cc, regular_user, regular_user_token
        ) as regular,
        use_configured_test_telegram_chat(database, owner) as telegram_enabled,
        as_recipient(
            database, cc, owner, owner_token, telegram_enabled=telegram_enabled
        ) as owner_notifications,
    ):
        armed_at = datetime.now(UTC)
        minutes = 2 if armed_at.second >= 50 else 1
        slot = (armed_at + timedelta(minutes=minutes)).replace(
            second=0, microsecond=0
        )
        schedule = NotificationSettingsUpdate(
            is_scheduled_alert_enable=True,
            scheduled_notification_time=slot.strftime("%H:%M"),
        )
        try:
            admin.update_settings(schedule)
            regular.update_settings(schedule)
            with owner_setting(
                database,
                owner,
                owner_token,
                "pu_instance_pg_size_limit",
                limit,
            ):
                resource = wait_for_delivered_alert(
                    owner_notifications,
                    NotificationType.INSTANCE_RESOURCE_LIMIT,
                    armed_at,
                )
                assert resource.user_uuid == owner.uuid
                assert (
                    resource.data["metrics"][InstanceResourceMetric.POSTGRESQL]
                    == expected_percent
                )

                instance = wait_for_delivered_alert(
                    admin, NotificationType.INSTANCE_DAILY_STATE, slot
                )
                errors = instance.data["errors"]
                assert len(errors) <= 3
                assert all(
                    {"count", "message"} <= item.keys() for item in errors
                )
                total = sum(item["count"] for item in errors)
                noun = "error" if total == 1 else "errors"
                assert instance.small_text == f"Instance has {total} {noun}"
                assert instance.table_text
                assert (
                    alerts_since(
                        regular, NotificationType.INSTANCE_DAILY_STATE, slot
                    )
                    == []
                )

                summary = wait_for_delivered_alert(
                    regular, NotificationType.UNIT_DAILY_SUMMARY, slot
                )
                rows = summary.data["units"]
                assert len(rows) <= 10
                assert {"unit_name": crud_unit.name, "error_count": 3} in rows
                total = sum(item["error_count"] for item in rows)
                assert summary.small_text == f"Unit has {total} errors"
                assert crud_unit.name in summary.table_text

                following = ensure_timezone_aware(
                    resource.create_datetime
                ) + timedelta(seconds=1)
                wait_until(
                    lambda: (
                        datetime.now(UTC) > following
                        and bool(
                            resources.history(following, datetime.now(UTC))
                        )
                    ),
                    timeout=80,
                    interval=2,
                    message="The resource collector did not take the next sample",
                )
                samples = resources.history(following, datetime.now(UTC))
                assert samples[-1].pg_disk_percent == expected_percent
                alerts = alerts_since(
                    owner_notifications,
                    NotificationType.INSTANCE_RESOURCE_LIMIT,
                    armed_at,
                    visible=False,
                )
                assert [item.uuid for item in alerts] == [resource.uuid]
        finally:
            drop_notifications(
                database,
                alerts_since(
                    admin,
                    NotificationType.INSTANCE_DAILY_STATE,
                    slot,
                    visible=False,
                ),
            )
            drop_notifications(
                database,
                alerts_since(
                    regular,
                    NotificationType.UNIT_DAILY_SUMMARY,
                    slot,
                    visible=False,
                ),
            )
            for alert in alerts_since(
                owner_notifications,
                NotificationType.INSTANCE_RESOURCE_LIMIT,
                armed_at,
                visible=False,
            ):
                if InstanceResourceMetric.POSTGRESQL in alert.data.get(
                    "metrics", {}
                ):
                    owner_notifications.notification_repository.delete(alert)
