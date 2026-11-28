import json
import logging
import shlex
import sys
import uuid as uuid_pkg
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from pydantic import ValidationError as SchemaValidationError
from sqlmodel import Session, col, select

from app import settings
from app.configs.errors import NoAccessError
from app.domain.notification_model import Notification
from app.dto.enum import (
    NotificationType,
    OperationTaskStatus,
    OperationTaskType,
)
from app.repositories.notification_settings_repository import (
    NotificationSettingsRepository,
)
from app.schemas.pydantic.notification import (
    NotificationFilter,
    NotificationRead,
    NotificationSettingsUpdate,
)
from app.schemas.pydantic.operation_task import OperationTaskCreate
from app.schemas.pydantic.unit_io import UnitIOUpdate
from app.utils.utils import create_upload_file_from_path, ensure_timezone_aware
from tests.client.mqtt import COUNTER_OUTPUT
from tests.integration.helpers.notifications import (
    drop_notifications,
    wait_for_task_alerts,
)
from tests.integration.helpers.services import (
    find_unit_io,
    notification_service,
    operation_task_service,
    peer_service,
    repo_service,
    unit_io_service,
)
from tests.integration.helpers.tasks import drop_task
from tests.integration.helpers.wait import wait_task_finish, wait_until

LIVE_ALERT_YAML = "tests/data/yaml/integra/data_pipe_alerts_live.yaml"

pytestmark = pytest.mark.notification


def test_get_notification_settings(
    regular_user, regular_user_token, database, cc
) -> None:
    settings_row = notification_service(
        database, cc, regular_user_token
    ).get_settings()
    logging.info(settings_row.uuid)
    assert settings_row.user_uuid == regular_user.uuid
    assert settings_row.is_scheduled_alert_enable is True
    assert settings_row.is_data_pipe_alert_enable is True
    assert settings_row.is_telegram_alert_enable is True
    assert settings_row.scheduled_notification_time == "16:00"


def test_user_create_makes_notification_settings(extra_user, database) -> None:
    # UserService.create stores the row, the API never relies on get_or_create
    settings_row = NotificationSettingsRepository(database).get_by_user(
        extra_user.uuid
    )
    assert settings_row is not None
    assert settings_row.is_data_pipe_alert_enable is True


def test_update_notification_settings(
    extra_user, extra_user_token, database, cc
) -> None:
    service = notification_service(database, cc, extra_user_token)
    updated = service.update_settings(
        NotificationSettingsUpdate(
            is_scheduled_alert_enable=True,
            scheduled_notification_time="16:00",
            is_data_pipe_alert_enable=True,
            is_telegram_alert_enable=True,
        )
    )
    logging.info(updated.uuid)
    assert updated.user_uuid == extra_user.uuid
    assert updated.is_scheduled_alert_enable is True
    assert updated.scheduled_notification_time == "16:00"
    assert updated.is_data_pipe_alert_enable is True
    assert updated.is_telegram_alert_enable is True

    stored = service.get_settings()
    assert stored.is_telegram_alert_enable is True
    assert stored.scheduled_notification_time == "16:00"


def test_update_notification_settings_invalid_time(
    extra_user_token, database, cc
) -> None:
    service = notification_service(database, cc, extra_user_token)
    with pytest.raises(SchemaValidationError):
        service.update_settings(
            NotificationSettingsUpdate(scheduled_notification_time="25:00")
        )
    assert service.get_settings().scheduled_notification_time == "16:00"


def test_integration_tests_big_text_is_stored(
    admin_user_token, database, cc
) -> None:
    full_log = "UNIQUE_STORED\n" + ("y" * 400)
    tasks = operation_task_service(database, admin_user_token)
    task = tasks.create(
        OperationTaskCreate(task_type=OperationTaskType.INTEGRATION_TESTS)
    )

    def failing_test_command(session: Session) -> str:
        service = peer_service(session, admin_user_token)
        service.INTEGRATION_TEST_COMMAND = shlex.join(
            [
                sys.executable,
                "-c",
                "import sys; sys.exit(sys.argv[1])",
                full_log,
            ]
        )
        return service.run_integration_tests()

    stored = ()
    try:
        tasks.schedule(task, failing_test_command)
        finished = wait_task_finish(database, task)
        assert finished.status == OperationTaskStatus.ERROR
        stored = wait_for_task_alerts(
            notification_service(database, cc, admin_user_token), finished
        )
        _, saved = stored
        assert saved.big_text == full_log
        assert "UNIQUE_STORED" not in saved.table_text
        assert saved.small_text == "IntegrationTests: Error"
        read = NotificationRead(**saved.dict())
        assert read.small_text == saved.small_text
        assert read.table_text == saved.table_text
        assert read.big_text == full_log
        dumped = read.model_dump()
        assert "data" not in dumped
        assert "text" not in dumped
    finally:
        drop_notifications(database, stored)
        drop_task(database, task.uuid)


def test_notification_stream(
    gitlab_public_repo, regular_user_token, database, cc
) -> None:
    url = f"{settings.pu_link_prefix_and_v1}/notifications/stream"
    rejected = httpx.get(
        url,
        headers={"x-auth-token": "not-a-token"},
        timeout=10,
    )
    assert rejected.status_code == 403

    alerts = ()
    task = None
    try:
        with httpx.stream(
            "GET",
            url,
            headers={
                "x-auth-token": regular_user_token,
                "accept": "text/event-stream",
            },
            timeout=httpx.Timeout(20.0, read=20.0),
        ) as response:
            assert response.status_code == 200
            assert response.headers["content-type"].startswith(
                "text/event-stream"
            )
            lines = response.iter_lines()
            assert next(lines) == ": connected"
            task = repo_service(database, regular_user_token).schedule_update(
                gitlab_public_repo.uuid
            )
            finished = wait_task_finish(database, task)
            assert finished.status == OperationTaskStatus.SUCCESS
            service = notification_service(database, cc, regular_user_token)
            alerts = wait_for_task_alerts(service, finished)
            _, notification = alerts

            message = None
            expected = str(notification.uuid)
            for line in lines:
                if not line.startswith("data: "):
                    continue
                payload = json.loads(line.removeprefix("data: "))
                if payload["uuid"] == expected:
                    message = payload
                    break
        assert message is not None
        assert message["uuid"] == str(notification.uuid)
        assert message["type"] == NotificationType.UPDATE_REPO.value
        assert "is_processed" not in message
        assert "data" not in message
        assert "Update Repo" in message["table_text"]
        assert message["small_text"]
        assert "text" not in message
    finally:
        drop_notifications(database, alerts)
        if task is not None:
            drop_task(database, task.uuid)


def test_delete_expired_data_pipe_alerts(database: Session) -> None:
    now = datetime.now(UTC)
    older = now - timedelta(days=91)
    newer = now - timedelta(days=89)
    user_uuid = uuid_pkg.uuid4()
    retained = [
        Notification(
            user_uuid=user_uuid,
            create_datetime=created_at,
            type=kind.value,
            is_processed=True,
            is_read=True,
        )
        for created_at, kind in (
            (
                newer - timedelta(microseconds=1),
                NotificationType.DATA_PIPE_ALERT,
            ),
            (newer, NotificationType.DATA_PIPE_ALERT),
            (
                newer + timedelta(microseconds=1),
                NotificationType.DATA_PIPE_ALERT,
            ),
            (older, NotificationType.INSTANCE_DAILY_STATE),
        )
    ]
    retained_uuids = {alert.uuid for alert in retained}
    before_cutoff_uuid = retained[0].uuid

    with database.get_bind().connect() as connection:
        connection.begin()
        connection.exec_driver_sql("SET LOCAL TIME ZONE 'UTC'")
        connection.exec_driver_sql(
            "CREATE TEMP TABLE notifications "
            "(LIKE notifications INCLUDING ALL) ON COMMIT DROP"
        )
        with Session(
            connection, join_transaction_mode="create_savepoint"
        ) as session:
            service = notification_service(session, None)
            expired_count = service.DATA_PIPE_ALERT_CLEANUP_BATCH_SIZE + 1
            expired = [
                Notification(
                    user_uuid=user_uuid,
                    create_datetime=older,
                    type=NotificationType.DATA_PIPE_ALERT.value,
                    is_processed=bool(index % 2),
                    is_read=bool(index % 2),
                )
                for index in range(expired_count)
            ]
            session.add_all(retained + expired)
            session.flush()
            connection.exec_driver_sql("SET LOCAL TIME ZONE 'Europe/Moscow'")
            assert service.delete_expired_data_pipe_alerts() == expired_count
            assert service.delete_expired_data_pipe_alerts() == 0

            remaining = session.exec(select(col(Notification.uuid))).all()
            assert set(remaining) == retained_uuids

            assert (
                service.notification_repository.delete_older_than(
                    NotificationType.DATA_PIPE_ALERT, newer, expired_count
                )
                == 1
            )
            remaining = session.exec(select(col(Notification.uuid))).all()
            assert set(remaining) == retained_uuids - {before_cutoff_uuid}


@pytest.mark.datapipe
async def test_data_pipe_alert_live(
    running_units, recipient_service, regular_user_token, database, cc
) -> None:
    """Unit emulator -> MQTT -> data pipe -> notifications table -> backend job"""
    service = unit_io_service(database, cc, regular_user_token)
    node = find_unit_io(
        service, running_units.chain_sink_unit.uuid, COUNTER_OUTPUT
    )
    started = datetime.now(UTC)
    created = []
    try:
        await service.update(node.uuid, UnitIOUpdate(is_data_pipe_active=True))
        await service.set_data_pipe_config(
            node.uuid, (await create_upload_file_from_path(LIVE_ALERT_YAML))
        )

        def alert_arrived() -> bool:
            _, notifications = recipient_service.list(
                NotificationFilter.unlimited(
                    type=[NotificationType.DATA_PIPE_ALERT.value]
                )
            )
            created[:] = [
                item
                for item in notifications
                if item.data["unit_io_uuid"] == str(node.uuid)
                and ensure_timezone_aware(item.create_datetime) >= started
            ]
            return bool(created)

        wait_until(
            alert_arrived,
            timeout=60,
            interval=2,
            message="data pipe alert did not reach the backend",
            session=database,
        )

        data = created[0].data
        assert data["type_value_threshold"] == "Min"
        assert data["threshold_min"] == 0
        assert data["topic_name"] == node.topic_name
        assert data["unit_uuid"] == str(node.unit_uuid)
        assert data["unit_name"] == running_units.chain_sink_unit.name
        assert float(data["value"]) >= 0
        alert = created[0]
        assert running_units.chain_sink_unit.name in alert.small_text
        assert running_units.chain_sink_unit.name in alert.table_text
        assert "Data pipe alert" in alert.table_text
        assert alert.big_text is None
    finally:
        await service.update(
            node.uuid, UnitIOUpdate(is_data_pipe_active=False)
        )
        recipient_service.update_settings(
            NotificationSettingsUpdate(is_data_pipe_alert_enable=False)
        )
        _, leftovers = recipient_service.list(
            NotificationFilter.unlimited(
                type=[NotificationType.DATA_PIPE_ALERT.value]
            )
        )
        drop_notifications(
            database,
            [
                item
                for item in leftovers
                if item.data["unit_io_uuid"] == str(node.uuid)
            ],
        )


def test_get_notification(
    repo_task_alerts, regular_user_token, database, cc
) -> None:
    service = notification_service(database, cc, regular_user_token)
    assert (
        service.get(repo_task_alerts[0].uuid).uuid == repo_task_alerts[0].uuid
    )


def test_get_notification_not_target(
    repo_task_alerts, extra_user_token, database, cc
) -> None:
    service = notification_service(database, cc, extra_user_token)
    with pytest.raises(NoAccessError):
        service.get(repo_task_alerts[0].uuid)


def test_get_many_notification(
    repo_task_alerts, regular_user, regular_user_token, database, cc
) -> None:
    service = notification_service(database, cc, regular_user_token)
    count, notifications = service.list(NotificationFilter.unlimited())
    assert count >= 1
    assert any(item.uuid == repo_task_alerts[0].uuid for item in notifications)
    assert all(item.user_uuid == regular_user.uuid for item in notifications)

    count, notifications = service.list(
        NotificationFilter.unlimited(
            type=[NotificationType.UPDATE_REPO.value],
            is_read=False,
        )
    )
    assert any(item.uuid == repo_task_alerts[0].uuid for item in notifications)
    assert all(
        item.type == NotificationType.UPDATE_REPO.value
        for item in notifications
    )
    assert all(item.is_read is False for item in notifications)

    count, notifications = service.list(
        NotificationFilter.unlimited(
            type=[NotificationType.UNIT_DAILY_SUMMARY.value]
        )
    )
    assert all(item.uuid != repo_task_alerts[0].uuid for item in notifications)


def test_list_notifications_only_own(
    repo_task_alerts, extra_user_token, database, cc
) -> None:
    service = notification_service(database, cc, extra_user_token)
    _count, notifications = service.list(NotificationFilter.unlimited())
    assert all(item.uuid != repo_task_alerts[0].uuid for item in notifications)


def test_notification_read_lifecycle(
    repo_task_alerts, regular_user_token, database, cc
) -> None:
    started, finished = repo_task_alerts
    service = notification_service(database, cc, regular_user_token)
    assert started.is_read is False
    assert finished.is_read is False

    read = service.mark_read(started.uuid)
    assert read.is_read is True
    assert read.read_datetime is not None
    assert service.mark_read(started.uuid).read_datetime == read.read_datetime

    assert service.mark_all_read() >= 1
    _, notifications = service.list(NotificationFilter.unlimited())
    assert {started.uuid, finished.uuid} <= {
        item.uuid for item in notifications
    }
    assert all(item.is_read is True for item in notifications)
    assert service.mark_all_read() == 0


def test_notification_anonymous(repo_task_alerts, database, cc) -> None:
    service = notification_service(database, cc, None)
    with pytest.raises(NoAccessError):
        service.get_settings()
    with pytest.raises(NoAccessError):
        service.update_settings(
            NotificationSettingsUpdate(is_telegram_alert_enable=True)
        )
    with pytest.raises(NoAccessError):
        service.get(repo_task_alerts[0].uuid)
    with pytest.raises(NoAccessError):
        service.list(NotificationFilter())
    with pytest.raises(NoAccessError):
        service.mark_read(repo_task_alerts[0].uuid)
    with pytest.raises(NoAccessError):
        service.mark_all_read()
