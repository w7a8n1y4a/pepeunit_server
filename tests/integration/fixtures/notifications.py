from collections.abc import Iterator

import pytest

from app.domain.notification_model import Notification
from app.domain.user_model import User
from app.repositories.user_repository import UserRepository
from app.schemas.pydantic.notification import NotificationSettingsUpdate
from app.services.notification_service import NotificationService
from tests.integration.helpers.notifications import (
    as_recipient,
    drop_notifications,
    wait_for_task_alerts,
)
from tests.integration.helpers.owner import as_owner
from tests.integration.helpers.services import notification_service


@pytest.fixture
def resource_alert_owner(database, extra_user) -> Iterator[User]:
    owner = UserRepository(database).get_owner()
    if owner is not None:
        yield owner
        return

    with as_owner(database, extra_user):
        database.refresh(extra_user)
        yield extra_user


@pytest.fixture
def recipient_service(
    regular_user, regular_user_token, database, cc
) -> NotificationService:
    """Regular user receives data pipe alerts, settings restored afterwards"""
    with as_recipient(
        database, cc, regular_user, regular_user_token
    ) as service:
        service.update_settings(
            NotificationSettingsUpdate(is_data_pipe_alert_enable=True)
        )
        yield service


@pytest.fixture(scope="session")
def repo_task_alerts(
    repo_sync_task, regular_user_token, database, cc
) -> Iterator[tuple[Notification, Notification]]:
    _, task = repo_sync_task
    service = notification_service(database, cc, regular_user_token)
    alerts = wait_for_task_alerts(service, task)
    yield alerts
    drop_notifications(database, alerts)
