import asyncio
import logging
import uuid as uuid_pkg
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from datetime import UTC, datetime, timedelta

from sqlmodel import Session, col, select

from app import runtime, settings
from app.configs.db import get_hand_session
from app.configs.redis import get_redis_session
from app.domain.notification_model import Notification
from app.domain.notification_settings_model import NotificationSettings
from app.domain.user_model import User
from app.dto.enum import NotificationType
from app.repositories.notification_repository import NotificationRepository
from app.repositories.user_repository import UserRepository
from app.schemas.pydantic.instance_resources import InstanceResourceSample
from app.schemas.pydantic.notification import NotificationSettingsUpdate
from app.services.notification_delivery import notification_delivery
from app.services.notification_service import NotificationService
from app.utils.utils import ensure_timezone_aware
from tests.integration.helpers.services import notification_service
from tests.integration.helpers.wait import wait_until


def settings_update_of(
    settings_row: NotificationSettings,
) -> NotificationSettingsUpdate:
    return NotificationSettingsUpdate(
        is_scheduled_alert_enable=settings_row.is_scheduled_alert_enable,
        scheduled_notification_time=settings_row.scheduled_notification_time,
        is_data_pipe_alert_enable=settings_row.is_data_pipe_alert_enable,
        is_telegram_alert_enable=settings_row.is_telegram_alert_enable,
    )


@contextmanager
def as_recipient(
    database, cc, _user, token, *, telegram_enabled: bool | None = None
) -> Iterator[NotificationService]:
    """User whose notification settings are restored afterwards."""
    service = notification_service(database, cc, token)
    saved = settings_update_of(service.get_settings())
    try:
        if (
            telegram_enabled is not None
            and saved.is_telegram_alert_enable != telegram_enabled
        ):
            service.update_settings(
                NotificationSettingsUpdate(
                    is_telegram_alert_enable=telegram_enabled
                )
            )
        yield service
    finally:
        service.update_settings(saved)


def set_telegram_chats(chats: dict[uuid_pkg.UUID, str | None]) -> None:
    with get_hand_session() as session, session.begin():
        users = session.exec(
            select(User)
            .where(col(User.uuid).in_(chats))
            .order_by(User.uuid)
            .with_for_update()
        ).all()
        assert len(users) == len(chats)
        for user in users:
            user.telegram_chat_id = None
        session.flush()
        for user in users:
            user.telegram_chat_id = chats[user.uuid]


@contextmanager
def use_configured_test_telegram_chat(
    database: Session, recipient: User
) -> Iterator[bool]:
    chat_id = settings.pu_test_integration_telegram_chat_id.strip()
    if not chat_id or not runtime.get().pu_ff_telegram_bot_enable:
        yield False
        return

    database.expire_all()
    if recipient.telegram_chat_id == chat_id:
        yield True
        return

    holder = UserRepository(database).get_user_by_telegram_id(chat_id)
    previous = {recipient.uuid: recipient.telegram_chat_id}
    temporary = {recipient.uuid: chat_id}
    if holder is not None:
        previous[holder.uuid] = chat_id
        temporary[holder.uuid] = None

    set_telegram_chats(temporary)
    try:
        database.expire_all()
        yield True
    finally:
        set_telegram_chats(previous)
        database.expire_all()


def wait_for_resource_alert_window(database: Session) -> None:
    latest = database.exec(
        select(Notification.create_datetime)
        .where(Notification.type == NotificationType.INSTANCE_RESOURCE_LIMIT)
        .order_by(col(Notification.create_datetime).desc())
        .limit(1)
    ).first()
    if latest is None:
        return

    cooldown = NotificationService.RESOURCE_ALERT_INTERVAL
    setup_margin = timedelta(
        seconds=InstanceResourceSample.INTERVAL_SECONDS // 2
    )
    start = ensure_timezone_aware(latest) + cooldown - setup_margin
    if start > datetime.now(UTC):
        logging.info("Waiting for the previous resource alert cooldown")
        wait_until(
            lambda: datetime.now(UTC) >= start,
            timeout=cooldown.total_seconds(),
            interval=1,
            message="resource alert cooldown did not expire",
        )


def notification_was_delivered(
    user_uuid: uuid_pkg.UUID, notification_uuid: uuid_pkg.UUID
) -> bool:
    async def has_notification() -> bool:
        session = get_redis_session()
        try:
            redis = await anext(session)
            messages = await redis.xrevrange(
                notification_delivery.stream_name(user_uuid), count=100
            )
        finally:
            await session.aclose()
        needle = str(notification_uuid)
        return any(
            needle in fields[notification_delivery.EVENT]
            for _message_id, fields in messages
        )

    return asyncio.run(has_notification())


def latest_notification(database, user_uuid) -> Notification | None:
    return (
        database.query(Notification)
        .filter(Notification.user_uuid == user_uuid)
        .order_by(Notification.create_datetime.desc())
        .first()
    )


def process_saved(service, notifications: list[Notification]) -> None:
    """Processes batches until these rows are closed.

    The job also picks up other unprocessed rows, so one pass is not enough
    when the table is already busy.
    """
    waiting = {item.uuid for item in notifications}
    for _ in range(10):
        if not waiting:
            return
        asyncio.run(service.process_pending())
        done = [
            uuid
            for uuid in waiting
            if service.notification_repository.get(
                Notification(uuid=uuid)
            ).is_processed
        ]
        waiting.difference_update(done)


def drop_notification(database, uuid: uuid_pkg.UUID) -> None:
    if not settings.pu_test_integration_clear_data:
        return
    with suppress(Exception):
        NotificationRepository(database).delete(Notification(uuid=uuid))


def drop_notifications(database, notifications) -> None:
    for notification in notifications:
        drop_notification(database, notification.uuid)
