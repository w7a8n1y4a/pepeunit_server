import asyncio
import uuid as uuid_pkg

from aiogram import F, types
from aiogram.fsm.context import FSMContext
from aiogram.types import BufferedInputFile, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder

from app import runtime
from app.configs.db import get_hand_session
from app.configs.rest import get_bot_notification_service
from app.domain.notification_model import Notification
from app.dto.enum import (
    CommandNames,
    EntityNames,
    NotificationType,
    TelegramCallback,
)
from app.schemas.bot.base_bot_router import (
    BaseBotFilters,
    BaseBotRouter,
    BotPage,
)
from app.schemas.pydantic.notification import NotificationFilter


class NotificationBotRouter(BaseBotRouter[Notification]):
    MAX_BUTTON_LENGTH = 64
    LOG_FILENAME = "alert.log"

    def __init__(self) -> None:
        super().__init__(EntityNames.NOTIFICATION, CommandNames.ALERTS)
        self.router.callback_query(
            F.data.startswith(self.callback_prefix(TelegramCallback.ITEM))
        )(self.handle_entity_click)
        self.router.callback_query(
            F.data.startswith(self.callback_prefix(TelegramCallback.LOG))
        )(self.handle_log_click)

    def load_page(
        self, filters: BaseBotFilters, chat_id: str
    ) -> BotPage[Notification]:
        with get_hand_session() as db:
            count, notifications = get_bot_notification_service(
                db, chat_id
            ).list(
                NotificationFilter(
                    offset=filters.offset,
                    limit=filters.page_size,
                    type=filters.notification_types,
                )
            )
        return BotPage(count, notifications, "Alerts")

    def build_list_keyboard(
        self, items: list[Notification], filters: BaseBotFilters
    ) -> InlineKeyboardBuilder:
        builder = InlineKeyboardBuilder()
        self.add_choices(builder, NotificationType, filters.notification_types)
        for notification in items:
            label = notification.small_text or notification.type
            if not notification.is_read:
                label = f"• {label}"
            builder.row(
                self.item_button(
                    self.shorten(label, self.MAX_BUTTON_LENGTH),
                    notification.uuid,
                    filters.page,
                )
            )
        return builder

    @staticmethod
    def read_notification(
        notification_uuid: uuid_pkg.UUID, chat_id: str
    ) -> Notification:
        with get_hand_session() as db:
            return get_bot_notification_service(db, chat_id).mark_read(
                notification_uuid
            )

    async def handle_entity_click(
        self, callback: types.CallbackQuery, state: FSMContext
    ) -> None:
        notification_uuid, page = self.item_selection(callback)
        notification = await asyncio.to_thread(
            self.read_notification,
            notification_uuid,
            str(self.chat_id(callback)),
        )
        filters = await self.detail_filters(
            state, callback.message.message_id, page
        )
        builder = InlineKeyboardBuilder()
        if notification.big_text:
            builder.row(
                InlineKeyboardButton(
                    text="Full log",
                    callback_data=self.callback_data(
                        TelegramCallback.LOG, notification.uuid, filters.page
                    ),
                )
            )
        builder.button(
            text="← Back",
            callback_data=self.callback_data(TelegramCallback.BACK),
        )
        await self.show_detail(
            callback,
            state,
            filters,
            f"Alert · {notification.type}",
            builder,
            body=notification.table_text or "",
            body_limit=runtime.get().pu_notification_telegram_alert_text_limit,
        )

    async def handle_log_click(
        self, callback: types.CallbackQuery, state: FSMContext
    ) -> None:
        notification_uuid, page = self.item_selection(callback)
        notification = await asyncio.to_thread(
            self.read_notification,
            notification_uuid,
            str(self.chat_id(callback)),
        )
        log = notification.big_text
        if not log:
            await self.answer_callback(
                callback, "This alert has no additional log."
            )
            return

        title = f"Alert log · {notification.type}"
        limit = min(
            runtime.get().pu_notification_telegram_alert_text_limit,
            self.body_budget(title),
        )
        if self.text_length(log) > limit:
            document = BufferedInputFile(
                log.encode(), filename=self.LOG_FILENAME
            )
            await callback.bot.send_document(
                chat_id=self.chat_id(callback), document=document
            )
            await self.answer_callback(callback)
            return

        filters = await self.detail_filters(
            state, callback.message.message_id, page
        )
        builder = InlineKeyboardBuilder()
        builder.row(
            self.item_button(
                "← Back to alert", notification.uuid, filters.page
            )
        )
        await self.show_detail(
            callback, state, filters, title, builder, body=log
        )
