import uuid as uuid_pkg
from enum import Enum
from html import escape

from aiogram import F, Router, types
from aiogram.enums import ChatType, ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import InlineKeyboardMarkup

from app import runtime, settings
from app.dto.enum import EntityNames, FrontendEntityPath, TelegramCallback


class TelegramRouter:
    MAX_MESSAGE_LENGTH = 4096
    MAX_CALLBACK_ANSWER_LENGTH = 200
    CALLBACK_SEPARATOR = "_"

    def __init__(self, entity_name: EntityNames | None = None) -> None:
        self.router = Router()
        self.entity_name = entity_name

    @staticmethod
    def chat_id(event: types.Message | types.CallbackQuery) -> int:
        if isinstance(event, types.Message):
            return event.chat.id
        return event.message.chat.id

    def callback_data(
        self,
        action: TelegramCallback,
        *values: Enum | str | int | uuid_pkg.UUID,
    ) -> str:
        fields = [self.entity_name.value, action.value]
        fields.extend(
            str(value.value) if isinstance(value, Enum) else str(value)
            for value in values
        )
        return self.CALLBACK_SEPARATOR.join(fields)

    def callback_prefix(self, action: TelegramCallback) -> str:
        return self.callback_data(action) + self.CALLBACK_SEPARATOR

    def item_selection(
        self, callback: types.CallbackQuery
    ) -> tuple[uuid_pkg.UUID, int]:
        _, entity_uuid, page = callback.data.rsplit(self.CALLBACK_SEPARATOR, 2)
        return uuid_pkg.UUID(entity_uuid), max(1, int(page))

    @staticmethod
    def text_length(text: str) -> int:
        return len(text.encode("utf-16-le")) // 2

    @staticmethod
    def shorten(text: str, limit: int) -> str:
        if limit <= 0:
            return ""
        encoded = text.encode("utf-16-le")
        if len(encoded) <= limit * 2:
            return text
        excerpt = encoded[: (limit - 1) * 2].decode(
            "utf-16-le", errors="ignore"
        )
        return f"{excerpt}…"

    @staticmethod
    def header_name_limit(text: str) -> str:
        return text[: runtime.get().pu_telegram_header_entity_length]

    @staticmethod
    def browser_url(
        path: FrontendEntityPath, entity_uuid: uuid_pkg.UUID
    ) -> str:
        return f"{settings.pu_link.rstrip('/')}/{path}/{entity_uuid}"

    def body_budget(self, title: str) -> int:
        return self.MAX_MESSAGE_LENGTH - self.text_length(title) - 1

    def render(
        self, title: str, body: str = "", body_limit: int | None = None
    ) -> str:
        title = self.shorten(title, self.MAX_MESSAGE_LENGTH)
        budget = self.body_budget(title)
        if body_limit is not None:
            budget = min(body_limit, budget)
        text = f"<b>{escape(title)}</b>"
        excerpt = self.shorten(body, budget)
        if excerpt:
            text += f"\n<pre>{escape(excerpt)}</pre>"
        return text

    async def respond(
        self,
        event: types.Message | types.CallbackQuery,
        title: str,
        keyboard: InlineKeyboardMarkup | None = None,
        *,
        body: str = "",
        body_limit: int | None = None,
        edit_message_id: int | None = None,
        send_new: bool = False,
    ) -> int:
        text = self.render(title, body, body_limit)
        if isinstance(event, types.CallbackQuery) and not send_new:
            edit_message_id = event.message.message_id

        chat_id = self.chat_id(event)
        if edit_message_id is None:
            message = await event.bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML,
            )
            if isinstance(event, types.CallbackQuery):
                await self.answer_callback(event)
            return message.message_id

        try:
            await event.bot.edit_message_text(
                chat_id=chat_id,
                message_id=edit_message_id,
                text=text,
                reply_markup=keyboard,
                parse_mode=ParseMode.HTML,
            )
        except TelegramBadRequest as error:
            reason = error.message.casefold()
            if "message is not modified" in reason:
                pass
            elif (
                "message to edit not found" in reason
                or "message can't be edited" in reason
            ):
                message = await event.bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML,
                )
                edit_message_id = message.message_id
            else:
                raise
        if isinstance(event, types.CallbackQuery):
            await self.answer_callback(event)
        return edit_message_id

    async def answer_callback(
        self,
        callback: types.CallbackQuery,
        text: str | None = None,
        *,
        show_alert: bool = False,
    ) -> None:
        try:
            await callback.answer(
                text=self.shorten(text, self.MAX_CALLBACK_ANSWER_LENGTH)
                if text
                else None,
                show_alert=show_alert,
            )
        except TelegramBadRequest as error:
            reason = error.message.casefold()
            if (
                "query is too old" not in reason
                and "query id is invalid" not in reason
            ):
                raise

    async def expired_button(self, callback: types.CallbackQuery) -> None:
        await self.answer_callback(
            callback,
            "This menu has expired. Open it again from the command menu.",
            show_alert=True,
        )


class PrivateBotRouter(TelegramRouter):
    def __init__(self) -> None:
        super().__init__()
        self.router.message.filter(F.chat.type == ChatType.PRIVATE)
        self.router.callback_query.filter(
            F.message.chat.type == ChatType.PRIVATE
        )
