import asyncio
import uuid as uuid_pkg
from abc import ABC, abstractmethod
from dataclasses import dataclass, replace
from enum import Enum

from aiogram import F, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from pydantic import BaseModel, Field

from app import runtime
from app.dto.enum import (
    CommandNames,
    EntityNames,
    LogLevel,
    NotificationType,
    RepoType,
    TelegramCallback,
    TelegramFilter,
    TelegramStateKey,
    UnitIOTypeEnum,
    UuidEntity,
    VisibilityLevel,
)
from app.schemas.bot.telegram_router import TelegramRouter
from app.services.validators import is_valid_uuid


class DashboardStates(StatesGroup):
    waiting_for_search = State()


class RepoStates(StatesGroup):
    waiting_for_search = State()


class UnitStates(StatesGroup):
    waiting_for_search = State()


class UnitIOStates(StatesGroup):
    waiting_for_search = State()


class BotSearch(BaseModel):
    message_id: int | None = None


class BaseBotFilters(BaseModel):
    page: int = Field(default=1, ge=1)
    visibility_levels: list[VisibilityLevel] = Field(
        default_factory=lambda: list(VisibilityLevel)
    )
    unit_types: list[UnitIOTypeEnum] = Field(
        default_factory=lambda: list(UnitIOTypeEnum)
    )
    log_levels: list[LogLevel] = Field(default_factory=lambda: list(LogLevel))
    repository_types: list[RepoType] = Field(
        default_factory=lambda: list(RepoType)
    )
    notification_types: list[NotificationType] = Field(
        default_factory=lambda: list(NotificationType)
    )
    is_only_my_entity: bool = False
    search_string: str | None = None
    unit_uuid: uuid_pkg.UUID | None = None

    @property
    def page_size(self) -> int:
        return runtime.get().pu_telegram_items_per_page

    @property
    def offset(self) -> int:
        return (self.page - 1) * self.page_size

    @property
    def is_public_repository(self) -> bool | None:
        if len(self.repository_types) == 1:
            return self.repository_types[0] == RepoType.PUBLIC
        return None

    def toggle(self, entity: EntityNames, target: str) -> bool:
        if target == TelegramFilter.MINE:
            self.is_only_my_entity = not self.is_only_my_entity
            return True

        if entity == EntityNames.REPO:
            if target not in RepoType:
                return False
            self.toggle_choice(self.repository_types, RepoType(target))
            return True

        for choices, selected in (
            (VisibilityLevel, self.visibility_levels),
            (UnitIOTypeEnum, self.unit_types),
            (LogLevel, self.log_levels),
            (NotificationType, self.notification_types),
        ):
            if target in choices:
                self.toggle_choice(selected, choices(target))
                return True
        return False

    @staticmethod
    def toggle_choice[Choice: Enum](
        selected: list[Choice], value: Choice
    ) -> None:
        if value in selected:
            selected.remove(value)
        else:
            selected.append(value)


@dataclass(frozen=True, slots=True)
class BotPage[Item]:
    count: int
    items: list[Item]
    title: str
    body: str = ""


class BaseBotRouter[Item](TelegramRouter, ABC):
    FILTER_BUTTONS_PER_ROW = 3

    def __init__(
        self,
        entity_name: EntityNames,
        command: CommandNames | None = None,
        *,
        search_state: State | None = None,
        from_unit: bool = False,
    ) -> None:
        super().__init__(entity_name)
        self.command = command
        self.search_state = search_state
        if command is not None:
            self.router.message(Command(command.value))(self.open_list)
        if from_unit:
            self.router.callback_query(
                F.data.startswith(self.callback_prefix(TelegramCallback.UNIT))
            )(self.open_unit_list)

        self.router.callback_query(
            F.data.in_(
                [
                    self.callback_data(TelegramCallback.PREV_PAGE),
                    self.callback_data(TelegramCallback.NEXT_PAGE),
                ]
            )
        )(self.handle_pagination)
        self.router.callback_query(
            F.data == self.callback_data(TelegramCallback.BACK)
        )(self.handle_back)
        self.router.callback_query(
            F.data.startswith(self.callback_prefix(TelegramCallback.TOGGLE))
        )(self.toggle_filter)
        if search_state is not None:
            self.router.callback_query(
                F.data == self.callback_data(TelegramCallback.SEARCH)
            )(self.handle_search)
            self.router.callback_query(
                F.data == self.callback_data(TelegramCallback.CLEAR_SEARCH)
            )(self.clear_search)
            self.router.message(search_state, F.text, ~F.text.startswith("/"))(
                self.process_search
            )

    @abstractmethod
    def load_page(
        self, filters: BaseBotFilters, chat_id: str
    ) -> BotPage[Item]:
        pass

    @abstractmethod
    def build_list_keyboard(
        self, items: list[Item], filters: BaseBotFilters
    ) -> InlineKeyboardBuilder:
        pass

    def view_context(self, state: FSMContext, message_id: int) -> FSMContext:
        key = replace(
            state.key,
            destiny=f"{TelegramStateKey.VIEW}:{self.entity_name.value}:{message_id}",
        )
        return FSMContext(state.storage, key)

    async def read_filters(
        self, state: FSMContext, message_id: int
    ) -> BaseBotFilters | None:
        data = await self.view_context(state, message_id).get_data()
        return BaseBotFilters.model_validate(data) if data else None

    async def save_filters(
        self, state: FSMContext, message_id: int, filters: BaseBotFilters
    ) -> None:
        await self.view_context(state, message_id).set_data(
            filters.model_dump(mode="json")
        )

    async def open_list(
        self, message: types.Message, state: FSMContext
    ) -> None:
        await self.show_entities(message, state, BaseBotFilters())

    async def open_unit_list(
        self, callback: types.CallbackQuery, state: FSMContext
    ) -> None:
        unit_uuid = is_valid_uuid(
            callback.data.removeprefix(
                self.callback_prefix(TelegramCallback.UNIT)
            ),
            UuidEntity.UNIT,
        )
        await self.show_entities(
            callback, state, BaseBotFilters(unit_uuid=unit_uuid)
        )

    async def show_entities(
        self,
        event: types.Message | types.CallbackQuery,
        state: FSMContext,
        filters: BaseBotFilters,
        *,
        edit_message_id: int | None = None,
    ) -> None:
        chat_id = str(self.chat_id(event))
        page = await asyncio.to_thread(self.load_page, filters, chat_id)
        size = filters.page_size
        total_pages = max(1, (page.count + size - 1) // size)
        if filters.page > total_pages:
            filters.page = total_pages
            page = await asyncio.to_thread(self.load_page, filters, chat_id)

        title = page.title
        if filters.search_string:
            title = f"{title} · {filters.search_string}"

        builder = self.build_list_keyboard(page.items, filters)
        if not page.items:
            builder.row(
                InlineKeyboardButton(
                    text="No results", callback_data=TelegramCallback.NOOP
                )
            )
        self.add_pagination(builder, filters.page, total_pages)
        if filters.unit_uuid is not None:
            builder.row(
                InlineKeyboardButton(
                    text="← Back to unit",
                    callback_data=self.CALLBACK_SEPARATOR.join(
                        (
                            EntityNames.UNIT.value,
                            TelegramCallback.ITEM,
                            str(filters.unit_uuid),
                            "1",
                        )
                    ),
                )
            )

        message_id = await self.respond(
            event,
            title,
            builder.as_markup(),
            body=page.body,
            edit_message_id=edit_message_id,
        )
        await self.save_filters(state, message_id, filters)
        await state.clear()

    def add_pagination(
        self, builder: InlineKeyboardBuilder, page: int, total_pages: int
    ) -> None:
        if total_pages <= 1:
            return
        buttons = []
        if page > 1:
            buttons.append(
                InlineKeyboardButton(
                    text="⬅️",
                    callback_data=self.callback_data(
                        TelegramCallback.PREV_PAGE
                    ),
                )
            )
        buttons.append(
            InlineKeyboardButton(
                text=f"{page}/{total_pages}",
                callback_data=TelegramCallback.NOOP,
            )
        )
        if page < total_pages:
            buttons.append(
                InlineKeyboardButton(
                    text="➡️",
                    callback_data=self.callback_data(
                        TelegramCallback.NEXT_PAGE
                    ),
                )
            )
        builder.row(*buttons)

    def add_choices[Choice: Enum](
        self,
        builder: InlineKeyboardBuilder,
        choices: type[Choice],
        selected: list[Choice],
    ) -> None:
        buttons = [
            InlineKeyboardButton(
                text=f"{'🟢' if choice in selected else '🔴'} {choice.value}",
                callback_data=self.callback_data(
                    TelegramCallback.TOGGLE, choice
                ),
            )
            for choice in choices
        ]
        builder.row(*buttons, width=self.FILTER_BUTTONS_PER_ROW)

    def add_search(
        self,
        builder: InlineKeyboardBuilder,
        filters: BaseBotFilters,
        *buttons: InlineKeyboardButton,
    ) -> None:
        search_buttons = [
            InlineKeyboardButton(
                text="🔍 Search",
                callback_data=self.callback_data(TelegramCallback.SEARCH),
            )
        ]
        if filters.search_string:
            search_buttons.append(
                InlineKeyboardButton(
                    text="Clear search",
                    callback_data=self.callback_data(
                        TelegramCallback.CLEAR_SEARCH
                    ),
                )
            )
        builder.row(*search_buttons, *buttons)

    def my_entities_button(
        self, label: str, filters: BaseBotFilters
    ) -> InlineKeyboardButton:
        return InlineKeyboardButton(
            text=f"{'🟢' if filters.is_only_my_entity else '🔴'} {label}",
            callback_data=self.callback_data(
                TelegramCallback.TOGGLE, TelegramFilter.MINE
            ),
        )

    def item_button(
        self, label: str, entity_uuid: uuid_pkg.UUID, page: int
    ) -> InlineKeyboardButton:
        return InlineKeyboardButton(
            text=label,
            callback_data=self.callback_data(
                TelegramCallback.ITEM, entity_uuid, page
            ),
        )

    def detail_buttons(
        self, entity_uuid: uuid_pkg.UUID, page: int, url: str | None = None
    ) -> list[InlineKeyboardButton]:
        buttons = [
            InlineKeyboardButton(
                text="← Back",
                callback_data=self.callback_data(TelegramCallback.BACK),
            ),
            InlineKeyboardButton(
                text="↻ Refresh",
                callback_data=self.callback_data(
                    TelegramCallback.ITEM, entity_uuid, page
                ),
            ),
        ]
        if url is not None:
            buttons.append(InlineKeyboardButton(text="Browser", url=url))
        return buttons

    async def detail_filters(
        self,
        state: FSMContext,
        message_id: int,
        page: int,
        unit_uuid: uuid_pkg.UUID | None = None,
    ) -> BaseBotFilters:
        filters = await self.read_filters(state, message_id)
        if filters is None or (
            unit_uuid is not None and filters.unit_uuid != unit_uuid
        ):
            filters = BaseBotFilters(page=page, unit_uuid=unit_uuid)
        return filters

    async def show_detail(
        self,
        callback: types.CallbackQuery,
        state: FSMContext,
        filters: BaseBotFilters,
        title: str,
        keyboard: InlineKeyboardBuilder,
        *,
        body: str = "",
        body_limit: int | None = None,
    ) -> None:
        message_id = await self.respond(
            callback,
            title,
            keyboard.as_markup(),
            body=body,
            body_limit=body_limit,
        )
        await self.save_filters(state, message_id, filters)
        await state.clear()

    async def handle_back(
        self, callback: types.CallbackQuery, state: FSMContext
    ) -> None:
        filters = await self.read_filters(state, callback.message.message_id)
        if filters is None:
            if self.command is None:
                await self.expired_button(callback)
                return
            filters = BaseBotFilters()
        await self.show_entities(callback, state, filters)

    async def handle_pagination(
        self, callback: types.CallbackQuery, state: FSMContext
    ) -> None:
        filters = await self.read_filters(state, callback.message.message_id)
        if filters is None:
            await self.expired_button(callback)
            return
        if callback.data == self.callback_data(TelegramCallback.NEXT_PAGE):
            filters.page += 1
        else:
            filters.page = max(1, filters.page - 1)
        await self.show_entities(callback, state, filters)

    async def toggle_filter(
        self, callback: types.CallbackQuery, state: FSMContext
    ) -> None:
        filters = await self.read_filters(state, callback.message.message_id)
        target = callback.data.removeprefix(
            self.callback_prefix(TelegramCallback.TOGGLE)
        )
        if filters is None or not filters.toggle(self.entity_name, target):
            await self.expired_button(callback)
            return
        filters.page = 1
        await self.show_entities(callback, state, filters)

    async def handle_search(
        self, callback: types.CallbackQuery, state: FSMContext
    ) -> None:
        filters = await self.read_filters(state, callback.message.message_id)
        if filters is None:
            await self.expired_button(callback)
            return
        builder = InlineKeyboardBuilder()
        builder.button(
            text="← Cancel",
            callback_data=self.callback_data(TelegramCallback.BACK),
        )
        message_id = await self.respond(
            callback, "Enter a search query", builder.as_markup()
        )
        await self.save_filters(state, message_id, filters)
        await state.set_data(
            BotSearch(message_id=message_id).model_dump(mode="json")
        )
        await state.set_state(self.search_state)

    async def process_search(
        self, message: types.Message, state: FSMContext
    ) -> None:
        search = BotSearch.model_validate(await state.get_data())
        if search.message_id is None:
            await state.clear()
            await self.respond(message, "Open the list again to search.")
            return
        filters = await self.read_filters(state, search.message_id)
        if filters is None:
            await state.clear()
            await self.respond(message, "Open the list again to search.")
            return
        filters.search_string = message.text.strip() or None
        filters.page = 1
        await self.show_entities(
            message, state, filters, edit_message_id=search.message_id
        )

    async def clear_search(
        self, callback: types.CallbackQuery, state: FSMContext
    ) -> None:
        filters = await self.read_filters(state, callback.message.message_id)
        if filters is None:
            await self.expired_button(callback)
            return
        filters.search_string = None
        filters.page = 1
        await self.show_entities(callback, state, filters)
