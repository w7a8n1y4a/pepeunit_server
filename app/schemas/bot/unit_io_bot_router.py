import asyncio
import json
import uuid as uuid_pkg

from aiogram import F, types
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder

from app.configs.db import get_hand_session
from app.configs.errors import CustomJSONDecodeError
from app.configs.rest import get_bot_unit_service
from app.domain.unit_io_model import UnitIO
from app.dto.enum import (
    EntityNames,
    FrontendEntityPath,
    JsonPayload,
    TelegramCallback,
    UnitIOTypeEnum,
    VisibilityLevel,
)
from app.schemas.bot.base_bot_router import (
    BaseBotFilters,
    BaseBotRouter,
    BotPage,
    UnitIOStates,
)
from app.schemas.bot.utils import (
    format_datetime,
    make_monospace_table_with_title,
)
from app.schemas.pydantic.unit_io import UnitIOFilter
from app.services.validators import is_valid_json


class UnitIOBotRouter(BaseBotRouter[UnitIO]):
    def __init__(self) -> None:
        super().__init__(
            EntityNames.UNIT_IO,
            search_state=UnitIOStates.waiting_for_search,
            from_unit=True,
        )
        self.router.callback_query(
            F.data.startswith(self.callback_prefix(TelegramCallback.ITEM))
        )(self.handle_entity_click)

    def load_page(
        self, filters: BaseBotFilters, chat_id: str
    ) -> BotPage[UnitIO]:
        with get_hand_session() as db:
            unit_service = get_bot_unit_service(db, None, chat_id)
            unit = unit_service.get(filters.unit_uuid)
            count, unit_ios = unit_service.unit_io_service.list(
                UnitIOFilter(
                    offset=filters.offset,
                    limit=filters.page_size,
                    visibility_level=filters.visibility_levels,
                    type=filters.unit_types,
                    search_string=filters.search_string,
                    unit_uuid=unit.uuid,
                )
            )
        return BotPage(
            count, unit_ios, f"Unit IO · {self.header_name_limit(unit.name)}"
        )

    def build_list_keyboard(
        self, items: list[UnitIO], filters: BaseBotFilters
    ) -> InlineKeyboardBuilder:
        builder = InlineKeyboardBuilder()
        self.add_search(builder, filters)
        self.add_choices(builder, UnitIOTypeEnum, filters.unit_types)
        self.add_choices(builder, VisibilityLevel, filters.visibility_levels)
        for unit_io in items:
            name = self.header_name_limit(unit_io.topic_name)
            builder.row(
                self.item_button(
                    f"{name} · {unit_io.type} · {unit_io.visibility_level}",
                    unit_io.uuid,
                    filters.page,
                )
            )
        return builder

    @staticmethod
    def get_unit_io(unit_io_uuid: uuid_pkg.UUID, chat_id: str) -> UnitIO:
        with get_hand_session() as db:
            return get_bot_unit_service(db, None, chat_id).unit_io_service.get(
                unit_io_uuid
            )

    async def handle_entity_click(
        self, callback: types.CallbackQuery, state: FSMContext
    ) -> None:
        unit_io_uuid, page = self.item_selection(callback)
        unit_io = await asyncio.to_thread(
            self.get_unit_io, unit_io_uuid, str(self.chat_id(callback))
        )
        filters = await self.detail_filters(
            state, callback.message.message_id, page, unit_io.unit_uuid
        )
        table = [
            ["Type", unit_io.type],
            ["Visibility", unit_io.visibility_level],
            ["Last update", format_datetime(unit_io.last_update_datetime)],
        ]
        if unit_io.type == UnitIOTypeEnum.INPUT:
            table.append(["Rewritable", unit_io.is_rewritable_input])
        if unit_io.last_message_error:
            table.append(["Last error", unit_io.last_message_error])

        body = make_monospace_table_with_title(
            table, "Unit IO", lengths=[15, 38]
        )
        if unit_io.state is not None:
            body += f"\nState\n{self.state_text(unit_io.state)}"

        builder = InlineKeyboardBuilder()
        builder.row(
            *self.detail_buttons(
                unit_io.uuid,
                filters.page,
                self.browser_url(FrontendEntityPath.UNIT_IO, unit_io.uuid),
            )
        )
        await self.show_detail(
            callback,
            state,
            filters,
            self.header_name_limit(unit_io.topic_name),
            builder,
            body=body,
        )

    @staticmethod
    def state_text(state: str) -> str:
        try:
            return json.dumps(
                is_valid_json(state, JsonPayload.UNIT_IO_STATE),
                ensure_ascii=False,
                indent=2,
            )
        except CustomJSONDecodeError:
            return state
