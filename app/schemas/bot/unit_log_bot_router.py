from aiogram.utils.keyboard import InlineKeyboardBuilder

from app.configs.clickhouse import get_hand_clickhouse_client
from app.configs.db import get_hand_session
from app.configs.rest import get_bot_unit_service
from app.dto.clickhouse.log import UnitLog
from app.dto.enum import EntityNames, LogLevel
from app.schemas.bot.base_bot_router import (
    BaseBotFilters,
    BaseBotRouter,
    BotPage,
)
from app.schemas.pydantic.unit import UnitLogFilter


class UnitLogBotRouter(BaseBotRouter[UnitLog]):
    MAX_PREVIEW_LENGTH = 120

    def __init__(self) -> None:
        super().__init__(EntityNames.UNIT_LOG, from_unit=True)

    def load_page(
        self, filters: BaseBotFilters, chat_id: str
    ) -> BotPage[UnitLog]:
        with get_hand_session() as db, get_hand_clickhouse_client() as cc:
            unit_service = get_bot_unit_service(db, cc, chat_id)
            unit = unit_service.get(filters.unit_uuid)
            count, logs = unit_service.log_list(
                UnitLogFilter(
                    offset=filters.offset,
                    limit=filters.page_size,
                    level=filters.log_levels,
                    uuid=unit.uuid,
                )
            )

        body = "\n".join(
            f"{log.create_datetime:%Y-%m-%d %H:%M:%S} {log.level.value} "
            f"{self.shorten(' '.join(log.text.splitlines()), self.MAX_PREVIEW_LENGTH)}"
            for log in reversed(logs)
        )
        return BotPage(
            count,
            logs,
            f"Unit logs · {self.header_name_limit(unit.name)}",
            body,
        )

    def build_list_keyboard(
        self, _items: list[UnitLog], filters: BaseBotFilters
    ) -> InlineKeyboardBuilder:
        builder = InlineKeyboardBuilder()
        self.add_choices(builder, LogLevel, filters.log_levels)
        return builder
