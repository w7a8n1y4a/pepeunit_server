from aiogram.types import InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder

from app.configs.clickhouse import get_hand_clickhouse_client
from app.configs.db import get_hand_session
from app.configs.rest import get_bot_grafana_service
from app.domain.dashboard_model import Dashboard
from app.dto.enum import CommandNames, EntityNames, FrontendEntityPath
from app.schemas.bot.base_bot_router import (
    BaseBotFilters,
    BaseBotRouter,
    BotPage,
    DashboardStates,
)
from app.schemas.pydantic.grafana import DashboardFilter


class DashboardBotRouter(BaseBotRouter[Dashboard]):
    def __init__(self) -> None:
        super().__init__(
            EntityNames.DASHBOARD,
            CommandNames.DASHBOARD,
            search_state=DashboardStates.waiting_for_search,
        )

    def load_page(
        self, filters: BaseBotFilters, chat_id: str
    ) -> BotPage[Dashboard]:
        with get_hand_session() as db, get_hand_clickhouse_client() as cc:
            count, dashboards = get_bot_grafana_service(
                db, cc, chat_id
            ).list_dashboards(
                DashboardFilter(
                    offset=filters.offset,
                    limit=filters.page_size,
                    search_string=filters.search_string,
                )
            )
        return BotPage(count, dashboards, "Dashboards")

    def build_list_keyboard(
        self, items: list[Dashboard], filters: BaseBotFilters
    ) -> InlineKeyboardBuilder:
        builder = InlineKeyboardBuilder()
        self.add_search(builder, filters)
        for dashboard in items:
            builder.row(
                InlineKeyboardButton(
                    text=self.header_name_limit(dashboard.name),
                    url=self.browser_url(
                        FrontendEntityPath.DASHBOARD, dashboard.uuid
                    ),
                )
            )
        return builder
