import asyncio

from aiogram import types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext

from app.configs.db import get_hand_session
from app.configs.rest import get_bot_metrics_service
from app.dto.enum import CommandNames
from app.schemas.bot.telegram_router import TelegramRouter
from app.schemas.bot.utils import make_monospace_table_with_title
from app.schemas.pydantic.metrics import BaseMetricsRead
from app.services.telegram_command_service import TelegramCommandSet


class InfoBotRouter(TelegramRouter):
    def __init__(self) -> None:
        super().__init__()
        self.router.message(Command(CommandNames.INFO.value))(
            self.show_metrics
        )

    @staticmethod
    def get_metrics(chat_id: str | None) -> BaseMetricsRead:
        with get_hand_session() as db:
            return get_bot_metrics_service(db, chat_id).get_instance_metrics(
                public_only=chat_id is None
            )

    async def show_metrics(
        self,
        message: types.Message,
        state: FSMContext,
        telegram_commands: TelegramCommandSet,
    ) -> None:
        await state.clear()
        chat_id = (
            str(self.chat_id(message))
            if telegram_commands.role is not None
            else None
        )
        metrics = await asyncio.to_thread(self.get_metrics, chat_id)
        table = [
            ["Type", "Count"],
            ["User", metrics.user_count],
            ["Repo", metrics.repo_count],
            ["Unit", metrics.unit_count],
            ["UnitIO", metrics.unit_io_count],
            ["Link", metrics.link_count],
        ]
        await self.respond(
            message,
            "Instance metrics",
            body=make_monospace_table_with_title(table),
        )
