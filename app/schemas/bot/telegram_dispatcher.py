from datetime import timedelta
from typing import Self

from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.base import (
    BaseEventIsolation,
    BaseStorage,
    DefaultKeyBuilder,
)
from aiogram.fsm.storage.redis import RedisStorage

from app import settings
from app.schemas.bot.control_bot_router import ControlBotRouter
from app.schemas.bot.dashboard_bot_router import DashboardBotRouter
from app.schemas.bot.error import ErrorBotRouter
from app.schemas.bot.info import InfoBotRouter
from app.schemas.bot.notification_bot_router import NotificationBotRouter
from app.schemas.bot.peer_bot_router import PeerBotRouter
from app.schemas.bot.repo_bot_router import RepoBotRouter
from app.schemas.bot.start_help import StartHelpBotRouter
from app.schemas.bot.telegram_menu import TelegramMenu
from app.schemas.bot.telegram_router import PrivateBotRouter
from app.schemas.bot.unit_bot_router import UnitBotRouter
from app.schemas.bot.unit_io_bot_router import UnitIOBotRouter
from app.schemas.bot.unit_log_bot_router import UnitLogBotRouter


class TelegramDispatcher(Dispatcher):
    VIEW_TTL = timedelta(days=7)

    def __init__(
        self,
        bot: Bot,
        storage: BaseStorage,
        events_isolation: BaseEventIsolation,
    ) -> None:
        super().__init__(storage=storage, events_isolation=events_isolation)
        self.menu = TelegramMenu(bot, self.fsm)
        private = PrivateBotRouter().router
        private.message.middleware(self.menu)
        private.callback_query.middleware(self.menu)
        private.include_routers(
            StartHelpBotRouter(self.menu).router,
            InfoBotRouter().router,
            ControlBotRouter().router,
            RepoBotRouter().router,
            UnitBotRouter().router,
            UnitIOBotRouter().router,
            UnitLogBotRouter().router,
            PeerBotRouter().router,
            NotificationBotRouter().router,
            DashboardBotRouter().router,
            ErrorBotRouter().router,
        )
        self.include_router(private)

    @classmethod
    def from_settings(cls, bot: Bot) -> Self:
        storage = RedisStorage.from_url(
            settings.pu_redis_url,
            key_builder=DefaultKeyBuilder(with_bot_id=True, with_destiny=True),
            state_ttl=cls.VIEW_TTL,
            data_ttl=cls.VIEW_TTL,
        )
        return cls(bot, storage, storage.create_isolation())
