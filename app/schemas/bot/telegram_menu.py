import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import replace
from typing import Any

from aiogram import BaseMiddleware, Bot, types
from aiogram.exceptions import TelegramAPIError
from aiogram.fsm.context import FSMContext
from aiogram.fsm.middleware import FSMContextMiddleware
from aiogram.types import BotCommandScopeAllPrivateChats, BotCommandScopeChat
from fastapi import BackgroundTasks, Request
from redis.exceptions import RedisError
from sqlalchemy.exc import SQLAlchemyError

from app import runtime
from app.configs.db import get_hand_session
from app.configs.rest import ServiceFactory
from app.dto.enum import TelegramStateKey
from app.schemas.bot.telegram_router import TelegramRouter
from app.services.telegram_command_service import (
    TelegramCommandService,
    TelegramCommandSet,
)


class TelegramMenu(BaseMiddleware):
    def __init__(self, bot: Bot, fsm: FSMContextMiddleware) -> None:
        self.bot = bot
        self.fsm = fsm

    async def __call__(
        self,
        handler: Callable[
            [types.TelegramObject, dict[str, Any]], Awaitable[Any]
        ],
        event: types.Message | types.CallbackQuery,
        data: dict[str, Any],
    ) -> Any:
        if not runtime.get().pu_ff_telegram_bot_enable:
            return None
        data.update(
            telegram_commands=await self.refresh(
                data["state"], TelegramRouter.chat_id(event)
            )
        )
        return await handler(event, data)

    @staticmethod
    def load_commands(chat_id: int) -> TelegramCommandSet:
        with get_hand_session() as db:
            return (
                ServiceFactory(db)
                .get_telegram_command_service()
                .get_commands(chat_id)
            )

    async def refresh(
        self, state: FSMContext, chat_id: int
    ) -> TelegramCommandSet:
        commands = await asyncio.to_thread(self.load_commands, chat_id)
        cache = FSMContext(
            state.storage, replace(state.key, destiny=TelegramStateKey.MENU)
        )
        payload = commands.model_dump(mode="json")
        if payload == await cache.get_data():
            return commands

        try:
            updated = await self.bot.set_my_commands(
                commands.commands, scope=BotCommandScopeChat(chat_id=chat_id)
            )
        except TelegramAPIError as error:
            logging.warning(
                "Telegram command menu update failed for %s: %s",
                chat_id,
                error,
            )
            return commands

        if updated:
            await cache.set_data(payload)
        return commands

    async def set_default_commands(self) -> None:
        try:
            await self.bot.set_my_commands(
                TelegramCommandService.default_commands(),
                scope=BotCommandScopeAllPrivateChats(),
            )
        except TelegramAPIError as error:
            logging.warning(
                "Telegram default command menu update failed: %s", error
            )

    @staticmethod
    def schedule_refresh(
        request: Request, tasks: BackgroundTasks, chat_id: int | None
    ) -> None:
        menu = getattr(request.app.state, "telegram_menu", None)
        if menu is not None and chat_id:
            tasks.add_task(menu.refresh_chat, chat_id)

    async def refresh_chat(self, chat_id: int) -> None:
        try:
            if not runtime.get().pu_ff_telegram_bot_enable:
                return
            state = self.fsm.get_context(
                bot=self.bot, chat_id=chat_id, user_id=chat_id
            )
            async with self.fsm.events_isolation.lock(key=state.key):
                await self.refresh(state, chat_id)
        except RedisError, SQLAlchemyError:
            logging.exception(
                "Could not refresh the Telegram menu for %s", chat_id
            )
