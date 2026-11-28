from aiogram import types
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder

from app import runtime, settings
from app.configs.db import get_hand_session
from app.configs.rest import get_bot_user_service
from app.dto.enum import CommandNames
from app.schemas.bot.telegram_menu import TelegramMenu
from app.schemas.bot.telegram_router import TelegramRouter
from app.schemas.bot.utils import make_monospace_table_with_title
from app.schemas.pydantic.shared import Root
from app.services.telegram_command_service import TelegramCommandSet


class StartHelpBotRouter(TelegramRouter):
    COMMAND_TABLE_WIDTHS = [10, 38]

    def __init__(self, menu: TelegramMenu) -> None:
        super().__init__()
        self.menu = menu
        self.router.message(Command(CommandNames.START.value))(self.start)
        self.router.message(Command(CommandNames.HELP.value))(self.show_help)

    async def start(
        self,
        message: types.Message,
        state: FSMContext,
        command: CommandObject,
        telegram_commands: TelegramCommandSet,
    ) -> None:
        if not command.args:
            await self.show_help(message, state, telegram_commands)
            return

        await state.clear()
        if telegram_commands.is_blocked:
            await self.respond(message, "Your Pepeunit account is blocked.")
            return
        if telegram_commands.role is not None:
            await self.respond(
                message,
                "Telegram is already linked to this Pepeunit instance.",
            )
            return

        chat_id = self.chat_id(message)
        with get_hand_session() as db:
            await get_bot_user_service(db, None, None).verification(
                str(chat_id), command.args
            )

        await self.menu.refresh(state, chat_id)
        await self.respond(
            message, "Telegram is linked to your Pepeunit account."
        )

    async def show_help(
        self,
        message: types.Message,
        state: FSMContext,
        telegram_commands: TelegramCommandSet,
    ) -> None:
        await state.clear()
        root = Root()
        table = [["Command", "Description"]]
        table.extend(
            [f"/{command.command}", command.description]
            for command in telegram_commands.commands
        )
        body = make_monospace_table_with_title(
            table, lengths=self.COMMAND_TABLE_WIDTHS
        )
        if telegram_commands.is_blocked:
            body += "\nYour Pepeunit account is blocked."
        elif telegram_commands.role is None:
            body += (
                "\nOpen your Pepeunit profile to link your Telegram account."
            )

        builder = InlineKeyboardBuilder()
        builder.row(
            InlineKeyboardButton(text="Frontend", url=settings.pu_link),
            InlineKeyboardButton(
                text="Documentation", url="https://pepeunit.com"
            ),
        )
        builder.button(text="Swagger", url=root.swagger)
        if runtime.get().pu_ff_grafana_integration_enable:
            builder.button(text="Grafana", url=root.grafana)

        await self.respond(
            message, f"Pepeunit {root.version}", builder.as_markup(), body=body
        )
