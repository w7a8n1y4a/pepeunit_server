from aiogram import F, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder

from app import runtime
from app.configs.db import get_hand_session
from app.configs.rest import (
    get_bot_peer_service,
    get_bot_repo_service,
    get_bot_unit_service,
)
from app.dto.enum import (
    AgentType,
    CommandNames,
    DecreesNames,
    EntityNames,
    TelegramCallback,
    UserRole,
)
from app.schemas.bot.telegram_router import TelegramRouter


class ControlBotRouter(TelegramRouter):
    def __init__(self) -> None:
        super().__init__(EntityNames.CONTROL)
        self.router.message(Command(CommandNames.CONTROL.value))(
            self.show_control
        )
        self.router.callback_query(
            F.data.startswith(self.callback_prefix(TelegramCallback.ACTION))
        )(self.handle_entity_decrees)

    async def show_control(
        self, message: types.Message, state: FSMContext
    ) -> None:
        await state.clear()
        with get_hand_session() as db:
            access = get_bot_peer_service(
                db, str(self.chat_id(message))
            ).access_service
            access.authorization.check_access(
                [AgentType.USER], [UserRole.ADMIN]
            )

        actions = [
            (DecreesNames.INTEGRATION_TESTS, "Run integration tests"),
            (DecreesNames.UPDATE_ALL_REPOS, "Update all repositories"),
            (
                DecreesNames.UPDATE_ALL_UNITS_FIRMWARE,
                "Update all unit firmware",
            ),
        ]
        if runtime.get().pu_ff_federation_enable:
            actions.insert(1, (DecreesNames.SCAN_ALL, "Scan all peers"))

        builder = InlineKeyboardBuilder()
        for decree, label in actions:
            builder.row(
                types.InlineKeyboardButton(
                    text=label,
                    callback_data=self.callback_data(
                        TelegramCallback.ACTION, decree
                    ),
                )
            )
        await self.respond(
            message, "Instance administration", builder.as_markup()
        )

    async def handle_entity_decrees(
        self, callback: types.CallbackQuery
    ) -> None:
        action = callback.data.removeprefix(
            self.callback_prefix(TelegramCallback.ACTION)
        ).split(self.CALLBACK_SEPARATOR, 1)[0]
        chat_id = str(self.chat_id(callback))
        with get_hand_session() as db:
            match action:
                case DecreesNames.INTEGRATION_TESTS:
                    task = get_bot_peer_service(
                        db, chat_id
                    ).start_integration_tests()
                case DecreesNames.SCAN_ALL:
                    task = get_bot_peer_service(db, chat_id).scan_all()
                case DecreesNames.UPDATE_ALL_REPOS:
                    task = get_bot_repo_service(
                        db, chat_id
                    ).schedule_update_all()
                case DecreesNames.UPDATE_ALL_UNITS_FIRMWARE:
                    task = get_bot_unit_service(
                        db, None, chat_id
                    ).schedule_bulk_update_units_firmware()
                case _:
                    await self.expired_button(callback)
                    return
        await self.answer_callback(
            callback, f"Operation scheduled. Task {task.uuid}"
        )
