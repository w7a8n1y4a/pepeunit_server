from aiogram.types import BotCommand
from pydantic import BaseModel

from app import runtime
from app.dto.enum import CommandNames, UserRole, UserStatus
from app.repositories.user_repository import UserRepository
from app.services.auth.authorization_service import AuthorizationService


class TelegramCommandSet(BaseModel):
    commands: list[BotCommand]
    role: UserRole | None = None
    is_blocked: bool = False


class TelegramCommandService:
    PUBLIC_COMMANDS = (
        (CommandNames.START, "Start or link your Pepeunit account"),
        (CommandNames.HELP, "Show available commands"),
        (CommandNames.INFO, "Instance metrics"),
    )
    ACCOUNT_COMMANDS = (
        (CommandNames.REPO, "Browse repositories"),
        (CommandNames.UNIT, "Browse units, topics and logs"),
        (CommandNames.PEERS, "Browse known peers"),
    )

    def __init__(self, user_repository: UserRepository) -> None:
        self.user_repository = user_repository

    @classmethod
    def default_commands(cls) -> list[BotCommand]:
        return [
            BotCommand(command=name.value, description=description)
            for name, description in cls.PUBLIC_COMMANDS
        ]

    def get_commands(self, chat_id: int) -> TelegramCommandSet:
        user = self.user_repository.get_user_by_telegram_id(str(chat_id))
        commands = self.default_commands()
        if user is None:
            return TelegramCommandSet(commands=commands)
        if user.status == UserStatus.BLOCKED:
            return TelegramCommandSet(commands=commands, is_blocked=True)
        if user.status != UserStatus.ACTIVE or user.role not in UserRole:
            return TelegramCommandSet(commands=commands)

        available = list(self.ACCOUNT_COMMANDS)
        instance = runtime.get()
        if instance.pu_ff_grafana_integration_enable:
            available.append(
                (CommandNames.DASHBOARD, "Browse your dashboards")
            )
        if instance.pu_ff_notification_enable:
            available.append(
                (CommandNames.ALERTS, "Alerts and operation results")
            )
        if AuthorizationService.role_is_allowed(user.role, (UserRole.ADMIN,)):
            available.append((CommandNames.CONTROL, "Instance administration"))

        commands.extend(
            BotCommand(command=name.value, description=description)
            for name, description in available
        )
        return TelegramCommandSet(commands=commands, role=UserRole(user.role))
