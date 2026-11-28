import asyncio
import uuid as uuid_pkg
from collections.abc import AsyncIterator
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Self
from unittest.mock import AsyncMock, Mock

import pytest
from aiogram import Bot, types
from aiogram.enums import ChatType
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.storage.memory import MemoryStorage, SimpleEventIsolation
from aiogram.methods import (
    AnswerCallbackQuery,
    DeleteWebhook,
    EditMessageText,
    GetWebhookInfo,
    SendDocument,
    SendMessage,
    SetMyCommands,
    SetWebhook,
)
from aiogram.methods.base import TelegramMethod
from fastapi import BackgroundTasks, FastAPI, Request

from app import runtime, settings
from app.configs.error_codes import ErrorCode
from app.configs.errors import NoAccessError, ValidationError
from app.domain.dashboard_model import Dashboard
from app.domain.instance_settings_model import InstanceSettings
from app.domain.notification_model import Notification
from app.domain.operation_task_model import OperationTask
from app.domain.peer_model import Peer
from app.domain.repo_model import Repo
from app.domain.unit_io_model import UnitIO
from app.domain.unit_model import Unit
from app.domain.user_model import User
from app.dto.clickhouse.log import UnitLog
from app.dto.enum import (
    BackendTopicCommand,
    CommandNames,
    LogLevel,
    NotificationType,
    RepoType,
    TelegramStateKey,
    UnitIOTypeEnum,
    UserRole,
    UserStatus,
)
from app.repositories.user_repository import UserRepository
from app.routers.v1.user_router import grant_admin, revoke_admin
from app.schemas.bot import (
    control_bot_router,
    dashboard_bot_router,
    info,
    notification_bot_router,
    peer_bot_router,
    repo_bot_router,
    start_help,
    unit_bot_router,
    unit_io_bot_router,
    unit_log_bot_router,
)
from app.schemas.bot.telegram_bot import TelegramBot
from app.schemas.bot.telegram_dispatcher import TelegramDispatcher
from app.schemas.pydantic.metrics import BaseMetricsRead
from app.schemas.pydantic.unit import SelectedVersionRead
from app.services import user_service
from app.services.startup_service import StartupService
from app.services.telegram_command_service import (
    TelegramCommandService,
)
from app.services.user_service import UserService


class LinkCodeStore:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.ttls: dict[str, int] = {}

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_error: object) -> None:
        return None

    async def session(self) -> AsyncIterator[Self]:
        yield self

    async def set(self, key: str, value: str, ex: int) -> None:
        self.values[key] = value
        self.ttls[key] = ex

    async def getdel(self, key: str) -> str | None:
        return self.values.pop(key, None)


class TelegramScenario:
    CHAT_ID = 123
    FAKE_TOKEN = "123456:telegram-scene-test"

    def __init__(self, patches: pytest.MonkeyPatch) -> None:
        self.settings = InstanceSettings(
            pu_ff_telegram_bot_enable=True,
            pu_ff_grafana_integration_enable=True,
            pu_ff_notification_enable=True,
            pu_ff_federation_enable=True,
            pu_telegram_items_per_page=1,
        )
        patches.setattr(runtime, "get", Mock(return_value=self.settings))
        self.now = datetime.now(UTC)
        self.user_repo = Mock(spec=UserRepository)
        self.user_repo.get_user_by_telegram_id.return_value = None
        self.command_service = TelegramCommandService(self.user_repo)
        self.bot = TelegramBot(self.FAKE_TOKEN)
        self.dp = TelegramDispatcher(
            self.bot, MemoryStorage(), SimpleEventIsolation()
        )
        patches.setattr(
            self.dp.menu, "load_commands", self.command_service.get_commands
        )
        patches.setattr(self.bot.session, "make_request", self.request)
        self.calls: list[TelegramMethod[Any]] = []
        self.update_id = 0
        self.message_id = 100
        self.upload_error = False
        self.webhook_registered = asyncio.Event()
        self.chat = types.Chat(id=self.CHAT_ID, type=ChatType.PRIVATE)
        self.sender = types.User(
            id=self.CHAT_ID, is_bot=False, first_name="User"
        )

    async def request(
        self, _bot: Bot, method: TelegramMethod[Any], **_options: Any
    ) -> Any:
        self.calls.append(method)
        match method:
            case SendMessage() | SendDocument():
                if isinstance(method, SendDocument) and self.upload_error:
                    msg = "Upload failed"
                    raise TelegramBadRequest(method, msg)
                self.message_id += 1
                return types.Message(
                    message_id=self.message_id, date=self.now, chat=self.chat
                )
            case GetWebhookInfo():
                return types.WebhookInfo(
                    url=f"{settings.pu_link_prefix_and_v1}/bot",
                    has_custom_certificate=False,
                    pending_update_count=0,
                    allowed_updates=self.dp.resolve_used_update_types(),
                )
            case SetWebhook():
                self.webhook_registered.set()
                return True
            case _:
                return True

    def link(
        self,
        role: UserRole = UserRole.USER,
        status: UserStatus = UserStatus.ACTIVE,
    ) -> User:
        user = User(
            role=role,
            status=status,
            telegram_chat_id=str(self.CHAT_ID),
            icon_uuid=uuid_pkg.uuid4(),
            public_key="test",
            hash_public_key="test",
            create_datetime=self.now,
        )
        self.user_repo.get_user_by_telegram_id.return_value = user
        return user

    async def say(self, text: str, chat: types.Chat | None = None) -> int:
        self.update_id += 1
        await self.dp.feed_update(
            self.bot,
            types.Update(
                update_id=self.update_id,
                message=types.Message(
                    message_id=self.update_id,
                    from_user=self.sender,
                    chat=chat or self.chat,
                    date=self.now,
                    text=text,
                ),
            ),
        )
        return self.message_id

    async def tap(self, message_id: int, data: str) -> None:
        self.update_id += 1
        await self.dp.feed_update(
            self.bot,
            types.Update(
                update_id=self.update_id,
                callback_query=types.CallbackQuery(
                    id=str(self.update_id),
                    from_user=self.sender,
                    chat_instance="test",
                    message=types.Message(
                        message_id=message_id, date=self.now, chat=self.chat
                    ),
                    data=data,
                ),
            ),
        )

    def methods[Method: TelegramMethod[Any]](
        self, method: type[Method]
    ) -> list[Method]:
        return [call for call in self.calls if isinstance(call, method)]

    def menu(self) -> set[str]:
        return {
            command.command
            for command in self.methods(SetMyCommands)[-1].commands
        }

    @staticmethod
    def button_data(reply: SendMessage | EditMessageText, label: str) -> str:
        return next(
            button.callback_data
            for row in reply.reply_markup.inline_keyboard
            for button in row
            if button.text == label
        )


class TestTelegramBot:
    @pytest.fixture
    def telegram(self, monkeypatch: pytest.MonkeyPatch) -> TelegramScenario:
        return TelegramScenario(monkeypatch)

    @pytest.mark.asyncio
    async def test_role_menu_and_dynamic_features(
        self, telegram: TelegramScenario
    ) -> None:
        await telegram.dp.menu.set_default_commands()
        assert isinstance(
            telegram.methods(SetMyCommands)[-1].scope,
            types.BotCommandScopeAllPrivateChats,
        )

        await telegram.say("/help")
        assert telegram.menu() == {
            CommandNames.START,
            CommandNames.HELP,
            CommandNames.INFO,
        }
        assert "/control" not in telegram.methods(SendMessage)[-1].text

        user = telegram.link()
        await telegram.say("/help")
        assert {
            CommandNames.REPO,
            CommandNames.UNIT,
            CommandNames.DASHBOARD,
            CommandNames.ALERTS,
        } <= telegram.menu()
        assert CommandNames.CONTROL not in telegram.menu()

        user.role = UserRole.ADMIN
        app = FastAPI()
        app.state.telegram_menu = telegram.dp.menu
        request = Request({"type": "http", "app": app})
        user_service = Mock()
        user_service.grant_admin.return_value = user
        tasks = BackgroundTasks()
        assert (
            grant_admin(user.uuid, request, tasks, user_service).role
            == UserRole.ADMIN
        )
        await tasks()
        assert CommandNames.CONTROL in telegram.menu()
        await telegram.say("/help")
        assert "/control" in telegram.methods(SendMessage)[-1].text

        user.role = UserRole.OWNER
        telegram.settings.pu_ff_grafana_integration_enable = False
        telegram.settings.pu_ff_notification_enable = False
        await telegram.say("/help")
        assert CommandNames.CONTROL in telegram.menu()
        assert telegram.menu().isdisjoint(
            {CommandNames.DASHBOARD, CommandNames.ALERTS}
        )

        user.role = UserRole.USER
        user_service.revoke_admin.return_value = user
        tasks = BackgroundTasks()
        assert (
            revoke_admin(user.uuid, request, tasks, user_service).role
            == UserRole.USER
        )
        await tasks()
        assert CommandNames.CONTROL not in telegram.menu()
        user.status = UserStatus.BLOCKED
        await telegram.dp.menu.refresh_chat(telegram.CHAT_ID)
        assert telegram.menu() == {
            CommandNames.START,
            CommandNames.HELP,
            CommandNames.INFO,
        }

    @pytest.mark.asyncio
    async def test_link_and_public_metrics(
        self, telegram: TelegramScenario, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        user_service = Mock()
        user_service.verification = AsyncMock(side_effect=self.link_account)
        self.scenario = telegram
        monkeypatch.setattr(
            start_help, "get_bot_user_service", Mock(return_value=user_service)
        )

        await telegram.say("/help ignored")
        user_service.verification.assert_not_awaited()
        await telegram.say("/start one-time-link")
        user_service.verification.assert_awaited_once_with(
            str(telegram.CHAT_ID), "one-time-link"
        )
        assert CommandNames.CONTROL in telegram.menu()
        assert "Telegram is linked" in telegram.methods(SendMessage)[-1].text

        metrics = Mock()
        metrics.get_instance_metrics.return_value = BaseMetricsRead(
            user_count=1,
            repo_count=2,
            unit_count=3,
            unit_io_count=4,
            link_count=5,
        )
        factory = Mock(return_value=metrics)
        monkeypatch.setattr(info, "get_bot_metrics_service", factory)
        telegram.user_repo.get_user_by_telegram_id.return_value = None
        await telegram.say("/info")
        assert factory.call_args.args[-1] is None
        metrics.get_instance_metrics.assert_called_once_with(public_only=True)

        calls = len(telegram.calls)
        await telegram.say(
            "/start irrelevant", types.Chat(id=-10, type=ChatType.GROUP)
        )
        assert len(telegram.calls) == calls

    async def link_account(self, _chat_id: str, _code: str) -> User:
        return self.scenario.link(UserRole.ADMIN)

    @pytest.mark.asyncio
    async def test_link_codes_are_single_use_and_accept_legacy_codes(
        self, telegram: TelegramScenario, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        user = telegram.link()
        store = LinkCodeStore()
        monkeypatch.setattr(user_service, "get_redis_session", store.session)
        repo = telegram.user_repo
        repo.get.return_value = user
        repo.update.return_value = user
        access = Mock()
        access.current_agent.uuid = user.uuid
        service = UserService(user_repository=repo, access_service=access)

        link = await service.generate_verification_link()
        code = link.rsplit("=", 1)[-1]
        key = f"{TelegramStateKey.VERIFICATION_CODE}:{code}"
        assert store.values[key] == str(user.uuid)
        assert store.ttls[key] == service.VERIFICATION_CODE_TTL
        chat_id = str(telegram.CHAT_ID + 1)
        assert (
            await service.verification(chat_id, code)
        ).telegram_chat_id == chat_id
        assert repo.get.call_args.args[0].uuid == user.uuid
        with pytest.raises(ValidationError) as error:
            await service.verification(chat_id, code)
        assert (
            error.value.error_code
            == ErrorCode.USER_VERIFICATION_CODE_NOT_FOUND
        )

        legacy_code = "code0123"
        store.values[legacy_code] = str(user.uuid)
        await service.verification(chat_id, legacy_code)
        assert legacy_code not in store.values

    @pytest.mark.asyncio
    async def test_search_and_pagination_preserve_each_message(
        self, telegram: TelegramScenario, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        telegram.link()
        repo = Repo(
            repository_url="https://host.example/project", platform="Gitlab"
        )
        repo_service = Mock()
        repo_service.list.return_value = (2, [(repo, 0)])
        repo_service.get.return_value = repo
        repo_service.schedule_update.return_value = OperationTask()
        monkeypatch.setattr(
            repo_bot_router,
            "get_bot_repo_service",
            Mock(return_value=repo_service),
        )

        unit_service = Mock()
        unit_service.list.return_value = (0, [])
        monkeypatch.setattr(
            unit_bot_router,
            "get_bot_unit_service",
            Mock(return_value=unit_service),
        )

        repo_id = await telegram.say("/repo")
        await telegram.tap(repo_id, "Repo_toggle_Private")
        assert repo_service.list.call_args.args[0].is_public_repository is True

        await telegram.say("/unit")
        await telegram.tap(repo_id, "Repo_next_page")
        assert repo_service.list.call_args.args[0].offset == 1

        await telegram.tap(repo_id, "Repo_search")
        await telegram.say("<query & text>")
        call = telegram.methods(EditMessageText)[-1]
        assert call.message_id == repo_id
        assert "&lt;query &amp; text&gt;" in call.text
        filters = repo_service.list.call_args.args[0]
        assert filters.offset == 0
        assert filters.search_string == "<query & text>"
        assert filters.is_public_repository is True

        await telegram.tap(repo_id, "Repo_search")
        await telegram.say("/help")
        assert "Description" in telegram.methods(SendMessage)[-1].text
        await telegram.tap(repo_id, "Repo_clear_search")
        assert repo_service.list.call_args.args[0].search_string is None
        await telegram.tap(repo_id, f"Repo_uuid_{repo.uuid}_1")
        repo_service.get.assert_called_once_with(repo.uuid)
        card = telegram.methods(EditMessageText)[-1]
        await telegram.tap(
            repo_id, telegram.button_data(card, "↻ Update local repository")
        )
        repo_service.schedule_update.assert_called_once_with(repo.uuid)
        await telegram.tap(repo_id, "Repo_back")
        assert repo_service.list.call_args.args[0].is_public_repository is True
        assert (
            telegram.button_data(call, f"🔴 {RepoType.PRIVATE.value}")
            == "Repo_toggle_Private"
        )

    @pytest.mark.asyncio
    async def test_unit_io_logs_environment_and_archives(
        self,
        telegram: TelegramScenario,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        user = telegram.link()
        unit = Unit(
            name="unit-name",
            creator_uuid=user.uuid,
            repo_uuid=uuid_pkg.uuid4(),
            icon_uuid=uuid_pkg.uuid4(),
            target_branch="main",
            create_datetime=telegram.now,
            last_update_datetime=telegram.now,
        )
        node = UnitIO(
            unit_uuid=unit.uuid,
            topic_name="state",
            type=UnitIOTypeEnum.OUTPUT,
            state="value <&>",
            last_update_datetime=telegram.now,
            creator_uuid=user.uuid,
        )
        unit_service = Mock()
        unit_service.access_service.current_agent.uuid = user.uuid
        unit_service.list.return_value = (2, [(unit, [])])
        unit_service.get.return_value = unit
        unit_service.get_selected_version.return_value = SelectedVersionRead(
            commit="ab" * 20
        )
        unit_service.get_env.return_value = {"SERIAL": "<&>"}
        unit_service.unit_io_service.list_commands.return_value = [
            BackendTopicCommand.IO_TOPICS_UPDATE
        ]
        unit_service.unit_io_service.command_to_input_base_topic.return_value = False
        unit_service.unit_io_service.list.return_value = (1, [node])
        unit_service.unit_io_service.get.return_value = node
        unit_service.log_list.return_value = (
            1,
            [
                UnitLog(
                    uuid=uuid_pkg.uuid4(),
                    unit_uuid=unit.uuid,
                    level=LogLevel.INFO,
                    text="log <&>",
                    create_datetime=telegram.now,
                    expiration_datetime=telegram.now,
                )
            ],
        )
        factory = Mock(return_value=unit_service)
        for module in (
            unit_bot_router,
            unit_io_bot_router,
            unit_log_bot_router,
        ):
            monkeypatch.setattr(module, "get_bot_unit_service", factory)

        card_id = await telegram.say("/unit")
        await telegram.tap(card_id, "Unit_next_page")
        await telegram.tap(card_id, f"Unit_uuid_{unit.uuid}_2")
        card = telegram.methods(EditMessageText)[-1]
        await telegram.tap(card_id, telegram.button_data(card, "Unit IO"))
        assert (
            unit_service.unit_io_service.list.call_args.args[0].unit_uuid
            == unit.uuid
        )
        await telegram.tap(card_id, f"UnitIO_uuid_{node.uuid}_1")
        detail = telegram.methods(EditMessageText)[-1]
        assert "value &lt;&amp;&gt;" in detail.text
        assert detail.reply_markup.inline_keyboard[-1][-1].url.endswith(
            f"/unit-node/{node.uuid}"
        )
        await telegram.tap(card_id, "UnitIO_back")
        await telegram.tap(
            card_id,
            telegram.button_data(
                telegram.methods(EditMessageText)[-1], "← Back to unit"
            ),
        )
        await telegram.tap(card_id, telegram.button_data(card, "Unit logs"))
        assert unit_service.log_list.call_args.args[0].uuid == unit.uuid
        assert (
            "log &lt;&amp;&gt;" in telegram.methods(EditMessageText)[-1].text
        )
        await telegram.tap(
            card_id,
            telegram.button_data(
                telegram.methods(EditMessageText)[-1], "← Back to unit"
            ),
        )
        await telegram.tap(card_id, "Unit_back")
        assert unit_service.list.call_args.args[0].offset == 1

        await telegram.tap(
            card_id, telegram.button_data(card, "Get environment")
        )
        assert (
            "&quot;SERIAL&quot;: &quot;&lt;&amp;&gt;&quot;"
            in telegram.methods(SendMessage)[-1].text
        )

        await telegram.tap(
            card_id, telegram.button_data(card, "IOTopicsUpdate")
        )
        unit_service.unit_io_service.command_to_input_base_topic.assert_called_once_with(
            unit.uuid, BackendTopicCommand.IO_TOPICS_UPDATE
        )
        assert telegram.methods(AnswerCallbackQuery)[-1].show_alert
        unit_service.unit_io_service.command_to_input_base_topic.return_value = True
        await telegram.tap(
            card_id, telegram.button_data(card, "IOTopicsUpdate")
        )
        assert (
            telegram.methods(AnswerCallbackQuery)[-1].text
            == "IOTopicsUpdate sent to unit."
        )

        archive = tmp_path / "firmware.zip"
        archive.write_bytes(b"test")
        unit_service.get_unit_firmware_zip.return_value = str(archive)
        telegram.upload_error = True
        await telegram.tap(card_id, telegram.button_data(card, "Zip"))
        assert not archive.exists()
        assert (
            telegram.methods(SendDocument)[-1].document.filename
            == "unit-name.zip"
        )
        assert telegram.methods(AnswerCallbackQuery)[-1].show_alert

    @pytest.mark.asyncio
    async def test_alerts_download_full_log_and_return_to_list(
        self, telegram: TelegramScenario, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        user = telegram.link(UserRole.ADMIN)
        note = Notification(
            type=NotificationType.INTEGRATION_TESTS,
            table_text="alert <&> `",
            big_text="long <&> log\n" * 600,
            user_uuid=user.uuid,
            create_datetime=telegram.now,
        )
        notification_service = Mock()
        notification_service.list.return_value = (1, [note])
        notification_service.mark_read.return_value = note
        monkeypatch.setattr(
            notification_bot_router,
            "get_bot_notification_service",
            Mock(return_value=notification_service),
        )

        alert_id = await telegram.say("/alerts")
        await telegram.tap(alert_id, f"Notification_uuid_{note.uuid}_1")
        detail = telegram.methods(EditMessageText)[-1]
        assert "alert &lt;&amp;&gt;" in detail.text
        await telegram.tap(alert_id, telegram.button_data(detail, "Full log"))
        assert (
            telegram.methods(SendDocument)[-1].document.data
            == note.big_text.encode()
        )
        await telegram.tap(alert_id, "Notification_back")
        assert notification_service.list.call_count == 2

    @pytest.mark.asyncio
    async def test_peers_dashboards_and_operation_callbacks(
        self, telegram: TelegramScenario, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        telegram.link(UserRole.ADMIN)
        peer = Peer(url="https://peer.example", state={"legacy": True})
        peer_service = Mock()
        peer_service.list.return_value = (1, [peer])
        peer_service.get.return_value = peer
        task = OperationTask()
        peer_service.scan_one.return_value = task
        peer_service.scan_all.return_value = task
        peer_service.start_integration_tests.return_value = task
        peer_factory = Mock(return_value=peer_service)
        monkeypatch.setattr(
            peer_bot_router, "get_bot_peer_service", peer_factory
        )
        monkeypatch.setattr(
            control_bot_router, "get_bot_peer_service", peer_factory
        )
        peer_id = await telegram.say("/peers")
        await telegram.tap(peer_id, f"Peer_uuid_{peer.uuid}_1")
        detail = telegram.methods(EditMessageText)[-1]
        await telegram.tap(
            peer_id, telegram.button_data(detail, "↻ Scan peer")
        )
        peer_service.scan_one.assert_called_once_with(peer.uuid)
        assert str(task.uuid) in telegram.methods(AnswerCallbackQuery)[-1].text

        control_id = await telegram.say("/control")
        control = telegram.methods(SendMessage)[-1]
        repo_service = Mock()
        repo_service.schedule_update_all.return_value = task
        unit_service = Mock()
        unit_service.schedule_bulk_update_units_firmware.return_value = task
        monkeypatch.setattr(
            control_bot_router,
            "get_bot_repo_service",
            Mock(return_value=repo_service),
        )
        monkeypatch.setattr(
            control_bot_router,
            "get_bot_unit_service",
            Mock(return_value=unit_service),
        )
        for label, action in (
            ("Scan all peers", peer_service.scan_all),
            ("Update all repositories", repo_service.schedule_update_all),
            (
                "Update all unit firmware",
                unit_service.schedule_bulk_update_units_firmware,
            ),
        ):
            await telegram.tap(
                control_id, telegram.button_data(control, label)
            )
            action.assert_called_once_with()
            assert (
                str(task.uuid)
                in telegram.methods(AnswerCallbackQuery)[-1].text
            )
        await telegram.tap(
            control_id, telegram.button_data(control, "Run integration tests")
        )
        peer_service.start_integration_tests.assert_called_once()
        msg = "Admin role required"
        peer_service.start_integration_tests.side_effect = NoAccessError(
            msg, code=ErrorCode.ACCESS_ROLE_NOT_ALLOWED
        )
        await telegram.tap(control_id, "Control_decres_IntegrationTests_0")
        answer = telegram.methods(AnswerCallbackQuery)[-1]
        assert answer.show_alert
        assert answer.text == msg

        dashboard = Dashboard(name="Demo")
        grafana = Mock()
        grafana.list_dashboards.return_value = (1, [dashboard])
        monkeypatch.setattr(
            dashboard_bot_router,
            "get_bot_grafana_service",
            Mock(return_value=grafana),
        )
        await telegram.say("/dashboard")
        button = telegram.methods(SendMessage)[
            -1
        ].reply_markup.inline_keyboard[1][0]
        assert button.url.endswith(f"/dashboard/{dashboard.uuid}")

    @pytest.mark.asyncio
    async def test_webhook_registration_sets_secret_on_existing_url(
        self, telegram: TelegramScenario
    ) -> None:
        startup = StartupService(FastAPI(), Mock(), telegram.bot, telegram.dp)
        work = asyncio.create_task(
            startup.run_telegram_webhook(telegram.bot, telegram.dp)
        )
        try:
            await asyncio.wait_for(
                telegram.webhook_registered.wait(), timeout=1
            )
        finally:
            work.cancel()
            with suppress(asyncio.CancelledError):
                await work
        webhook = telegram.methods(SetWebhook)[0]
        assert not webhook.drop_pending_updates
        assert set(webhook.allowed_updates) == {"message", "callback_query"}
        assert telegram.bot.accepts_webhook(webhook.secret_token)
        assert not telegram.bot.accepts_webhook("other")

    @pytest.mark.asyncio
    async def test_polling_registers_menu_and_removes_webhook(
        self, telegram: TelegramScenario, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        polling = AsyncMock()
        monkeypatch.setattr(telegram.dp, "start_polling", polling)
        startup = StartupService(FastAPI(), Mock(), telegram.bot, telegram.dp)
        await startup.run_telegram_bot(telegram.bot, telegram.dp)

        assert len(telegram.methods(SetMyCommands)) == 1
        assert len(telegram.methods(DeleteWebhook)) == 1
        polling.assert_awaited_once_with(
            telegram.bot, handle_signals=False, close_bot_session=False
        )
