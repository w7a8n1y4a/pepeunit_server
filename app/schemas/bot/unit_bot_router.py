import asyncio
import json
import math
import uuid as uuid_pkg
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path

from aiogram import F, types
from aiogram.fsm.context import FSMContext
from aiogram.types import BufferedInputFile, FSInputFile, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder

from app import runtime
from app.configs.clickhouse import get_hand_clickhouse_client
from app.configs.db import get_hand_session
from app.configs.errors import GitRepoError, NoAccessError
from app.configs.rest import get_bot_unit_service
from app.domain.unit_model import Unit
from app.dto.enum import (
    BackendTopicCommand,
    CommandNames,
    DecreesNames,
    EntityNames,
    FrontendEntityPath,
    OwnershipType,
    TelegramCallback,
    UnitFirmwareUpdateStatus,
    UuidEntity,
    VisibilityLevel,
)
from app.schemas.bot.base_bot_router import (
    BaseBotFilters,
    BaseBotRouter,
    BotPage,
    UnitStates,
)
from app.schemas.bot.utils import (
    byte_converter,
    calculate_flash_mem,
    format_datetime,
    format_millis,
    make_monospace_table_with_title,
)
from app.schemas.pydantic.unit import (
    SelectedVersionRead,
    UnitFilter,
    UnitRead,
    UnitStateRead,
)
from app.services.validators import is_valid_uuid


@dataclass(frozen=True, slots=True)
class UnitCard:
    unit: UnitRead
    is_creator: bool
    selected_version: SelectedVersionRead | None = None
    version_error: str | None = None
    commands: tuple[BackendTopicCommand, ...] = ()


class UnitBotRouter(BaseBotRouter[Unit]):
    ARCHIVES = (DecreesNames.TGZ, DecreesNames.TAR, DecreesNames.ZIP)
    NETWORK_CONFIG_FIELDS = ("IP", "Subnet", "Gateway", "DNS")
    FLASH_MEMORY_FIELDS = 4
    ENV_TITLE = "Unit environment"
    ENV_FILENAME = "env.json"

    def __init__(self) -> None:
        super().__init__(
            EntityNames.UNIT,
            CommandNames.UNIT,
            search_state=UnitStates.waiting_for_search,
        )
        self.router.callback_query(
            F.data.startswith(self.callback_prefix(TelegramCallback.ITEM))
        )(self.handle_entity_click)
        self.router.callback_query(
            F.data.startswith(self.callback_prefix(TelegramCallback.ACTION))
        )(self.handle_entity_decrees)

    def load_page(
        self, filters: BaseBotFilters, chat_id: str
    ) -> BotPage[Unit]:
        with get_hand_session() as db:
            unit_service = get_bot_unit_service(db, None, chat_id)
            count, rows = unit_service.list(
                UnitFilter(
                    offset=filters.offset,
                    limit=filters.page_size,
                    visibility_level=filters.visibility_levels,
                    creator_uuid=(
                        unit_service.access_service.current_agent.uuid
                        if filters.is_only_my_entity
                        else None
                    ),
                    search_string=filters.search_string,
                )
            )
        return BotPage(count, [unit for unit, _ in rows], "Units")

    def build_list_keyboard(
        self, items: list[Unit], filters: BaseBotFilters
    ) -> InlineKeyboardBuilder:
        builder = InlineKeyboardBuilder()
        self.add_search(
            builder, filters, self.my_entities_button("My units", filters)
        )
        self.add_choices(builder, VisibilityLevel, filters.visibility_levels)
        for unit in items:
            name = self.header_name_limit(unit.name)
            builder.row(
                self.item_button(
                    f"{name} · {unit.visibility_level}",
                    unit.uuid,
                    filters.page,
                )
            )
        return builder

    @staticmethod
    def get_unit_card(unit_uuid: uuid_pkg.UUID, chat_id: str) -> UnitCard:
        with get_hand_session() as db:
            service = get_bot_unit_service(db, None, chat_id)
            row = service.get(unit_uuid)
            unit = UnitRead(**row.to_dict())
            try:
                service.access_service.authorization.check_ownership(
                    row, [OwnershipType.CREATOR]
                )
            except NoAccessError:
                return UnitCard(unit, is_creator=False)

            selected_version = None
            version_error = None
            try:
                selected_version = service.get_selected_version(unit_uuid)
            except GitRepoError as error:
                version_error = error.raw_message

            commands = tuple(service.unit_io_service.list_commands(unit_uuid))
            return UnitCard(
                unit, True, selected_version, version_error, commands
            )

    async def handle_entity_click(
        self, callback: types.CallbackQuery, state: FSMContext
    ) -> None:
        unit_uuid, page = self.item_selection(callback)
        card = await asyncio.to_thread(
            self.get_unit_card, unit_uuid, str(self.chat_id(callback))
        )
        filters = await self.detail_filters(
            state, callback.message.message_id, page
        )
        builder = self.build_card_keyboard(card, filters)
        unit = card.unit
        await self.show_detail(
            callback,
            state,
            filters,
            f"{self.header_name_limit(unit.name)} · {unit.visibility_level.value}",
            builder,
            body=self.card_text(card),
        )

    def card_text(self, card: UnitCard) -> str:
        unit = card.unit
        table = [
            ["Auto-update", unit.is_auto_update_unit],
            ["Tags only", unit.is_only_tag_update],
            ["Branch", unit.target_branch],
            ["Compiled firmware", unit.is_compilable_unit],
        ]
        if not unit.is_auto_update_unit:
            table.append(
                ["Pinned commit", self.short_commit(unit.target_commit)]
            )
        if unit.is_compilable_unit:
            table.append(["Platform", unit.target_firmware_platform])

        sections = [
            make_monospace_table_with_title(table, "Unit", lengths=[18, 38])
        ]
        if card.selected_version is not None or card.version_error is not None:
            sections.append(self.version_info(card))
        if unit.unit_state is not None:
            sections.append(self.state_info(unit.unit_state))
        return "\n".join(section for section in sections if section)

    @staticmethod
    def short_commit(commit: str | None) -> str | None:
        if commit is None:
            return None
        return commit[: runtime.get().pu_telegram_git_hash_length]

    def version_info(self, card: UnitCard) -> str:
        unit = card.unit
        target_commit = (
            card.selected_version.commit
            if card.selected_version is not None
            else None
        )
        status = unit.firmware_update_status
        if status is not None:
            status_text = status.value
        elif unit.current_commit_version and target_commit:
            status_text = (
                "Up to date"
                if unit.current_commit_version == target_commit
                else "Update available"
            )
        else:
            status_text = "Unknown"
        table = [
            ["Device commit", self.short_commit(unit.current_commit_version)],
            ["Selected commit", self.short_commit(target_commit)],
            ["Update status", status_text],
        ]
        if card.selected_version is not None and card.selected_version.tag:
            table.append(["Selected tag", card.selected_version.tag])
        if card.version_error:
            table.append(["Version error", card.version_error])
        if status == UnitFirmwareUpdateStatus.REQUEST_SENT:
            table.append(
                [
                    "Requested at",
                    format_datetime(unit.last_firmware_update_datetime),
                ]
            )
        if (
            status == UnitFirmwareUpdateStatus.ERROR
            and unit.firmware_update_error
        ):
            table.append(["Update error", unit.firmware_update_error])
        return make_monospace_table_with_title(
            table, "Firmware", lengths=[15, 38]
        )

    def state_info(self, state: UnitStateRead) -> str:
        table = [
            [label, value]
            for label, value in zip(
                self.NETWORK_CONFIG_FIELDS, state.ifconfig, strict=False
            )
        ]
        for label, value, as_bytes in (
            ("Frequency", state.freq, False),
            ("Allocated RAM", state.mem_alloc, True),
            ("Free RAM", state.mem_free, True),
        ):
            if value is not None and math.isfinite(value):
                table.append(
                    [
                        label,
                        byte_converter(value) if as_bytes else round(value, 1),
                    ]
                )
        if state.millis is not None and math.isfinite(state.millis):
            with suppress(ValueError, OverflowError, OSError):
                table.append(["Time / uptime", format_millis(state.millis)])

        flash_memory = state.statvfs[: self.FLASH_MEMORY_FIELDS]
        if len(flash_memory) == self.FLASH_MEMORY_FIELDS and all(
            isinstance(value, int | float) for value in flash_memory
        ):
            total, free, used = calculate_flash_mem(flash_memory)
            table.extend(
                [
                    ["Flash total", byte_converter(total)],
                    ["Flash free", byte_converter(free)],
                    ["Flash used", byte_converter(used)],
                ]
            )
        return make_monospace_table_with_title(
            table, "Unit state", lengths=[15, 38]
        )

    def build_card_keyboard(
        self, card: UnitCard, filters: BaseBotFilters
    ) -> InlineKeyboardBuilder:
        builder = InlineKeyboardBuilder()
        unit_uuid = card.unit.uuid
        if card.is_creator:
            builder.row(
                InlineKeyboardButton(
                    text="Get environment",
                    callback_data=self.callback_data(
                        TelegramCallback.ACTION,
                        DecreesNames.GET_ENV,
                        unit_uuid,
                    ),
                )
            )
            if card.commands:
                builder.row(
                    *[
                        InlineKeyboardButton(
                            text=command.value,
                            callback_data=self.callback_data(
                                TelegramCallback.ACTION, command, unit_uuid
                            ),
                        )
                        for command in card.commands
                    ],
                    width=self.FILTER_BUTTONS_PER_ROW,
                )
            if card.selected_version is not None:
                builder.row(
                    *[
                        InlineKeyboardButton(
                            text=archive.value,
                            callback_data=self.callback_data(
                                TelegramCallback.ACTION, archive, unit_uuid
                            ),
                        )
                        for archive in self.ARCHIVES
                    ]
                )

        buttons = [
            InlineKeyboardButton(
                text="Unit IO",
                callback_data=self.CALLBACK_SEPARATOR.join(
                    (
                        EntityNames.UNIT_IO.value,
                        TelegramCallback.UNIT,
                        str(unit_uuid),
                    )
                ),
            )
        ]
        if card.is_creator:
            buttons.append(
                InlineKeyboardButton(
                    text="Unit logs",
                    callback_data=self.CALLBACK_SEPARATOR.join(
                        (
                            EntityNames.UNIT_LOG.value,
                            TelegramCallback.UNIT,
                            str(unit_uuid),
                        )
                    ),
                )
            )
        builder.row(*buttons)
        builder.row(
            *self.detail_buttons(
                unit_uuid,
                filters.page,
                self.browser_url(FrontendEntityPath.UNIT, unit_uuid),
            )
        )
        return builder

    async def handle_entity_decrees(
        self, callback: types.CallbackQuery
    ) -> None:
        action, unit_id = callback.data.removeprefix(
            self.callback_prefix(TelegramCallback.ACTION)
        ).rsplit(self.CALLBACK_SEPARATOR, 1)
        unit_uuid = is_valid_uuid(unit_id, UuidEntity.UNIT)
        chat_id = str(self.chat_id(callback))

        if action == DecreesNames.GET_ENV:
            env = await asyncio.to_thread(self.read_env, unit_uuid, chat_id)
            await self.send_env(callback, env)
            return
        if action in self.ARCHIVES:
            path, filename = self.build_archive(
                unit_uuid, chat_id, DecreesNames(action)
            )
            try:
                await callback.bot.send_document(
                    chat_id=self.chat_id(callback),
                    document=FSInputFile(path, filename=filename),
                )
            finally:
                path.unlink(missing_ok=True)
            await self.answer_callback(callback)
            return
        if action not in BackendTopicCommand:
            await self.expired_button(callback)
            return

        command = BackendTopicCommand(action)
        with get_hand_session() as db, get_hand_clickhouse_client() as cc:
            service = get_bot_unit_service(db, cc, chat_id)
            sent = service.unit_io_service.command_to_input_base_topic(
                unit_uuid, command
            )
        if sent:
            await self.answer_callback(
                callback, f"{command.value} sent to unit."
            )
        else:
            await self.answer_callback(
                callback,
                "This command is no longer available. Refresh the unit card.",
                show_alert=True,
            )

    @staticmethod
    def read_env(unit_uuid: uuid_pkg.UUID, chat_id: str) -> str:
        with get_hand_session() as db:
            env = get_bot_unit_service(db, None, chat_id).get_env(unit_uuid)
        return json.dumps(env, ensure_ascii=False, indent=4)

    async def send_env(self, callback: types.CallbackQuery, env: str) -> None:
        if self.text_length(env) <= self.body_budget(self.ENV_TITLE):
            await self.respond(
                callback, self.ENV_TITLE, body=env, send_new=True
            )
            return
        document = BufferedInputFile(env.encode(), filename=self.ENV_FILENAME)
        await callback.bot.send_document(
            chat_id=self.chat_id(callback), document=document
        )
        await self.answer_callback(callback)

    @staticmethod
    def build_archive(
        unit_uuid: uuid_pkg.UUID, chat_id: str, archive: DecreesNames
    ) -> tuple[Path, str]:
        with get_hand_session() as db:
            service = get_bot_unit_service(db, None, chat_id)
            unit = service.get(unit_uuid)
            archive_methods = {
                DecreesNames.TGZ: service.get_unit_firmware_tgz,
                DecreesNames.TAR: service.get_unit_firmware_tar,
                DecreesNames.ZIP: service.get_unit_firmware_zip,
            }
            path = Path(archive_methods[archive](unit_uuid))
            return path, f"{unit.name}.{archive.value.lower()}"
