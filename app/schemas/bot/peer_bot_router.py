import asyncio
import uuid as uuid_pkg
from urllib.parse import urlsplit

from aiogram import F, types
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from pydantic import ValidationError

from app import runtime
from app.configs.db import get_hand_session
from app.configs.rest import get_bot_peer_service
from app.domain.peer_model import Peer
from app.dto.enum import (
    CommandNames,
    DecreesNames,
    EntityNames,
    TelegramCallback,
    UserRole,
)
from app.schemas.bot.base_bot_router import (
    BaseBotFilters,
    BaseBotRouter,
    BotPage,
)
from app.schemas.bot.utils import (
    format_datetime,
    make_monospace_table_with_title,
)
from app.schemas.pydantic.instance import CurrentInstanceSchemaV1
from app.schemas.pydantic.peer import PeerFilter


class PeerBotRouter(BaseBotRouter[Peer]):
    def __init__(self) -> None:
        super().__init__(EntityNames.PEER, CommandNames.PEERS)
        self.router.callback_query(
            F.data.startswith(self.callback_prefix(TelegramCallback.ITEM))
        )(self.handle_entity_click)
        self.router.callback_query(
            F.data.startswith(self.callback_prefix(TelegramCallback.ACTION))
        )(self.handle_entity_decrees)

    def load_page(
        self, filters: BaseBotFilters, chat_id: str
    ) -> BotPage[Peer]:
        with get_hand_session() as db:
            count, peers = get_bot_peer_service(db, chat_id).list(
                PeerFilter(offset=filters.offset, limit=filters.page_size)
            )
        return BotPage(count, peers, "Peers")

    def build_list_keyboard(
        self, items: list[Peer], filters: BaseBotFilters
    ) -> InlineKeyboardBuilder:
        builder = InlineKeyboardBuilder()
        for peer in items:
            name = self.header_name_limit(urlsplit(peer.url).netloc)
            builder.row(
                self.item_button(
                    f"{peer.trust_status} · {name}", peer.uuid, filters.page
                )
            )
        return builder

    @staticmethod
    def get_details(
        peer_uuid: uuid_pkg.UUID, chat_id: str
    ) -> tuple[Peer, bool]:
        with get_hand_session() as db:
            peer_service = get_bot_peer_service(db, chat_id)
            peer = peer_service.get(peer_uuid)
            access = peer_service.access_service
            can_scan = (
                runtime.get().pu_ff_federation_enable
                and access.authorization.role_is_allowed(
                    access.current_agent.role, (UserRole.ADMIN,)
                )
            )
            return peer, can_scan

    async def handle_entity_click(
        self, callback: types.CallbackQuery, state: FSMContext
    ) -> None:
        peer_uuid, page = self.item_selection(callback)
        peer, can_scan = await asyncio.to_thread(
            self.get_details, peer_uuid, str(self.chat_id(callback))
        )
        filters = await self.detail_filters(
            state, callback.message.message_id, page
        )
        builder = InlineKeyboardBuilder()
        if can_scan:
            builder.row(
                InlineKeyboardButton(
                    text="↻ Scan peer",
                    callback_data=self.callback_data(
                        TelegramCallback.ACTION, DecreesNames.SCAN, peer.uuid
                    ),
                )
            )
        builder.row(*self.detail_buttons(peer.uuid, filters.page))
        await self.show_detail(
            callback,
            state,
            filters,
            urlsplit(peer.url).netloc,
            builder,
            body=make_monospace_table_with_title(
                self.details_table(peer), "Peer", lengths=[15, 38]
            ),
        )

    async def handle_entity_decrees(
        self, callback: types.CallbackQuery
    ) -> None:
        action, peer_id = callback.data.removeprefix(
            self.callback_prefix(TelegramCallback.ACTION)
        ).rsplit(self.CALLBACK_SEPARATOR, 1)
        if action != DecreesNames.SCAN:
            await self.expired_button(callback)
            return
        peer_uuid = uuid_pkg.UUID(peer_id)
        with get_hand_session() as db:
            task = get_bot_peer_service(
                db, str(self.chat_id(callback))
            ).scan_one(peer_uuid)
        await self.answer_callback(
            callback, f"Peer scan scheduled. Task {task.uuid}"
        )

    @staticmethod
    def details_table(peer: Peer) -> list[list[str | int | None]]:
        table = [
            ["Trust", peer.trust_status],
            ["Collection", peer.last_collection_status],
            [
                "Last ping (ms)",
                round(peer.last_ping) if peer.last_ping is not None else None,
            ],
            ["Last success", format_datetime(peer.last_success_datetime)],
        ]
        if peer.last_collection_error:
            table.append(["Error", peer.last_collection_error])
        if not peer.state:
            return table
        try:
            current = CurrentInstanceSchemaV1.model_validate(peer.state)
        except ValidationError:
            return table
        table.extend(
            [
                ["Version", current.version],
                ["Units", current.metrics.unit_count],
            ]
        )
        if current.contacts.email:
            table.append(["Email", current.contacts.email])
        if current.contacts.telegram:
            table.append(["Telegram", current.contacts.telegram])
        return table
