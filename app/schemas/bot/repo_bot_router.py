import asyncio
import uuid as uuid_pkg
from urllib.parse import urlsplit

from aiogram import F, types
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder

from app.configs.db import get_hand_session
from app.configs.errors import NoAccessError
from app.configs.rest import get_bot_repo_service
from app.domain.repo_model import Repo
from app.dto.enum import (
    CommandNames,
    CredentialStatus,
    DecreesNames,
    EntityNames,
    FrontendEntityPath,
    RepoType,
    TelegramCallback,
    UuidEntity,
)
from app.schemas.bot.base_bot_router import (
    BaseBotFilters,
    BaseBotRouter,
    BotPage,
    RepoStates,
)
from app.schemas.bot.utils import (
    byte_converter,
    format_datetime,
    make_monospace_table_with_title,
)
from app.schemas.pydantic.repo import RepoFilter
from app.services.validators import is_valid_uuid


class RepoBotRouter(BaseBotRouter[Repo]):
    def __init__(self) -> None:
        super().__init__(
            EntityNames.REPO,
            CommandNames.REPO,
            search_state=RepoStates.waiting_for_search,
        )
        self.router.callback_query(
            F.data.startswith(self.callback_prefix(TelegramCallback.ITEM))
        )(self.handle_entity_click)
        self.router.callback_query(
            F.data.startswith(self.callback_prefix(TelegramCallback.ACTION))
        )(self.handle_entity_decrees)

    def load_page(
        self, filters: BaseBotFilters, chat_id: str
    ) -> BotPage[Repo]:
        with get_hand_session() as db:
            repo_service = get_bot_repo_service(db, chat_id)
            if not filters.repository_types:
                return BotPage(0, [], "Repositories")

            count, rows = repo_service.list(
                RepoFilter(
                    offset=filters.offset,
                    limit=filters.page_size,
                    is_public_repository=filters.is_public_repository,
                    creator_uuid=(
                        repo_service.access_service.current_agent.uuid
                        if filters.is_only_my_entity
                        else None
                    ),
                    search_string=filters.search_string,
                )
            )
        return BotPage(count, [repo for repo, _ in rows], "Repositories")

    def build_list_keyboard(
        self, items: list[Repo], filters: BaseBotFilters
    ) -> InlineKeyboardBuilder:
        builder = InlineKeyboardBuilder()
        self.add_search(
            builder, filters, self.my_entities_button("My repos", filters)
        )
        self.add_choices(builder, RepoType, filters.repository_types)
        for repo in items:
            name = self.header_name_limit(
                self.display_url(repo.repository_url)
            )
            builder.row(
                self.item_button(
                    f"{repo.platform} · {name}", repo.uuid, filters.page
                )
            )
        return builder

    @staticmethod
    def display_url(url: str) -> str:
        parsed = urlsplit(url)
        return f"{parsed.netloc}{parsed.path.rstrip('/').removesuffix('.git')}"

    @staticmethod
    def get_details(
        repo_uuid: uuid_pkg.UUID, chat_id: str
    ) -> tuple[Repo, bool, CredentialStatus | None]:
        with get_hand_session() as db:
            repo_service = get_bot_repo_service(db, chat_id)
            repo = repo_service.get(repo_uuid)
            credentials = (
                repo_service.get_credentials(repo.uuid)
                if not repo.is_public_repository
                else None
            )
            can_update = True
            try:
                repo_service.access_service.authorization.check_repo_access(
                    repo
                )
            except NoAccessError:
                can_update = False
            return (
                repo,
                can_update,
                credentials.status if credentials else None,
            )

    async def handle_entity_click(
        self, callback: types.CallbackQuery, state: FSMContext
    ) -> None:
        repo_uuid, page = self.item_selection(callback)
        repo, can_update, credential_status = await asyncio.to_thread(
            self.get_details, repo_uuid, str(self.chat_id(callback))
        )
        filters = await self.detail_filters(
            state, callback.message.message_id, page
        )
        table = [
            ["Platform", repo.platform],
            ["Sync status", repo.sync_status],
            ["Sync last time", format_datetime(repo.sync_last_datetime)],
            ["Sync error", repo.sync_error],
            ["Local size", byte_converter(repo.local_repository_size)],
        ]
        if credential_status is not None:
            table.append(["Credentials", credential_status.value])

        builder = InlineKeyboardBuilder()
        if can_update:
            builder.row(
                types.InlineKeyboardButton(
                    text="↻ Update local repository",
                    callback_data=self.callback_data(
                        TelegramCallback.ACTION,
                        DecreesNames.LOCAL_UPDATE,
                        repo.uuid,
                    ),
                )
            )
        builder.row(
            *self.detail_buttons(
                repo.uuid,
                filters.page,
                self.browser_url(FrontendEntityPath.REPO, repo.uuid),
            )
        )
        await self.show_detail(
            callback,
            state,
            filters,
            self.display_url(repo.repository_url),
            builder,
            body=make_monospace_table_with_title(
                table, "Repository", lengths=[15, 38]
            ),
        )

    async def handle_entity_decrees(
        self, callback: types.CallbackQuery
    ) -> None:
        action, repo_id = callback.data.removeprefix(
            self.callback_prefix(TelegramCallback.ACTION)
        ).rsplit(self.CALLBACK_SEPARATOR, 1)
        if action != DecreesNames.LOCAL_UPDATE:
            await self.expired_button(callback)
            return
        repo_uuid = is_valid_uuid(repo_id, UuidEntity.REPO)
        with get_hand_session() as db:
            task = get_bot_repo_service(
                db, str(self.chat_id(callback))
            ).schedule_update(repo_uuid)
        await self.answer_callback(
            callback, f"Repository update scheduled. Task {task.uuid}"
        )
