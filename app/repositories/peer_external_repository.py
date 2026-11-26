import asyncio
import logging
import time
from dataclasses import dataclass

import httpx
from pydantic import TypeAdapter, ValidationError

from app import runtime
from app.configs.error_codes import ErrorCode
from app.configs.errors import PeerError
from app.schemas.pydantic.instance import CurrentInstanceSchemaV1
from app.schemas.pydantic.peer import PeerPublicRepo


@dataclass
class CollectedPeer:
    state: CurrentInstanceSchemaV1
    urls: list[str]
    repos: list[PeerPublicRepo]
    last_ping: float


class PeerExternalRepository:
    async def collect(self, current_url: str) -> CollectedPeer:
        started_at = time.perf_counter()
        state, urls, repos = await asyncio.gather(
            self._get_current(current_url),
            self._get_all_items(current_url, "urls", str),
            self._get_all_items(current_url, "repos", PeerPublicRepo),
        )
        return CollectedPeer(
            state=state,
            urls=urls,
            repos=repos,
            last_ping=round((time.perf_counter() - started_at) * 1000, 3),
        )

    async def _get_current(self, current_url: str) -> CurrentInstanceSchemaV1:
        response = await self._request(current_url)
        return CurrentInstanceSchemaV1.model_validate_json(response.content)

    async def _get_all_items[T](
        self,
        current_url: str,
        endpoint: str,
        item_type: type[T],
    ) -> list[T]:
        url = self.list_url(current_url, endpoint)
        page_type = TypeAdapter(dict[str, object])
        values_type = TypeAdapter(list[object])
        count_type = TypeAdapter(int)
        item_adapter = TypeAdapter(item_type)

        items = []
        offset = 0
        while True:
            response = await self._request(
                url,
                httpx.QueryParams(
                    offset=offset,
                    limit=runtime.get().pu_instance_max_pagination_size,
                ),
            )
            page = page_type.validate_json(response.content)
            total_count = count_type.validate_python(page.get("total_count"))
            page_items = values_type.validate_python(page.get(endpoint))
            for index, item in enumerate(page_items, start=offset):
                try:
                    items.append(item_adapter.validate_python(item))
                except ValidationError:
                    logging.warning(
                        "Skip invalid peer %s entry at offset %s from %s",
                        endpoint,
                        index,
                        current_url,
                    )

            offset += len(page_items)
            if not page_items or offset >= total_count:
                return items

    @staticmethod
    def list_url(current_url: str, endpoint: str) -> str:
        base, found, _rest = current_url.rpartition("/instances/")
        if not found:
            msg = 'Peer URL must point to the "/instances/current" endpoint'
            raise PeerError(msg, code=ErrorCode.PEER_URL_ENDPOINT)
        return f"{base}/peers/{endpoint}"

    async def _request(
        self,
        url: str,
        params: httpx.QueryParams | None = None,
    ) -> httpx.Response:
        async with httpx.AsyncClient(
            follow_redirects=False,
            timeout=runtime.get().http_timeout(),
        ) as client:
            response = await client.get(url, params=params)

        response.raise_for_status()
        self.is_valid_response_size(response)
        return response

    @staticmethod
    def is_valid_response_size(response: httpx.Response) -> None:
        if len(response.content) > runtime.get().pu_instance_max_state_size:
            msg = f"Peer response {len(response.content)} B exceeds the limit {runtime.get().pu_instance_max_state_size} B"
            raise PeerError(msg, code=ErrorCode.PEER_RESPONSE_TOO_LARGE)
