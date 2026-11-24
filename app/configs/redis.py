import logging
from collections.abc import AsyncIterator

from redis import Redis as SyncRedis
from redis.asyncio import Redis, from_url
from redis.exceptions import RedisError
from sqlalchemy.exc import SQLAlchemyError

from app import runtime, settings
from app.dto.enum import MessageProcessorStream


async def get_redis_session(
    socket_timeout: float | None = None,
) -> AsyncIterator[Redis]:
    timeout = runtime.get().pu_instance_http_timeout
    if socket_timeout is not None:
        timeout = socket_timeout
    session = from_url(
        settings.pu_redis_url,
        encoding="utf-8",
        decode_responses=True,
        socket_connect_timeout=runtime.get().pu_instance_http_connect_timeout,
        socket_timeout=timeout,
    )
    yield session
    session.close()
    await session.wait_closed()


def notify_message_processor() -> None:
    try:
        row = runtime.get()
        with SyncRedis.from_url(
            settings.pu_redis_url,
            socket_connect_timeout=row.pu_instance_http_connect_timeout,
            socket_timeout=row.pu_instance_http_timeout,
        ) as redis:
            redis.xadd(
                MessageProcessorStream.NAME.value,
                {MessageProcessorStream.RELOAD.value: 1},
            )
    except RedisError, RuntimeError, SQLAlchemyError:
        logging.exception("Failed to notify MessageProcessor")
