import logging
from hashlib import sha256
from threading import Lock
from typing import TYPE_CHECKING

from cachetools import TTLCache

from app.configs.db import get_hand_session
from app.services.auth.auth_service import JwtAuthService

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlmodel import Session

    from app.services.unit_service import UnitService


class MqttAuthService:
    CACHE_MAX_SIZE = 16_384
    CACHE_TTL_SECONDS = 60

    def __init__(
        self, unit_service_factory: Callable[[Session, None, str], UnitService]
    ) -> None:
        self.unit_service_factory = unit_service_factory
        self.allowed: TTLCache[tuple[bytes, str, bool], bool] = TTLCache(
            maxsize=self.CACHE_MAX_SIZE, ttl=self.CACHE_TTL_SECONDS
        )
        self.cache_lock = Lock()

    def authorize(self, token: str, topic: str, *, is_subscribe: bool) -> bool:
        try:
            key = sha256(token.encode()).digest(), topic, is_subscribe
            with self.cache_lock:
                if self.allowed.get(key):
                    return True

            with get_hand_session() as db:
                service = self.unit_service_factory(db, None, token)
                service.get_mqtt_auth(topic, is_subscribe=is_subscribe)
                auth = service.access_service.auth
        except Exception as error:
            logging.info("%r", error)
            return False

        if isinstance(auth, JwtAuthService) and not auth.has_expiration:
            with self.cache_lock:
                self.allowed[key] = True
        return True
