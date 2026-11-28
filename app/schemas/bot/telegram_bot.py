import hashlib
import hmac
import logging
from typing import Self

from aiogram import Bot
from aiogram.client.session.aiohttp import AiohttpSession

from app import runtime, settings


class TelegramBot(Bot):
    @classmethod
    def from_settings(cls) -> Self | None:
        instance = runtime.get()
        if not instance.pu_ff_telegram_bot_enable:
            return None
        try:
            return cls(
                token=instance.pu_telegram_token,
                session=AiohttpSession(
                    timeout=instance.pu_instance_http_timeout,
                    proxy=instance.pu_telegram_proxy_url or None,
                ),
            )
        except Exception:
            logging.exception("Telegram bot failed to start")
            return None

    @property
    def webhook_secret(self) -> str:
        return hmac.new(
            settings.pu_secret_key.encode(),
            self.token.encode(),
            hashlib.sha256,
        ).hexdigest()

    def accepts_webhook(self, secret: str | None) -> bool:
        return secret is not None and hmac.compare_digest(
            secret.encode(), self.webhook_secret.encode()
        )
