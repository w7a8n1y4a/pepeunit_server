import logging

from aiogram import F, types
from aiogram.exceptions import TelegramAPIError
from fastapi import HTTPException

from app.configs.error_codes import ErrorCode
from app.configs.errors import CustomException
from app.dto.enum import TelegramCallback
from app.schemas.bot.telegram_router import TelegramRouter


class ErrorBotRouter(TelegramRouter):
    def __init__(self) -> None:
        super().__init__()
        self.router.error()(self.handle_error)
        self.router.callback_query(F.data == TelegramCallback.NOOP)(
            self.answer_callback
        )
        self.router.callback_query()(self.expired_button)

    async def handle_error(self, event: types.ErrorEvent) -> None:
        error = event.exception
        text = self.error_message(error)
        try:
            if event.update.callback_query is not None:
                await self.answer_callback(
                    event.update.callback_query, text, show_alert=True
                )
            elif event.update.message is not None:
                await event.update.message.answer(text, parse_mode=None)
        except TelegramAPIError as answer_error:
            logging.warning(
                "Could not answer Telegram update: %s", answer_error
            )

    @staticmethod
    def error_message(error: Exception) -> str:
        if isinstance(error, CustomException):
            match error.error_code:
                case ErrorCode.ACCESS_USER_NOT_FOUND:
                    return "Link your Telegram account from your Pepeunit profile first."
                case ErrorCode.ACCESS_USER_BLOCKED:
                    return "Your Pepeunit account is blocked."
                case ErrorCode.USER_VERIFICATION_CODE_NOT_FOUND:
                    return "This link has expired. Generate a new Telegram link in Pepeunit."
                case _:
                    return error.raw_message
        if isinstance(error, HTTPException) and isinstance(error.detail, str):
            return error.detail

        logging.error("Telegram update failed", exc_info=error)
        return "The operation failed. Try again later."
