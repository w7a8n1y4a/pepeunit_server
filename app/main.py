import logging
from contextlib import asynccontextmanager
from typing import Annotated

from aiogram import types
from fastapi import FastAPI, Header, HTTPException, status
from prometheus_fastapi_instrumentator import Instrumentator
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from app import BackendLogLevel, runtime, settings
from app.configs.errors import CustomException
from app.configs.logging_config import setup_logging
from app.configs.rest import get_unit_service
from app.configs.utils import recreate_directory
from app.dto.enum import TelegramBotMode
from app.routers.v1.endpoints import api_router
from app.schemas.bot.telegram_bot import TelegramBot
from app.schemas.bot.telegram_dispatcher import TelegramDispatcher
from app.schemas.mqtt.manager import mqtt_manager
from app.services.mqtt_auth_service import MqttAuthService
from app.services.startup_service import StartupService

setup_logging()


if runtime.get().pu_ff_prometheus_enable:
    recreate_directory(settings.pu_prometheus_multiproc_dir)


@asynccontextmanager
async def _lifespan(_app: FastAPI):
    _app.state.mqtt_auth_service = MqttAuthService(get_unit_service)
    startup = StartupService(
        _app,
        mqtt_manager,
        bot,
        dp,
    )
    try:
        await startup.start()
        yield
    finally:
        await startup.stop()


class CustomExceptionMiddleware:
    """Catch domain errors without running the app in a side task.

    BaseHTTPMiddleware executes the request in another task group. That
    task can close the database session while a worker thread is still
    inside libpq. This middleware stays on the request task, so session
    cleanup runs only after that work has returned.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        try:
            await self.app(scope, receive, send)
        except CustomException as exc:
            response = JSONResponse(
                status_code=exc.status_code,
                content={
                    "detail": exc.message,
                    "error_code": exc.error_code,
                },
            )
            await response(scope, receive, send)


app = FastAPI(
    title=settings.project_name,
    version=settings.version,
    openapi_url=f"{settings.pu_app_prefix}{settings.pu_api_v1_prefix}/openapi.json",
    docs_url=f"{settings.pu_app_prefix}/docs",
    debug=runtime.get().pu_instance_log_min_level == BackendLogLevel.DEBUG,
    lifespan=_lifespan,
)

app.add_middleware(CustomExceptionMiddleware)


bot = TelegramBot.from_settings()
dp = TelegramDispatcher.from_settings(bot) if bot is not None else None

if dp is not None:
    app.state.telegram_menu = dp.menu

    @app.post(
        f"{settings.pu_app_prefix}{settings.pu_api_v1_prefix}/bot",
        status_code=status.HTTP_204_NO_CONTENT,
    )
    async def bot_webhook(
        update: types.Update,
        secret_token: Annotated[
            str | None, Header(alias="X-Telegram-Bot-Api-Secret-Token")
        ] = None,
    ) -> None:
        if not bot.accepts_webhook(secret_token):
            msg = "Invalid Telegram webhook token"
            raise HTTPException(status.HTTP_403_FORBIDDEN, msg)
        instance = runtime.get()
        if (
            not instance.pu_ff_telegram_bot_enable
            or instance.pu_telegram_bot_mode != TelegramBotMode.WEBHOOK
        ):
            return
        try:
            await dp.feed_update(bot=bot, update=update)
        except Exception:
            logging.exception(
                "Could not process Telegram webhook update %s",
                update.update_id,
            )


if runtime.get().pu_ff_prometheus_enable:
    Instrumentator().instrument(app).expose(
        app, endpoint=f"{settings.pu_app_prefix}/metrics"
    )

app.include_router(
    api_router,
    prefix=f"{settings.pu_app_prefix}{settings.pu_api_v1_prefix}",
)
