import asyncio
import logging
from collections.abc import Callable
from contextlib import suppress
from datetime import UTC, datetime, timedelta

import aiogram.exceptions
import httpx
from aiogram import Bot, Dispatcher
from aiogram.types import WebhookInfo
from clickhouse_migrations.clickhouse_cluster import ClickhouseCluster
from fastapi import FastAPI

from app import runtime, settings
from app.configs.datapipe_process import DataPipeProcess
from app.configs.emqx import ControlEmqx
from app.configs.utils import acquire_file_lock, wait_for_file_unlock
from app.dto.agent.abc import AgentBackend
from app.dto.enum import FileLock
from app.repositories.grafana_repository import GrafanaRepository
from app.repositories.logporter_repository import LogporterRepository
from app.schemas.mqtt.manager import MqttManager
from app.services.background import BackgroundService
from app.services.notification_delivery import notification_delivery
from app.services.peer_service import PeerService
from app.utils.utils import logo_to_console


class StartupService:
    NOTIFICATION_POLL_SECONDS = 5

    def __init__(
        self,
        app: FastAPI,
        mqtt: MqttManager,
        bot: Bot | None = None,
        dp: Dispatcher | None = None,
    ) -> None:
        self.app = app
        self.mqtt = mqtt
        self.bot = bot
        self.dp = dp
        self.data_pipe = DataPipeProcess()
        self._init_lock = None
        self._bot_tasks: list[asyncio.Task] = []
        self._instance_tasks: list[asyncio.Task] = []
        self._singleton_tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        self._init_lock = acquire_file_lock(FileLock.INIT)
        if self._init_lock:
            await self._start_once()
        wait_for_file_unlock(FileLock.MQTT_RUN)
        await self._start_every_worker()
        await self._start_telegram_alert_queue()
        if self._init_lock is not None:
            self.data_pipe.start()
        self._start_singleton()

    async def stop(self) -> None:
        try:
            if self._init_lock is not None:
                await asyncio.to_thread(self.data_pipe.stop)
            await self._cancel_tasks(self._bot_tasks, "Telegram bot")
            await self._cancel_tasks(self._instance_tasks, "instance")
            await self._cancel_tasks(self._singleton_tasks, "singleton")
        finally:
            try:
                await asyncio.to_thread(self.mqtt.stop)
            finally:
                self._release_locks()

    def _release_locks(self) -> None:
        if self._init_lock is not None:
            self._init_lock.close()
            self._init_lock = None

    async def _start_once(self) -> None:
        mqtt_run_lock = acquire_file_lock(FileLock.MQTT_RUN)
        await self._init_clickhouse()
        await ControlEmqx().init()
        if runtime.get().pu_ff_grafana_integration_enable:
            asyncio.get_running_loop().run_in_executor(
                None,
                GrafanaRepository.configure_admin_dashboard_permissions,
            )
        self._start_telegram_bot()
        self._sync_local_repository()
        mqtt_run_lock.close()
        self._mark_instance_started()
        logo_to_console()

    async def _start_every_worker(self) -> None:
        self.mqtt.start()
        with BackgroundService() as services:
            services.get_peer_service().refresh_cache()
        self._instance_tasks.append(
            asyncio.create_task(
                self._run_instance_cache_loop(),
                name="run_instance_cache",
            )
        )

    def _start_singleton(self) -> None:
        if runtime.get().pu_ff_federation_enable:
            self._singleton_tasks.append(
                asyncio.create_task(
                    self._run_instance_collection_loop(),
                    name="run_instance_collection",
                )
            )
        self._singleton_tasks.append(
            asyncio.create_task(
                self._run_locked_hourly(
                    FileLock.UPDATE_REPOS,
                    0,
                    self._update_repositories,
                ),
                name="automatic_update_repositories",
            )
        )
        self._singleton_tasks.append(
            asyncio.create_task(
                self._run_locked_hourly(
                    FileLock.UPDATE_REPO,
                    30,
                    self._update_repo,
                ),
                name="automatic_update_repo",
            )
        )
        self._singleton_tasks.append(
            asyncio.create_task(
                self.run_instance_resources(), name="instance_resources"
            )
        )
        if runtime.get().pu_ff_notification_enable:
            self._singleton_tasks.append(
                asyncio.create_task(
                    self._run_scheduled_notifications(),
                    name="scheduled_notifications",
                )
            )
            self._singleton_tasks.append(
                asyncio.create_task(
                    self._run_notifications(),
                    name="notifications",
                )
            )

    async def _init_clickhouse(self) -> None:
        clickhouse_cluster = ClickhouseCluster(
            settings.pu_clickhouse_connection.host,
            settings.pu_clickhouse_connection.user,
            settings.pu_clickhouse_connection.password,
        )
        clickhouse_cluster.migrate(
            settings.pu_clickhouse_connection.database,
            "./clickhouse/migrations",
            cluster_name=None,
            create_db_if_no_exists=True,
            multi_statement=True,
        )

    def _sync_local_repository(self) -> None:
        with BackgroundService(
            AgentBackend(name=settings.pu_domain).generate_agent_token()
        ) as services:
            services.get_repo_service().sync_local_repository_storage()

    def _start_telegram_bot(self) -> None:
        if (
            not runtime.get().pu_ff_telegram_bot_enable
            or not self.bot
            or not self.dp
        ):
            return
        if runtime.get().pu_telegram_bot_mode == "pooling":
            self._bot_tasks.append(
                asyncio.create_task(
                    self._run_polling_bot(), name="run_polling_bot"
                )
            )
        elif runtime.get().pu_telegram_bot_mode == "webhook":
            self._bot_tasks.append(
                asyncio.create_task(
                    self._run_webhook_bot(), name="run_webhook_bot"
                )
            )

    async def _run_polling_bot(self) -> None:
        logging.info("Delete webhook before run polling")
        await self.bot.delete_webhook()
        logging.info("Run polling")
        try:
            await self.dp.start_polling(
                self.bot, allowed_updates=self.dp.resolve_used_update_types()
            )
        except asyncio.CancelledError:
            logging.info("Polling bot task cancelled")
            raise
        except Exception as e:
            logging.error(f"Error in polling bot: {e}")
            raise

    def _webhook_is_current(
        self, webhook_info: WebhookInfo, webhook_url: str
    ) -> bool:
        if webhook_info.url != webhook_url:
            return False
        expected_updates = set(self.dp.resolve_used_update_types())
        current_updates = set(webhook_info.allowed_updates or [])
        return current_updates == expected_updates

    async def _wait_webhook_route(self) -> None:
        inc = 0
        while True:
            result = httpx.post(
                f"{settings.pu_link_prefix_and_v1}/bot",
                headers={"Content-Type": "application/json"},
                timeout=runtime.get().http_timeout(),
            )
            if result.status_code == 422:
                return
            await asyncio.sleep(2)
            if inc > 10:
                msg = "Webhook route not valid"
                raise Exception(msg)
            inc += 1

    async def _install_webhook(self, webhook_url: str) -> None:
        webhook_info = await self.bot.get_webhook_info()
        if self._webhook_is_current(webhook_info, webhook_url):
            logging.info("Telegram webhook already matches this backend")
            return

        if webhook_info.url and webhook_info.url != webhook_url:
            logging.info(
                f"Delete old webhook {webhook_info.url} before set new webhook"
            )
            await self.bot.delete_webhook()

        await self._wait_webhook_route()
        try:
            await self.bot.set_webhook(
                url=webhook_url,
                drop_pending_updates=webhook_info.url != webhook_url,
                allowed_updates=self.dp.resolve_used_update_types(),
            )
            logging.info("Success set TG bot webhook url")
        except aiogram.exceptions.TelegramBadRequest:
            logging.info("Error set TG bot webhook url")

    async def _run_webhook_bot(self) -> None:
        webhook_url = f"{settings.pu_link_prefix_and_v1}/bot"
        await self._install_webhook(webhook_url)

        try:
            while True:
                await asyncio.sleep(60)
                try:
                    webhook_info = await self.bot.get_webhook_info()
                    if not webhook_info.url:
                        logging.warning("Webhook was removed, re-setting...")
                        await self.bot.set_webhook(
                            url=webhook_url,
                            drop_pending_updates=False,
                            allowed_updates=self.dp.resolve_used_update_types(),
                        )
                except Exception as e:
                    logging.warning(f"Webhook check failed: {e}")
        except asyncio.CancelledError:
            logging.info("Webhook bot task cancelled")
            raise

    async def _run_instance_cache_loop(self) -> None:
        while True:
            await asyncio.sleep(60)
            with BackgroundService() as services:
                services.get_peer_service().refresh_cache()

    async def _run_instance_collection_loop(self) -> None:
        while True:
            await asyncio.sleep(self._seconds_until(minute=0))
            if not runtime.get().pu_ff_federation_enable:
                continue
            lock = acquire_file_lock(FileLock.COLLECT_PEERS)
            if lock is None:
                continue
            try:
                with BackgroundService() as services:
                    await services.get_peer_service().collect_all(
                        PeerService.COLLECT_ALL_MAX_DELAY
                    )
                with BackgroundService() as services:
                    service = services.get_peer_service()
                    await service.delete_stale_peers()
                    service.refresh_cache()
            finally:
                lock.close()

    async def _run_locked_hourly(
        self, lock: FileLock, minute: int, work: Callable[[], None]
    ) -> None:
        while True:
            await asyncio.sleep(self._seconds_until(minute=minute))
            lock_fd = acquire_file_lock(lock)
            await asyncio.sleep(10)
            if lock_fd:
                logging.info("Run update with lock")
                try:
                    # Git sync is synchronous and must not stall the
                    # notification loop on this event loop
                    await asyncio.to_thread(work)
                finally:
                    lock_fd.close()
            else:
                logging.info("Skip update without lock")

    def _update_repositories(self) -> None:
        with BackgroundService() as services:
            services.get_unit_service().bulk_update_units_firmware()

    def _update_repo(self) -> None:
        with BackgroundService() as services:
            services.get_repo_service().sync_local_repository_storage(True)

    async def _start_telegram_alert_queue(self) -> None:
        if (
            not runtime.get().pu_ff_notification_enable
            or not runtime.get().pu_ff_telegram_bot_enable
            or not self.bot
        ):
            return
        task = asyncio.create_task(
            notification_delivery.telegram.run(self.bot),
            name="telegram_alert_queue",
        )
        self._singleton_tasks.append(task)
        await notification_delivery.telegram.ready.wait()

    async def run_instance_resources(self) -> None:
        logporter = LogporterRepository()
        lock = None
        try:
            while True:
                await asyncio.sleep(self._seconds_until_next_minute())
                if lock is None:
                    lock = acquire_file_lock(FileLock.INSTANCE_RESOURCES)
                    if lock is None:
                        continue
                work = asyncio.create_task(
                    asyncio.to_thread(
                        self.collect_instance_resources, logporter
                    )
                )
                try:
                    await asyncio.shield(work)
                except asyncio.CancelledError:
                    with suppress(Exception):
                        await work
                    raise
                except Exception:
                    logging.exception("Instance resource sample failed")
        finally:
            if lock is not None:
                lock.close()

    @staticmethod
    def collect_instance_resources(logporter: LogporterRepository) -> None:
        with BackgroundService() as services:
            services.get_instance_resources_service().collect(logporter)

    async def _run_scheduled_notifications(self) -> None:
        # Once a minute, the resolution of scheduled_notification_time.
        # The write is synchronous and must not stall the processing loop.
        while True:
            await asyncio.sleep(self._seconds_until_next_minute())
            if not runtime.get().pu_ff_notification_enable:
                continue
            lock = acquire_file_lock(FileLock.NOTIFICATION_SCHEDULE)
            if lock is None:
                continue
            try:
                await asyncio.to_thread(self._generate_scheduled)
            except Exception:
                logging.exception("Scheduled notifications failed")
            finally:
                lock.close()

    def _generate_scheduled(self) -> None:
        with BackgroundService() as services:
            services.get_notification_service().generate_scheduled()

    async def _run_notifications(self) -> None:
        # One worker processes the table, the others retry the lock
        while True:
            if not runtime.get().pu_ff_notification_enable:
                await asyncio.sleep(self.NOTIFICATION_POLL_SECONDS)
                continue
            lock = acquire_file_lock(FileLock.NOTIFICATION_PROCESS)
            if lock is None:
                await asyncio.sleep(self.NOTIFICATION_POLL_SECONDS)
                continue
            try:
                await self._process_notifications()
            except asyncio.CancelledError:
                raise
            except Exception:
                logging.exception("Notification processing failed")
                await asyncio.sleep(self.NOTIFICATION_POLL_SECONDS)
            finally:
                lock.close()

    async def _process_notifications(self) -> None:
        while True:
            with BackgroundService() as services:
                handled = (
                    await services.get_notification_service().process_pending()
                )
            if handled < runtime.get().pu_notification_data_pipe_alert_batch:
                await asyncio.sleep(self.NOTIFICATION_POLL_SECONDS)

    def _mark_instance_started(self) -> None:
        from app.configs.db import get_hand_session
        from app.services.instance_settings_service import (
            InstanceSettingsService,
        )

        with get_hand_session() as db:
            InstanceSettingsService.mark_started(db)

    def _seconds_until_next_minute(self) -> float:
        now = datetime.now(UTC)
        next_run = (now + timedelta(minutes=1)).replace(
            second=0, microsecond=0
        )
        return (next_run - now).total_seconds()

    def _seconds_until(self, *, minute: int) -> float:
        now = datetime.now(UTC)
        next_run = now.replace(minute=minute, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(hours=1)
        return (next_run - now).total_seconds()

    async def _cancel_tasks(
        self, tasks: list[asyncio.Task], label: str
    ) -> None:
        if tasks:
            logging.info(f"Stopping {label} tasks...")
        for task in tasks:
            if not task.done():
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
        tasks.clear()
