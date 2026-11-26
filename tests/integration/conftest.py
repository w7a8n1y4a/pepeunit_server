import asyncio
from concurrent.futures import ThreadPoolExecutor

import pytest
from clickhouse_driver import Client
from sqlmodel import Session

from app import settings
from app.configs.clickhouse import get_clickhouse_client
from app.configs.db import get_session
from tests.client.mqtt import MQTTClient
from tests.integration.helpers.cleanup import (
    clear_integration_data,
    ensure_own_peer,
)
from tests.integration.helpers.frontend_state import preserve_demo_dashboard
from tests.integration.helpers.owner import preserve_instance_owner


@pytest.fixture(scope="session")
def database() -> Session:
    generator = get_session()
    session = next(generator)
    yield session
    generator.close()


@pytest.fixture(scope="session")
def cc() -> Client:
    return next(get_clickhouse_client())


@pytest.fixture(scope="session", autouse=True)
def keep_instance_owner(clean_leftovers, database) -> None:  # noqa: ARG001
    """Snapshot the owner after leftover test users are removed."""
    with preserve_instance_owner(database):
        yield


@pytest.fixture(scope="session", autouse=True)
def clean_leftovers(database, cc) -> None:
    clear_integration_data(database)
    yield
    if settings.pu_test_integration_clear_data:
        clear_integration_data(database)
        return
    ensure_own_peer(database)
    preserve_demo_dashboard(database, cc)


@pytest.fixture(scope="session")
def test_hash() -> str:
    from tests.integration.helpers.names import TEST_HASH

    return TEST_HASH


class ClientEmulator:
    def __init__(self) -> None:
        self.executor = ThreadPoolExecutor()
        self.clients = {}
        self.futures = []

    def start_units(self, units) -> None:
        for unit in units:
            client = MQTTClient(unit)
            self.clients[unit.uuid] = client
            self.futures.append(
                self.executor.submit(asyncio.run, client.run())
            )

    def require_running(self) -> None:
        for future in self.futures:
            if future.done():
                future.result()
                raise RuntimeError("MQTT emulator stopped unexpectedly")

    def stop(self) -> None:
        for client in self.clients.values():
            client.stop()
        self.executor.shutdown(wait=True)


@pytest.fixture(scope="session")
def client_emulator():
    emulator = ClientEmulator()
    yield emulator
    emulator.stop()
