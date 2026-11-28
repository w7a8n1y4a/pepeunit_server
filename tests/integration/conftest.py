import json
import subprocess
import sys
import time
import tomllib
import uuid as uuid_pkg
from collections.abc import Iterator
from datetime import UTC, datetime
from importlib.metadata import distribution
from pathlib import Path

import pepeunit_client
import pytest
from clickhouse_driver import Client
from pepeunit_client.file_manager import FileManager
from sqlmodel import Session

from app import settings
from app.configs.clickhouse import get_clickhouse_client
from app.configs.db import get_session
from app.domain.unit_model import Unit
from app.dto.enum import StaticRepoFileName
from app.services.utils import get_topic_name
from tests.client import mqtt
from tests.integration.helpers.cleanup import (
    clear_integration_data,
    ensure_own_peer,
)
from tests.integration.helpers.frontend_state import preserve_demo_dashboard
from tests.integration.helpers.http import patch_input_state
from tests.integration.helpers.owner import preserve_instance_owner
from tests.integration.helpers.wait import wait_until


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
def integration_started_at() -> datetime:
    return datetime.now(UTC)


@pytest.fixture(scope="session")
def sdk_distribution() -> None:
    lock_file = Path(__file__).resolve().parents[2] / "uv.lock"
    with lock_file.open("rb") as lock:
        packages = tomllib.load(lock)["package"]
    installed = distribution("pepeunit-client")
    locked = next(item for item in packages if item["name"] == installed.name)
    assert locked["source"] == {"registry": "https://test.pypi.org/simple/"}
    assert installed.version == locked["version"]
    assert installed.read_text("direct_url.json") is None
    entry_point = installed.locate_file("pepeunit_client/__init__.py")
    assert Path(pepeunit_client.__file__).resolve() == entry_point.resolve()


@pytest.fixture(scope="session", autouse=True)
def keep_instance_owner(clean_leftovers, database) -> None:  # noqa: ARG001
    """Snapshot the owner after leftover test users are removed."""
    with preserve_instance_owner(database):
        yield


@pytest.fixture(scope="session", autouse=True)
def clean_leftovers(
    database: Session, cc: Client, sdk_distribution: None
) -> Iterator[None]:
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
    PROCESS_LOG = "process.log"
    STOP_TIMEOUT_SECONDS = 5

    def __init__(self) -> None:
        self.processes: dict[uuid_pkg.UUID, subprocess.Popen[bytes]] = {}

    def start_units(self, units: list[Unit]) -> None:
        program = Path(mqtt.__file__).resolve()
        for unit in units:
            directory = mqtt.UNIT_ROOT / str(unit.uuid)
            env_file = (directory / StaticRepoFileName.ENV).resolve()
            with (directory / self.PROCESS_LOG).open("wb") as log:
                self.processes[unit.uuid] = subprocess.Popen(
                    [sys.executable, str(program), str(env_file)],
                    stdin=subprocess.DEVNULL,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                )

    def require_running(self) -> None:
        for unit_uuid, process in self.processes.items():
            if (status := process.poll()) is not None:
                log = mqtt.UNIT_ROOT / str(unit_uuid) / self.PROCESS_LOG
                raise RuntimeError(
                    f"Unit {unit_uuid} exited with status {status}. See {log}"
                )

    def last_startup_log(self, unit_uuid: uuid_pkg.UUID) -> str | None:
        path = mqtt.UNIT_ROOT / str(unit_uuid) / mqtt.CounterUnit.LOG_FILENAME
        text = None
        for entry in FileManager.iter_ndjson(path):
            if entry["text"].startswith(mqtt.CounterUnit.STARTUP_LOG_PREFIX):
                text = entry["text"]
        return text

    def wait_for_counter_input(
        self, token: str, unit_uuid: uuid_pkg.UUID, input_uuid: uuid_pkg.UUID
    ) -> None:
        path = (
            mqtt.UNIT_ROOT
            / str(unit_uuid)
            / mqtt.CounterUnit.INPUT_LOG_FILENAME
        )
        topic = get_topic_name(input_uuid)
        sent_after = time.time()

        def received() -> bool:
            self.require_running()
            if path.is_file():
                record = json.loads(path.read_text(encoding="utf-8"))
                if (
                    record["input_topic"] == topic
                    and record["timestamp"] >= sent_after
                ):
                    return True
            assert patch_input_state(token, input_uuid, "0") == 200
            return False

        wait_until(
            received,
            interval=0.5,
            message=f"Unit {unit_uuid} did not receive a counter input",
        )

    def stop(self) -> None:
        for process in self.processes.values():
            process.terminate()
        for process in self.processes.values():
            try:
                process.wait(timeout=self.STOP_TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


@pytest.fixture(scope="session")
def client_emulator(clean_leftovers: None) -> Iterator[ClientEmulator]:
    emulator = ClientEmulator()
    yield emulator
    emulator.stop()
