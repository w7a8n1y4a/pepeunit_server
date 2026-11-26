import asyncio
import json
import threading
import time
from pathlib import Path

from pepeunit_client import PepeunitClient
from pepeunit_client.enums import BaseOutputTopicType, RestartMode

from app.dto.enum import StaticRepoFileName

COUNTER_INPUT = "input_counter"
COUNTER_OUTPUT = "output_counter"
UNIT_ROOT = Path("tmp/test_units")


class EmulatorPepeunitClient(PepeunitClient):
    def _restart_program(self) -> None:
        self._reload_after_update()


class MQTTClient:
    def __init__(self, unit):
        self.directory = UNIT_ROOT / str(unit.uuid)
        self.running = threading.Event()
        self.running.set()
        self.client = EmulatorPepeunitClient(
            env_file_path=str(self.directory / StaticRepoFileName.ENV),
            log_file_path=str(self.directory / "log.json"),
            enable_mqtt=True,
            restart_mode=RestartMode.ENV_TOPICS_ONLY,
            ff_version_check_enable=False,
            ff_console_log_enable=False,
        )
        self.client.set_mqtt_input_handler(self.mqtt_input_handler)
        self.connected_since = None

    def mqtt_input_handler(self, client: PepeunitClient, msg) -> None:
        if client.io_topics.input_name(msg.topic) != COUNTER_INPUT:
            return
        if json.loads(msg.payload) != 0:
            return
        record = {
            "value": 0,
            "input_topic": msg.topic,
            "timestamp": time.time(),
        }
        destination = self.directory / "log_state.json"
        pending = destination.with_suffix(".tmp")
        pending.write_text(json.dumps(record), encoding="utf-8")
        pending.replace(destination)
        client.publish_to_topics(COUNTER_OUTPUT, "0")

    def report_state(self) -> None:
        now = time.time()
        if self.connected_since is None:
            self.connected_since = now
            self.client._last_state_send = 0
        interval = self.client.settings.PU_STATE_SEND_INTERVAL
        gap = (
            min(interval, 2)
            if now - self.connected_since < interval
            else interval
        )
        if now - self.client._last_state_send < gap:
            return
        payload = json.dumps(self.client.get_system_state())
        info = self.client.publish_to_topics(
            BaseOutputTopicType.STATE.value, payload
        )
        if info is not None and info.rc == 0:
            self.client._last_state_send = now

    async def run(self) -> None:
        count = 0
        try:
            self.client.connect()
            while self.running.is_set():
                if self.client.mqtt_client._client.is_connected():
                    self.client.publish_to_topics(
                        COUNTER_OUTPUT, str(count // 10)
                    )
                    self.report_state()
                    count += 1
                else:
                    self.connected_since = None
                await asyncio.sleep(0.25)
        finally:
            self.client.mqtt_client.disconnect()

    def stop(self) -> None:
        self.running.clear()
