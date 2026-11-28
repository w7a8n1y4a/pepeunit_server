import json
import signal
import sys
import time
import uuid as uuid_pkg
from pathlib import Path
from types import FrameType

from paho.mqtt.client import MQTT_ERR_SUCCESS, MQTTMessage
from pepeunit_client import PepeunitClient

COUNTER_INPUT = "input_counter"
COUNTER_OUTPUT = "output_counter"
UNIT_ROOT = Path("tmp/test_units")


class CounterUnit:
    CYCLE_SECONDS = 0.25
    MESSAGES_PER_VALUE = 10
    LOG_FILENAME = "log.ndjson"
    INPUT_LOG_FILENAME = "log_state.json"
    STARTUP_LOG_PREFIX = "Counter unit started: "

    def __init__(self, env_file: Path) -> None:
        self.directory = env_file.parent
        self.stopping = False
        self.count = 0
        self.client = PepeunitClient(
            env_file_path=str(env_file),
            log_file_path=str(self.directory / self.LOG_FILENAME),
            enable_mqtt=True,
            cycle_speed=self.CYCLE_SECONDS,
            ff_version_check_enable=False,
            ff_console_log_enable=False,
        )
        self.client.set_mqtt_input_handler(self.handle_input)
        self.client.set_output_handler(self.publish_counter)

    def handle_input(
        self, client: PepeunitClient, message: MQTTMessage
    ) -> None:
        if client.io_topics.input_name(message.topic) != COUNTER_INPUT:
            return
        if json.loads(message.payload) != 0:
            return
        record = {
            "value": 0,
            "input_topic": message.topic,
            "timestamp": time.time(),
        }
        destination = self.directory / self.INPUT_LOG_FILENAME
        pending = destination.with_suffix(".tmp")
        pending.write_text(json.dumps(record), encoding="utf-8")
        pending.replace(destination)
        client.publish_to_topics(COUNTER_OUTPUT, "0")

    def publish_counter(self, client: PepeunitClient) -> None:
        if self.stopping:
            client.stop_main_cycle()
            return
        value = self.count // self.MESSAGES_PER_VALUE
        publish = client.publish_to_topics(COUNTER_OUTPUT, str(value))
        if publish is not None and publish.rc == MQTT_ERR_SUCCESS:
            self.count += 1

    def request_stop(self, signum: int, frame: FrameType | None) -> None:
        self.stopping = True

    def run(self) -> None:
        signal.signal(signal.SIGTERM, self.request_stop)
        mqtt = self.client.mqtt_client
        assert mqtt is not None
        try:
            self.client.connect()
            self.client.logger.info(
                f"{self.STARTUP_LOG_PREFIX}{uuid_pkg.uuid4()}", file_only=True
            )
            self.client.run_main_cycle()
        finally:
            mqtt.disconnect()


if __name__ == "__main__":
    CounterUnit(Path(sys.argv[1])).run()
