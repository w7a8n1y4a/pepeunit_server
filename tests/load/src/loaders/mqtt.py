import asyncio
import time
import uuid

from paho.mqtt import client as mqtt_client

from app.utils.utils import generate_random_string
from tests.load.src.dto.config import LoadTestConfig

MQTT_TIMEOUT = 10
OUTPUT_TOPIC = "output/pepeunit"


class MQTTLoadTest:
    start_time: float

    def __init__(self, config: LoadTestConfig, units):
        self.config = config
        self.units = units
        self.clients = []

    async def connect_mqtt(self, token, broker, port):
        loop = asyncio.get_running_loop()
        connected = loop.create_future()
        client = mqtt_client.Client(
            mqtt_client.CallbackAPIVersion.VERSION2,
            client_id=str(uuid.uuid4()),
            reconnect_on_failure=False,
        )
        self.clients.append(client)

        def finish_connect(reason_code):
            if connected.done():
                return
            if reason_code.is_failure:
                connected.set_exception(
                    RuntimeError(f"MQTT connection rejected: {reason_code}")
                )
            else:
                connected.set_result(None)

        def on_connect(client, userdata, flags, reason_code, properties):
            loop.call_soon_threadsafe(finish_connect, reason_code)

        client.username_pw_set(token, "")
        client.on_connect = on_connect
        client.connect_async(broker, int(port))
        client.loop_start()
        await asyncio.wait_for(connected, timeout=MQTT_TIMEOUT)
        return client

    async def publish(self, client, unit):
        topic_dict = self.get_dict_by_topic(unit, OUTPUT_TOPIC)
        topic = f"{unit['env']['PU_DOMAIN']}/{topic_dict['uuid']}/pepeunit"
        message = generate_random_string(self.config.message_size)
        count = 0
        last_message = None

        while time.monotonic() - self.start_time < self.config.duration:
            if not client.is_connected():
                raise RuntimeError(
                    f"MQTT disconnected for unit {unit['uuid']}"
                )
            if self.config.value_type == "Text":
                if count % self.config.duplicate_count == 0:
                    message = generate_random_string(self.config.message_size)
            else:
                message = count // self.config.duplicate_count

            last_message = client.publish(topic, message)
            if last_message.rc != mqtt_client.MQTT_ERR_SUCCESS:
                raise RuntimeError(
                    f"MQTT publish failed for unit {unit['uuid']}: "
                    f"{mqtt_client.error_string(last_message.rc)}"
                )
            count += 1
            await asyncio.sleep(1 / self.config.rps)

        # QoS 0 success means queued. Flush the final packet before disconnecting.
        if last_message is not None:
            try:
                async with asyncio.timeout(MQTT_TIMEOUT):
                    while not last_message.is_published():
                        await asyncio.sleep(0.01)
            except TimeoutError as exc:
                raise RuntimeError(
                    f"MQTT publish timed out for unit {unit['uuid']}"
                ) from exc
        return count

    async def start_test(self):
        for unit in self.units:
            self.validate_unit(unit)
        try:
            async with asyncio.TaskGroup() as group:
                connections = [
                    group.create_task(
                        self.connect_mqtt(
                            unit["env"]["PU_AUTH_TOKEN"],
                            unit["env"]["PU_MQTT_HOST"],
                            unit["env"]["PU_MQTT_PORT"],
                        )
                    )
                    for unit in self.units
                ]

            self.start_time = time.monotonic()
            async with asyncio.TaskGroup() as group:
                tasks = [
                    group.create_task(self.publish(connection.result(), unit))
                    for connection, unit in zip(connections, self.units)
                ]
            return sum(task.result() for task in tasks)
        finally:
            for client in self.clients:
                client.disconnect()
            for client in self.clients:
                client.loop_stop()

    @staticmethod
    def get_dict_by_topic(unit, topic_name):
        for item in unit.get("unit_ios") or []:
            if (
                item.get("topic_name") == topic_name
                and item.get("type") == "Output"
            ):
                return item
        available = [
            item.get("topic_name") for item in unit.get("unit_ios") or []
        ]
        raise ValueError(
            f"Unit {unit.get('uuid')} has no output topic {topic_name!r}; "
            f"available topics: {available}"
        )

    @classmethod
    def validate_unit(cls, unit):
        env = unit.get("env")
        required = (
            "PU_AUTH_TOKEN",
            "PU_MQTT_HOST",
            "PU_MQTT_PORT",
            "PU_DOMAIN",
        )
        if not isinstance(env, dict) or any(
            not env.get(key) for key in required
        ):
            raise ValueError(
                f"Unit {unit.get('uuid')} has missing or invalid MQTT env"
            )
        cls.get_dict_by_topic(unit, OUTPUT_TOPIC)
