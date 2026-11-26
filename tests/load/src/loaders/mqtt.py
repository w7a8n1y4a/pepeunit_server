import asyncio
import logging
import time
import uuid
from dataclasses import dataclass
from multiprocessing.managers import AcquirerProxy, BarrierProxy, ValueProxy
from threading import BrokenBarrierError

from paho.mqtt import client as mqtt_client

from app.dto.enum import TypeInputValue
from app.utils.utils import generate_random_string
from tests.load.src.dto.config import LoadTestConfig

MQTT_TIMEOUT = 10


@dataclass(frozen=True)
class MQTTLoadStartup:
    connection_interval: float
    next_connection: ValueProxy
    lock: AcquirerProxy
    barrier: BarrierProxy
    deadline: float

    async def wait_for_connection(self) -> None:
        if not self.connection_interval:
            return
        while True:
            with self.lock:
                now = time.monotonic()
                delay = self.next_connection.get() - now
                if delay <= 0:
                    self.next_connection.set(now + self.connection_interval)
                    return
            await asyncio.sleep(delay)

    async def wait_for_publish(self) -> None:
        try:
            await asyncio.to_thread(
                self.barrier.wait, max(0, self.deadline - time.monotonic())
            )
        except BrokenBarrierError as exc:
            msg = "MQTT load workers did not connect all units in time"
            raise TimeoutError(msg) from exc


class MQTTLoadTest:
    CONNECT_ATTEMPTS = 3
    start_time: float

    def __init__(self, config: LoadTestConfig, units: list[dict]):
        self.config = config
        self.units = units
        self.clients: list[mqtt_client.Client] = []

    async def connect_unit(
        self, unit: dict, startup: MQTTLoadStartup | None = None
    ) -> mqtt_client.Client:
        env = unit["env"]
        target = f"{env['PU_MQTT_HOST']}:{env['PU_MQTT_PORT']}"
        attempt = 1
        while True:
            if startup is not None:
                await startup.wait_for_connection()
            try:
                return await self.connect_mqtt(unit)
            except OSError as exc:
                detail = str(exc) or f"no CONNACK within {MQTT_TIMEOUT}s"
                if attempt == self.CONNECT_ATTEMPTS:
                    msg = (
                        f"MQTT connection to {target} failed for unit "
                        f"{unit['uuid']} after {attempt} attempts: {detail}"
                    )
                    raise RuntimeError(msg) from exc
                logging.warning(
                    "MQTT connection to %s failed for unit %s (attempt %s/%s): %s",
                    target,
                    unit["uuid"],
                    attempt,
                    self.CONNECT_ATTEMPTS,
                    detail,
                )
                await asyncio.sleep(1)
                attempt += 1

    async def connect_mqtt(self, unit: dict) -> mqtt_client.Client:
        env = unit["env"]
        loop = asyncio.get_running_loop()
        connected = loop.create_future()
        client = mqtt_client.Client(
            mqtt_client.CallbackAPIVersion.VERSION2,
            client_id=str(uuid.uuid4()),
            reconnect_on_failure=False,
        )

        def finish_connect(
            reason_code: mqtt_client.ReasonCode, disconnected: bool = False
        ) -> None:
            if connected.done():
                return
            if disconnected:
                connected.set_exception(
                    ConnectionError(
                        f"Disconnected before CONNACK: {reason_code}"
                    )
                )
            else:
                connected.set_result(reason_code)

        def on_connect(_client, _userdata, _flags, reason_code, _properties):
            loop.call_soon_threadsafe(finish_connect, reason_code)

        def on_disconnect(
            _client, _userdata, _flags, reason_code, _properties
        ):
            loop.call_soon_threadsafe(finish_connect, reason_code, True)

        client.username_pw_set(env["PU_AUTH_TOKEN"], "")
        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        try:
            status = await asyncio.to_thread(
                client.connect, env["PU_MQTT_HOST"], int(env["PU_MQTT_PORT"])
            )
            if status != mqtt_client.MQTT_ERR_SUCCESS:
                raise ConnectionError(mqtt_client.error_string(status))
            client.loop_start()
            reason = await asyncio.wait_for(connected, timeout=MQTT_TIMEOUT)
            if reason.is_failure:
                msg = f"MQTT connection rejected for unit {unit['uuid']}: {reason}"
                raise RuntimeError(msg)
        except BaseException:
            client.disconnect()
            client.loop_stop()
            raise
        self.clients.append(client)
        return client

    async def publish(self, client: mqtt_client.Client, unit: dict) -> int:
        topic = unit["output"]["topic"]
        qos = unit["output"].get("qos", 0)
        message = generate_random_string(self.config.message_size)
        count = 0
        last_message = None

        while time.monotonic() - self.start_time < self.config.duration:
            if not client.is_connected():
                msg = f"MQTT disconnected for unit {unit['uuid']}"
                raise RuntimeError(msg)
            if self.config.value_type == TypeInputValue.TEXT:
                if count % self.config.duplicate_count == 0:
                    message = generate_random_string(self.config.message_size)
            else:
                message = count // self.config.duplicate_count

            last_message = client.publish(topic, message, qos=qos)
            if last_message.rc != mqtt_client.MQTT_ERR_SUCCESS:
                msg = (
                    f"MQTT publish failed for unit {unit['uuid']}: "
                    f"{mqtt_client.error_string(last_message.rc)}"
                )
                raise RuntimeError(msg)
            count += 1
            next_send = self.start_time + count / self.config.rps
            await asyncio.sleep(max(0, next_send - time.monotonic()))

        # QoS 0 success means queued. Flush the final packet before disconnecting.
        if last_message is not None:
            try:
                async with asyncio.timeout(MQTT_TIMEOUT):
                    while not last_message.is_published():
                        await asyncio.sleep(0.01)
            except TimeoutError as exc:
                msg = f"MQTT publish timed out for unit {unit['uuid']}"
                raise RuntimeError(msg) from exc
        return count

    async def start_test(self, startup: MQTTLoadStartup) -> int:
        try:
            for unit in self.units:
                await self.connect_unit(unit, startup)
            await startup.wait_for_publish()
            self.start_time = time.monotonic()
            async with asyncio.TaskGroup() as group:
                tasks = [
                    group.create_task(self.publish(connection, unit))
                    for connection, unit in zip(
                        self.clients, self.units, strict=True
                    )
                ]
            return sum(task.result() for task in tasks)
        finally:
            self.close()

    def close(self) -> None:
        for client in self.clients:
            client.disconnect()
        for client in self.clients:
            client.loop_stop()
