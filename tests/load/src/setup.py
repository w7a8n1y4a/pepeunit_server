import asyncio
import time

import httpx
from paho.mqtt import client as mqtt

from tests.load.src.clients.rest_client import HTTP_TIMEOUT, RestClient
from tests.load.src.dto.config import LoadTestConfig
from tests.load.src.loaders.mqtt import MQTTLoadTest


class MqttTestPreparation:
    PROBE_BEFORE_LOAD = "1"
    PROBE_AFTER_LOAD = "2"
    probe: dict

    def __init__(self, config: LoadTestConfig):
        self.config = config
        self.rest_client = RestClient(config)
        self.broker = httpx.AsyncClient(
            base_url=f"{config.mqtt_api_url}/api/v5/", timeout=HTTP_TIMEOUT
        )
        self.units = []

    async def setup(self) -> None:
        response = await self.broker.post(
            "login",
            json={
                "username": self.config.mqtt_admin,
                "password": self.config.mqtt_password,
            },
        )
        response.raise_for_status()
        self.broker.headers["Authorization"] = (
            f"Bearer {response.json()['token']}"
        )
        await self.rest_client.authenticate()
        repo = await self.rest_client.get_repo()
        self.units, self.probe = await self.rest_client.create_units(repo)

        deadline = time.monotonic() + 30
        while not await self.is_backend_subscribed():
            if time.monotonic() >= deadline:
                msg = "Message Processor did not subscribe to the instance MQTT topics"
                raise TimeoutError(msg)
            await asyncio.sleep(1)

        await self.wait_for_processor(self.PROBE_BEFORE_LOAD)

    async def wait_for_processor(self, value: str) -> None:
        loader = MQTTLoadTest(self.config, [self.probe])
        topic = self.probe["output"]["topic"]
        qos = self.probe["output"].get("qos", 0)
        try:
            client = await loader.connect_unit(self.probe)
            deadline = time.monotonic() + 60
            while await self.rest_client.output_state(self.probe) != value:
                if time.monotonic() >= deadline:
                    msg = f"Message Processor did not store probe {value!r} on {topic}"
                    raise TimeoutError(msg)
                result = client.publish(topic, value, qos=qos)
                if result.rc != mqtt.MQTT_ERR_SUCCESS:
                    msg = f"MQTT probe publish failed: {mqtt.error_string(result.rc)}"
                    raise RuntimeError(msg)
                await asyncio.sleep(1)
        finally:
            loader.close()

    async def is_backend_subscribed(self) -> bool:
        domain = self.units[0]["env"]["PU_DOMAIN"]
        topic = f"{domain}/+"
        response = await self.broker.get(
            "subscriptions",
            params={"clientid": domain, "topic": topic, "qos": 1},
        )
        response.raise_for_status()
        return any(
            item["clientid"] == domain
            and item["topic"] == topic
            and item["qos"] == 1
            for item in response.json()["data"]
        )

    async def get_tcp_listener(self) -> dict:
        response = await self.broker.get("listeners/tcp:default")
        response.raise_for_status()
        return response.json()

    async def close(self) -> None:
        try:
            await self.rest_client.close()
        finally:
            await self.broker.aclose()
