import queue
import uuid as uuid_pkg
from contextlib import contextmanager

from paho.mqtt import client as mqtt
from pepeunit_client import PepeunitClient
from pydantic import JsonValue

from app.dto.enum import LogLevel
from app.schemas.pydantic.unit import UnitLogFilter
from app.schemas.pydantic.unit_io import (
    LinkCreate,
    UnitIOTopicRead,
    UnitIOTopicsRead,
)
from app.services.unit_service import UnitService
from tests.integration.helpers.wait import wait_until

MQTT_TIMEOUT = 10


class MQTTProbe:
    def __init__(self, env: dict[str, JsonValue], topics: UnitIOTopicsRead):
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"probe-{uuid_pkg.uuid4()}",
            protocol=mqtt.MQTTv5,
            reconnect_on_failure=False,
        )
        self.client.username_pw_set(env["PU_AUTH_TOKEN"], "")
        self.client.on_connect = self.on_connect
        self.client.on_subscribe = self.on_subscribe
        self.client.on_message = self.on_message
        self.env = env
        self.topics = topics
        self.connack = queue.Queue()
        self.subacks = queue.Queue()
        self.messages = queue.Queue()

    @classmethod
    def from_unit(
        cls, service: UnitService, unit_uuid: uuid_pkg.UUID
    ) -> MQTTProbe:
        return cls(
            service.get_env(unit_uuid), service.get_io_topics(unit_uuid)
        )

    def on_connect(
        self, _client, _userdata, _flags, reason_code, _properties
    ) -> None:
        self.connack.put(reason_code)

    def on_subscribe(
        self, _client, _userdata, mid, reason_codes, _properties
    ) -> None:
        self.subacks.put((mid, reason_codes))

    def on_message(
        self, _client, _userdata, message: mqtt.MQTTMessage
    ) -> None:
        self.messages.put(message)

    @contextmanager
    def connected(self):
        self.client.connect(
            self.env["PU_MQTT_HOST"],
            self.env["PU_MQTT_PORT"],
            keepalive=self.env["PU_MQTT_KEEPALIVE"],
        )
        self.client.loop_start()
        try:
            reason = self.connack.get(timeout=MQTT_TIMEOUT)
            assert not reason.is_failure, f"CONNECT: {reason}"
            yield self
        finally:
            self.client.disconnect()
            self.client.loop_stop()

    def subscribe(self, topic: UnitIOTopicRead) -> None:
        qos = topic.qos.value
        status, mid = self.client.subscribe(topic.topic, qos=qos)
        assert status == mqtt.MQTT_ERR_SUCCESS
        received_mid, reasons = self.subacks.get(timeout=MQTT_TIMEOUT)
        assert received_mid == mid
        assert [reason.value for reason in reasons] == [qos]

    def publish(self, topic: UnitIOTopicRead, payload: bytes) -> None:
        result = self.client.publish(topic.topic, payload, qos=topic.qos.value)
        result.wait_for_publish(timeout=MQTT_TIMEOUT)
        assert result.is_published(), (
            f"publish did not finish for {topic.topic}"
        )

    def receive(self, timeout: float = 0.5) -> mqtt.MQTTMessage | None:
        try:
            return self.messages.get(timeout=timeout)
        except queue.Empty:
            return None

    def received_payload(self, topic: UnitIOTopicRead, payload: bytes) -> bool:
        while message := self.receive():
            if message.topic == topic.topic and message.payload == payload:
                return True
        return False


def topic_uuid(topic: UnitIOTopicRead) -> uuid_pkg.UUID:
    return uuid_pkg.UUID(topic.topic.rsplit("/", 1)[-1])


def link_between(
    output: UnitIOTopicRead, input_: UnitIOTopicRead
) -> LinkCreate:
    return LinkCreate(
        output_uuid=topic_uuid(output), input_uuid=topic_uuid(input_)
    )


def publish_and_receive(
    sender: MQTTProbe,
    output: UnitIOTopicRead,
    payload: bytes,
    receiver: MQTTProbe,
) -> mqtt.MQTTMessage:
    message = None

    def delivered() -> bool:
        nonlocal message
        sender.publish(output, payload)
        message = receiver.receive()
        return message is not None

    wait_until(
        delivered, interval=0.25, message=f"No delivery from {output.topic}"
    )
    assert message is not None
    return message


def wait_for_live_log(
    client: PepeunitClient, service: UnitService, unit_uuid: uuid_pkg.UUID
) -> None:
    filters = UnitLogFilter.unlimited(uuid=unit_uuid, level=[LogLevel.INFO])
    text = f"SDK connected to the log pipeline {unit_uuid}"

    def stored() -> bool:
        if any(entry.text == text for entry in service.log_list(filters)[1]):
            return True
        client.logger.info(text)
        return False

    wait_until(
        stored,
        timeout=30,
        message="MessageProcessor did not store a live SDK log",
    )
