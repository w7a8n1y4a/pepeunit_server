import json
import logging
import uuid as uuid_pkg

from paho.mqtt import client as mqtt

from app import runtime, settings
from app.configs.error_codes import ErrorCode
from app.configs.errors import MqttError
from app.dto.agent.abc import AgentBackend
from app.dto.enum import MqttQos


class MqttManager:
    RECONNECT_DELAY = 6

    def __init__(self) -> None:
        self.client: mqtt.Client | None = None

    def start(self) -> None:
        if self.client is not None:
            return

        client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=uuid_pkg.uuid4().hex,
            protocol=mqtt.MQTTv5,
        )
        client.username_pw_set(
            AgentBackend(name=settings.pu_domain).generate_agent_token(),
            password="",
        )
        client.reconnect_delay_set(self.RECONNECT_DELAY, self.RECONNECT_DELAY)
        client.enable_logger()

        host, port = settings.pu_mqtt_host, settings.pu_mqtt_port
        logging.info("Connect to MQTT broker %s:%s", host, port)
        client.connect_async(
            host,
            port,
            keepalive=runtime.get().pu_mqtt_keepalive,
            clean_start=True,
        )
        result = client.loop_start()
        if result != mqtt.MQTT_ERR_SUCCESS:
            message = f"Cannot start MQTT client: {mqtt.error_string(result)}"
            raise MqttError(
                message, code=ErrorCode.MQTT_PUBLISH_LOOP_NOT_INITIALIZED
            )

        self.client = client

    def stop(self) -> None:
        client = self.client
        if client is None:
            return
        self.client = None

        try:
            client.disconnect()
        finally:
            client.loop_stop()

    def publish(
        self, topic: str, msg: dict | str | bytes, qos: MqttQos
    ) -> None:
        client = self.client
        if client is None:
            message = f"Cannot publish to {topic}: MQTT client is not started"
            raise MqttError(
                message, code=ErrorCode.MQTT_PUBLISH_LOOP_NOT_INITIALIZED
            )

        disconnected = (
            f"Cannot publish to {topic}: MQTT client is disconnected"
        )
        if not client.is_connected():
            raise MqttError(
                disconnected, code=ErrorCode.MQTT_PUBLISH_DISCONNECTED
            )

        try:
            payload = json.dumps(msg) if isinstance(msg, dict) else msg
            result = client.publish(topic, payload, qos=qos.value).rc
        except Exception as error:
            message = f"Cannot publish to {topic}: {error}"
            raise MqttError(
                message, code=ErrorCode.MQTT_PUBLISH_FAILED
            ) from error

        if result == mqtt.MQTT_ERR_SUCCESS:
            return
        if result == mqtt.MQTT_ERR_NO_CONN:
            if qos == MqttQos.AT_LEAST_ONCE:
                return
            raise MqttError(
                disconnected, code=ErrorCode.MQTT_PUBLISH_DISCONNECTED
            )

        message = f"Cannot publish to {topic}: {mqtt.error_string(result)}"
        raise MqttError(message, code=ErrorCode.MQTT_PUBLISH_FAILED)


mqtt_manager = MqttManager()
