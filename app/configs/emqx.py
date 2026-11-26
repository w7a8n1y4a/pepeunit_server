import logging
import time
from hashlib import sha256

import httpx
from pydantic import JsonValue
from redis.asyncio import Redis
from redis.asyncio.connection import ConnectionPool, parse_url

from app import runtime, settings
from app.dto.agent.abc import AgentBackend
from app.dto.enum import MqttAction


class ControlEmqx:
    AUTH_CACHE_MAX_SIZE = 1024
    AUTH_CACHE_TTL = "10m"
    REDIS_ACL_PREFIX = "mqtt_acl"
    REDIS_DEFAULT_PORT = 6379
    REDIS_POOL_SIZE = 8

    current_link: str

    @staticmethod
    def get_emqx_link():
        if settings.pu_mqtt_secure:
            return f"{settings.pu_mqtt_http_type}://{settings.pu_mqtt_host}"
        return f"{settings.pu_mqtt_http_type}://{settings.pu_mqtt_host}:{settings.pu_mqtt_api_port}"

    def __init__(self):
        self.current_link = self.get_emqx_link()

        logging.info(f"Check state EMQX Broker {self.current_link}")

        code = 500
        inc = 0
        while not self.is_broker_reachable(code) and inc <= 60:
            code = self.check_state()

            if not self.is_broker_reachable(code):
                logging.info(
                    f"Iteration {inc}, result code - {code}. EMQX Broker not ready"
                )
                time.sleep(1)

            inc += 1

        logging.info(f"EMQX Broker {self.current_link} - Ready to work")

        self.token = self.get_bearer()

        self.headers = {
            "accept": "*/*",
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    async def init(self) -> None:
        redis_auth = await self.prepare_backend_acl()
        await self.set_auth_settings()
        await self.delete_auth_hooks()
        await self.set_file_auth_hook()
        await self.set_http_auth_hook()
        await self.set_auth_hook("redis", redis_auth)
        await self.set_tcp_listener_settings()
        await self.set_global_mqtt_settings()

    async def prepare_backend_acl(self) -> dict[str, JsonValue]:
        token = AgentBackend(name=settings.pu_domain).generate_agent_token()
        acl_prefix = (
            f"{self.REDIS_ACL_PREFIX}:{sha256(token.encode()).hexdigest()}"
        )
        redis_url = settings.pu_mqtt_redis_auth_url or settings.pu_redis_url
        broker_redis = parse_url(redis_url)
        database = broker_redis.get("db", 0)
        instance = runtime.get()
        redis_options = parse_url(settings.pu_redis_url)
        redis_options.update(
            db=database,
            socket_connect_timeout=instance.pu_instance_http_connect_timeout,
            socket_timeout=instance.pu_instance_http_timeout,
        )
        async with Redis.from_pool(ConnectionPool(**redis_options)) as redis:
            await redis.hset(
                f"{acl_prefix}:{token}",
                f"eq {settings.pu_domain}/+",
                MqttAction.SUBSCRIBE.value,
            )

        host = broker_redis["host"]
        if ":" in host:
            host = f"[{host}]"
        port = broker_redis.get("port", self.REDIS_DEFAULT_PORT)
        return {
            "enable": True,
            "server": f"{host}:{port}",
            "redis_type": "single",
            "pool_size": self.REDIS_POOL_SIZE,
            "username": broker_redis.get("username", ""),
            "password": broker_redis.get("password", ""),
            "database": database,
            "auto_reconnect": True,
            "ssl": {"enable": redis_url.startswith("rediss://")},
            "cmd": f"HGETALL {acl_prefix}:${{username}}",
        }

    @staticmethod
    def is_broker_reachable(status_code: int) -> bool:
        # EMQX 6.3+ serves /api-docs with 401 until login; the node is already up.
        return status_code < 400 or status_code in {401, 403}

    def check_state(self) -> int:
        response = httpx.get(
            f"{self.current_link}/api-docs/swagger.json",
            timeout=runtime.get().http_timeout(),
        )
        return response.status_code

    def _log_response(self, response):
        assert response.status_code < 500, (
            f"Error connect to {self.current_link}"
        )

    def get_bearer(self) -> str:
        headers = {
            "accept": "*/*",
            "Content-Type": "application/json",
        }
        data = {
            "username": settings.pu_mqtt_username,
            "password": settings.pu_mqtt_password,
        }
        response = httpx.post(
            f"{self.current_link}/api/v5/login",
            json=data,
            headers=headers,
            timeout=runtime.get().http_timeout(),
        )
        self._log_response(response)

        return response.json()["token"]

    async def delete_auth_hooks(self) -> None:
        sources = f"{self.current_link}/api/v5/authorization/sources"
        async with httpx.AsyncClient(
            timeout=runtime.get().http_timeout(),
            headers=self.headers,
        ) as client:
            for source in ("file", "http", "redis"):
                logging.info("Delete %s auth hook MQTT Broker", source)
                response = await client.delete(f"{sources}/{source}")
                if response.status_code != httpx.codes.NOT_FOUND:
                    response.raise_for_status()

    async def set_file_auth_hook(self) -> None:
        await self.set_auth_hook(
            "file", {"enable": True, "rules": "{deny, all}.\n"}
        )

    async def set_auth_hook(
        self, source: str, config: dict[str, JsonValue]
    ) -> None:
        logging.info("Set %s auth hook MQTT Broker", source)
        async with httpx.AsyncClient(
            timeout=runtime.get().http_timeout(),
            headers=self.headers,
        ) as client:
            response = await client.post(
                f"{self.current_link}/api/v5/authorization/sources",
                json={"type": source, **config},
            )
            response.raise_for_status()

    async def set_http_auth_hook(self) -> None:
        data = {
            "body": {
                "token": "${username}",
                "topic": "${topic}",
            },
            "connect_timeout": "15s",
            "enable": True,
            "enable_pipelining": 100,
            "headers": {"content-type": "application/json"},
            "method": "post",
            "pool_size": 8,
            "request_timeout": "30s",
            "ssl": {
                "enable": settings.pu_secure,
                "versions": ["tlsv1.3", "tlsv1.2"],
                "verify": "verify_none",
                "reuse_sessions": True,
                "secure_renegotiate": True,
                "log_level": "notice",
                "hibernate_after": "5s",
                "depth": 10,
            },
            "url": (
                f"{settings.pu_link_prefix_and_v1}/units/auth"
                "?action=${action}"
            ),
        }
        await self.set_auth_hook("http", data)

    async def set_auth_settings(self) -> None:
        authorization = f"{self.current_link}/api/v5/authorization"
        data = {
            "no_match": "deny",
            "deny_action": "ignore",
            "cache": {
                "enable": True,
                "max_size": self.AUTH_CACHE_MAX_SIZE,
                "ttl": self.AUTH_CACHE_TTL,
                "excludes": [],
            },
        }
        logging.info("Set authorization settings MQTT Broker")
        async with httpx.AsyncClient(
            timeout=runtime.get().http_timeout(),
            headers=self.headers,
        ) as client:
            response = await client.put(f"{authorization}/settings", json=data)
            response.raise_for_status()

            response = await client.put(
                f"{authorization}/node_cache", json={"enable": False}
            )
            if response.status_code != httpx.codes.NOT_FOUND:
                response.raise_for_status()

    async def disable_default_listeners(self) -> None:
        async with httpx.AsyncClient(
            timeout=runtime.get().http_timeout()
        ) as client:
            for source in ["ssl", "ws", "wss"]:
                logging.info(f"Disable {source} listener MQTT Broker")
                response = await client.post(
                    f"{self.current_link}/api/v5/listeners/{source}:default/stop",
                    headers=self.headers,
                )
                self._log_response(response)

    async def set_tcp_listener_settings(self) -> None:
        data = {
            "acceptors": 16,
            "access_rules": ["allow all"],
            "bind": "0.0.0.0:1883",
            "bytes_rate": runtime.get().pu_mqtt_client_max_bytes_rate,
            "enable": True,
            "enable_authn": True,
            "id": "tcp:default",
            "max_conn_rate": runtime.get().pu_mqtt_max_client_connection_rate,
            "max_connections": runtime.get().pu_mqtt_max_clients,
            "messages_rate": runtime.get().pu_mqtt_client_max_messages_rate,
            "mountpoint": "",
            "proxy_protocol": False,
            "proxy_protocol_timeout": "3s",
            "running": True,
            "tcp_options": {
                "active_n": 100,
                "backlog": 1024,
                "buffer": "4KB",
                "high_watermark": "1MB",
                "keepalive": "none",
                "nodelay": True,
                "nolinger": False,
                "reuseaddr": True,
                "send_timeout": "15s",
                "send_timeout_close": True,
            },
            "type": "tcp",
            "zone": "default",
        }

        logging.info("Set settings for tcp listener")
        async with httpx.AsyncClient(
            timeout=runtime.get().http_timeout()
        ) as client:
            response = await client.put(
                f"{self.current_link}/api/v5/listeners/tcp:default",
                json=data,
                headers=self.headers,
            )
        self._log_response(response)

    async def set_global_mqtt_settings(self) -> None:
        data = {
            # EMQX 6.3 requires shared_subs and dropped the 5.x session GC fields.
            "durable_sessions": {
                "enable": False,
                "batch_size": 100,
                "checkpoint_interval": "15s",
                "message_retention_period": "1d",
                "shared_subs": {
                    "checkpoint_interval": "15s",
                    "realloc_interval": "100ms",
                    "revocation_timeout": "10s",
                },
            },
            # Flat enable/window_time/max_count/ban_time were removed in 6.3.
            "flapping_detect": {
                "by_clientid": "none",
                "by_username": "none",
                "by_peerhost": "none",
            },
            "force_gc": {
                "enable": True,
                "bytes": "16MB",
                "count": 16000,
            },
            "force_shutdown": {
                "enable": True,
                "max_heap_size": "32MB",
                "max_mailbox_size": 1000,
            },
            "mqtt": {
                "max_clientid_len": runtime.get().pu_mqtt_max_client_id_len,
                "max_topic_alias": runtime.get().pu_mqtt_max_topic_alias,
                "max_qos_allowed": runtime.get().pu_mqtt_max_qos,
                "max_mqueue_len": runtime.get().pu_mqtt_max_len_message_queue,
                "max_topic_levels": runtime.get().pu_mqtt_max_topic_levels,
                "max_packet_size": f"{runtime.get().pu_mqtt_max_payload_size}KB",
                "session_expiry_interval": "2h",
                "max_subscriptions": "infinity",
                "exclusive_subscription": False,
                "use_username_as_clientid": False,
                "idle_timeout": "15s",
                "mqueue_store_qos0": False,
                "upgrade_qos": False,
                "mqueue_priorities": "disabled",
                "strict_mode": False,
                "shared_subscription": True,
                "server_keepalive": "disabled",
                "wildcard_subscription": True,
                "response_information": "",
                "shared_subscription_initial_sticky_pick": "random",
                "max_awaiting_rel": 100,
                "peer_cert_as_username": "disabled",
                "client_attrs_init": [],
                "retry_interval": "infinity",
                "retain_available": False,
                "message_expiry_interval": "infinity",
                "ignore_loop_deliver": False,
                "clientid_override": "disabled",
                "peer_cert_as_clientid": "disabled",
                "mqueue_default_priority": "lowest",
                "max_inflight": 32,
                "keepalive_multiplier": 1.5,
                "shared_subscription_strategy": "round_robin",
                "keepalive_check_interval": "30s",
                "await_rel_timeout": "300s",
            },
        }

        logging.info("Set global mqtt settings")
        async with httpx.AsyncClient(
            timeout=runtime.get().http_timeout()
        ) as client:
            response = await client.put(
                f"{self.current_link}/api/v5/configs/global_zone",
                json=data,
                headers=self.headers,
            )
            self._log_response(response)

            data = {
                "enable": False,
            }

            logging.info("Disable retainer")
            response = await client.put(
                f"{self.current_link}/api/v5/mqtt/retainer",
                json=data,
                headers=self.headers,
            )
            self._log_response(response)
