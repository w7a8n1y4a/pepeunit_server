import asyncio
import logging
import math
import multiprocessing
import re
import time

from app import settings
from app.configs.emqx import ControlEmqx
from app.dto.enum import ProcessingPolicyType, TypeInputValue
from tests.load.src.dto.config import LoadTestConfig
from tests.load.src.loaders.mqtt import (
    MQTT_TIMEOUT,
    MQTTLoadStartup,
    MQTTLoadTest,
)
from tests.load.src.setup import MqttTestPreparation


def run_mqtt_batch(
    config: LoadTestConfig, units_batch: list[dict], startup: MQTTLoadStartup
) -> int:
    return asyncio.run(MQTTLoadTest(config, units_batch).start_test(startup))


class LoadTester:
    def __init__(
        self, config: LoadTestConfig, preparation: MqttTestPreparation
    ):
        self.config = config
        self.preparation = preparation

    async def run(self):
        await self.run_mqtt_test()

    def split_units(self):
        num_units = len(self.preparation.units)
        if not num_units or self.config.workers < 1:
            msg = "MQTT load test requires units and at least one worker"
            raise ValueError(msg)
        num_workers = min(self.config.workers, num_units)
        batch_size = math.ceil(num_units / num_workers)
        return [
            self.preparation.units[i : i + batch_size]
            for i in range(0, num_units, batch_size)
        ]

    async def check_backend(self):
        if not await self.preparation.is_backend_subscribed():
            msg = "Message Processor MQTT subscription disappeared"
            raise RuntimeError(msg)

    @staticmethod
    def rate_interval(rate: str | int) -> float:
        if rate == "infinity":
            return 0.0
        match = re.fullmatch(r"([1-9]\d*)(?:/(\d*)(ms|s|m|h|d))?", str(rate))
        if match is None:
            msg = f"Invalid EMQX listener rate: {rate!r}"
            raise ValueError(msg)
        count, period, unit = match.groups()
        seconds = {"ms": 0.001, "s": 1, "m": 60, "h": 3600, "d": 86400}
        return seconds[unit or "s"] * int(period or 1) / int(count)

    async def run_mqtt_test(self):
        unit_batches = self.split_units()
        await self.check_backend()
        listener = await self.preparation.get_tcp_listener()
        connection_interval = self.rate_interval(listener["max_conn_rate"])
        logging.info(
            "Connecting %s MQTT clients across %s workers (EMQX max_conn_rate=%s)",
            len(self.preparation.units),
            len(unit_batches),
            listener["max_conn_rate"],
        )
        if self.config.rps * self.rate_interval(listener["messages_rate"]) > 1:
            logging.warning(
                "Publish target is %s msg/s per client; EMQX messages_rate is %s",
                self.config.rps,
                listener["messages_rate"],
            )

        context = multiprocessing.get_context("spawn")
        with (
            context.Manager() as manager,
            context.Pool(processes=len(unit_batches)) as pool,
        ):
            startup = MQTTLoadStartup(
                connection_interval=connection_interval,
                next_connection=manager.Value("d", 0.0),
                lock=manager.Lock(),
                barrier=manager.Barrier(len(unit_batches)),
                deadline=(
                    time.monotonic()
                    + connection_interval * len(self.preparation.units)
                    + MQTT_TIMEOUT * (MQTTLoadTest.CONNECT_ATTEMPTS + 1)
                ),
            )
            pending = [
                pool.apply_async(run_mqtt_batch, (self.config, batch, startup))
                for batch in unit_batches
            ]
            results = []
            deadline = startup.deadline + self.config.duration + MQTT_TIMEOUT
            next_check = time.monotonic() + 1
            while pending:
                for result in pending[:]:
                    if result.ready():
                        # Propagate worker exceptions, preserving their traceback
                        # and a nonzero process exit status for make/Docker/CI.
                        results.append(result.get())
                        pending.remove(result)
                if time.monotonic() >= next_check:
                    await self.check_backend()
                    next_check = time.monotonic() + 1
                if pending:
                    if time.monotonic() >= deadline:
                        msg = "MQTT load workers did not finish in time"
                        raise TimeoutError(msg)
                    await asyncio.sleep(0.1)
            await self.check_backend()
            pool.close()
            pool.join()

        await self.preparation.wait_for_processor(
            self.preparation.PROBE_AFTER_LOAD
        )
        await self.preparation.rest_client.check_processing(
            self.preparation.units[0]
        )
        logging.info(
            "MQTT clients sent %s messages (%.2f msg/s over %s seconds); "
            "Message Processor recorded load traffic",
            sum(results),
            sum(results) / self.config.duration,
            self.config.duration,
        )


async def main():
    config = LoadTestConfig(
        api_url=settings.pu_link_prefix_and_v1,
        repo_url=settings.pu_test_load_mqtt_repo_url,
        mqtt_api_url=ControlEmqx.get_emqx_link(),
        duration=settings.pu_test_load_mqtt_duration,
        unit_count=settings.pu_test_load_mqtt_unit_count,
        rps=settings.pu_test_load_mqtt_rps,
        value_type=TypeInputValue(settings.pu_test_load_mqtt_value_type),
        duplicate_count=settings.pu_test_load_mqtt_duplicate_count,
        message_size=settings.pu_test_load_mqtt_message_size,
        policy_type=ProcessingPolicyType(
            settings.pu_test_load_mqtt_policy_type
        ),
        workers=settings.pu_test_load_mqtt_workers,
        mqtt_admin=settings.pu_mqtt_username,
        mqtt_password=settings.pu_mqtt_password,
    )

    if (
        config.policy_type == ProcessingPolicyType.AGGREGATION
        and config.value_type != TypeInputValue.NUMBER
    ):
        msg = "The Aggregation load profile requires PU_TEST_LOAD_MQTT_VALUE_TYPE=Number"
        raise ValueError(msg)

    preparation = MqttTestPreparation(config)
    try:
        await preparation.setup()
        await LoadTester(config, preparation).run()
    finally:
        await preparation.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
