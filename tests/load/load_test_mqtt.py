import asyncio
import hashlib
import logging
import math
import multiprocessing
import time

from app import settings
from tests.load.src.dto.config import LoadTestConfig
from tests.load.src.loaders.mqtt import MQTT_TIMEOUT, MQTTLoadTest
from tests.load.src.setup import MqttTestPreparation


def run_mqtt_batch(config, units_batch):
    return asyncio.run(MQTTLoadTest(config, units_batch).start_test())


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
            raise ValueError(
                "MQTT load test requires units and at least one worker"
            )
        num_workers = min(self.config.workers, num_units)
        batch_size = math.ceil(num_units / num_workers)
        return [
            self.preparation.units[i : i + batch_size]
            for i in range(0, num_units, batch_size)
        ]

    def check_backend(self):
        if not self.preparation.is_backend_subs():
            raise RuntimeError("Backend MQTT subscription disappeared")

    async def run_mqtt_test(self):
        unit_batches = self.split_units()
        for unit in self.preparation.units:
            MQTTLoadTest.validate_unit(unit)
        self.check_backend()
        # A module-level SIGTERM handler is inherited/imported by workers and
        # interrupts multiprocessing finalizers. Let Pool own worker shutdown.
        context = multiprocessing.get_context("spawn")
        with context.Pool(processes=len(unit_batches)) as pool:
            pending = [
                pool.apply_async(run_mqtt_batch, (self.config, batch))
                for batch in unit_batches
            ]
            results = []
            deadline = (
                time.monotonic() + self.config.duration + 3 * MQTT_TIMEOUT + 10
            )
            next_check = time.monotonic() + 1
            while pending:
                for result in pending[:]:
                    if result.ready():
                        # Propagate worker exceptions, preserving their traceback
                        # and a nonzero process exit status for make/Docker/CI.
                        results.append(result.get())
                        pending.remove(result)
                if time.monotonic() >= next_check:
                    self.check_backend()
                    next_check = time.monotonic() + 1
                if pending:
                    if time.monotonic() >= deadline:
                        raise TimeoutError(
                            "MQTT load workers did not finish in time"
                        )
                    await asyncio.sleep(0.1)
            self.check_backend()
            pool.close()
            pool.join()

        logging.info(
            "MQTT clients sent %s messages (%.2f msg/s over %s seconds per batch); "
            "backend subscription remained active",
            sum(results),
            sum(results) / self.config.duration,
            self.config.duration,
        )


async def main():
    url = f"{settings.pu_http_type}://{settings.pu_domain}"

    config = LoadTestConfig(
        url=url,
        duration=settings.pu_test_load_mqtt_duration,
        unit_count=settings.pu_test_load_mqtt_unit_count,
        rps=settings.pu_test_load_mqtt_rps,
        value_type=settings.pu_test_load_mqtt_value_type,
        duplicate_count=settings.pu_test_load_mqtt_duplicate_count,
        message_size=settings.pu_test_load_mqtt_message_size,
        policy_type=settings.pu_test_load_mqtt_policy_type,
        workers=settings.pu_test_load_mqtt_workers,
        mqtt_admin=settings.pu_mqtt_username,
        mqtt_password=settings.pu_mqtt_password,
        test_hash=hashlib.md5(url.encode("utf-8")).hexdigest()[:10],
    )

    preparation = MqttTestPreparation(config)
    await preparation.setup()

    tester = LoadTester(config, preparation)
    await tester.run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
