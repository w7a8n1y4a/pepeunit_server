import time
from dataclasses import dataclass
from datetime import datetime
from math import isfinite

import httpx
from prometheus_client.parser import text_string_to_metric_families

from app import settings
from app.dto.enum import LogporterMetric
from app.schemas.pydantic.instance_resources import InstanceResourceSample

type LogporterValues = dict[str, dict[str, float]]


@dataclass(frozen=True)
class LogporterUsage:
    cpu_percent: float
    ram_percent: float
    network_bytes_per_second: float


class LogporterRepository:
    CONTAINER_LABEL = "containerId"

    def __init__(self) -> None:
        self.previous: tuple[datetime, float, LogporterValues] | None = None

    def sample(
        self, moment: datetime, timeout: httpx.Timeout
    ) -> LogporterUsage | None:
        response = httpx.get(
            f"{settings.pu_logporter_url.rstrip('/')}/metrics", timeout=timeout
        )
        response.raise_for_status()
        values = self.read_values(response.text)
        received_at = time.monotonic()
        previous = self.previous
        self.previous = (moment, received_at, values)
        if previous is None:
            return None
        previous_moment, previous_received_at, previous_values = previous
        if (moment - previous_moment).total_seconds() != (
            InstanceResourceSample.INTERVAL_SECONDS
        ):
            return None
        elapsed = received_at - previous_received_at
        if elapsed <= 0:
            return None

        cpu = self.counter_increase(
            values, previous_values, LogporterMetric.CPU_SECONDS
        )
        received = self.counter_increase(
            values, previous_values, LogporterMetric.NETWORK_RECEIVED
        )
        sent = self.counter_increase(
            values, previous_values, LogporterMetric.NETWORK_TRANSMITTED
        )
        return LogporterUsage(
            cpu_percent=100
            * cpu
            / elapsed
            / max(values[LogporterMetric.CPU_CORES].values()),
            ram_percent=100
            * sum(values[LogporterMetric.MEMORY_USAGE].values())
            / max(values[LogporterMetric.MEMORY_TOTAL].values()),
            network_bytes_per_second=(received + sent) / elapsed,
        )

    @classmethod
    def read_values(cls, content: str) -> LogporterValues:
        values: LogporterValues = {
            metric.value: {} for metric in LogporterMetric
        }
        for family in text_string_to_metric_families(content):
            for sample in family.samples:
                metric = values.get(sample.name)
                if metric is None and family.type == "counter":
                    metric = values.get(family.name)
                if (
                    metric is not None
                    and isfinite(sample.value)
                    and sample.value >= 0
                ):
                    metric[sample.labels.get(cls.CONTAINER_LABEL, "")] = (
                        sample.value
                    )
        if (
            not all(values.values())
            or max(values[LogporterMetric.CPU_CORES].values()) <= 0
            or max(values[LogporterMetric.MEMORY_TOTAL].values()) <= 0
        ):
            msg = "Logporter returned incomplete resource metrics"
            raise ValueError(msg)
        return values

    @staticmethod
    def counter_increase(
        current: LogporterValues,
        previous: LogporterValues,
        name: LogporterMetric,
    ) -> float:
        return sum(
            max(0, value - previous[name].get(container, value))
            for container, value in current[name].items()
        )
