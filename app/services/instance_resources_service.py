from datetime import UTC, datetime, timedelta

from app import runtime
from app.configs.error_codes import ErrorCode
from app.configs.errors import InstanceError
from app.dto.enum import AgentType, InstanceResourceMetric, UserRole
from app.repositories.instance_resources_repository import (
    InstanceResourcesRepository,
)
from app.repositories.logporter_repository import LogporterRepository
from app.schemas.pydantic.instance_resources import InstanceResourceSample
from app.services.access_service import AccessService
from app.services.notification_service import NotificationService


class InstanceResourcesService:
    RETENTION = timedelta(days=90)

    def __init__(
        self,
        repository: InstanceResourcesRepository,
        access_service: AccessService,
        notification_service: NotificationService,
    ) -> None:
        self.repository = repository
        self.access_service = access_service
        self.notification_service = notification_service

    def collect(self, logporter: LogporterRepository) -> None:
        moment = datetime.now(UTC).replace(second=0, microsecond=0)
        instance = runtime.get()
        usage = logporter.sample(moment, instance.http_timeout())
        if usage is None:
            return

        pg_bytes, cc_bytes = self.repository.database_sizes()
        sample = InstanceResourceSample(
            create_datetime=moment,
            cpu_percent=usage.cpu_percent,
            ram_percent=usage.ram_percent,
            network_width_percent=100
            * usage.network_bytes_per_second
            / instance.pu_instance_network_speed_limit,
            pu_instance_network_speed_limit=instance.pu_instance_network_speed_limit,
            pg_disk_percent=100
            * pg_bytes
            / instance.pu_instance_pg_size_limit,
            cc_disk_percent=100
            * cc_bytes
            / instance.pu_instance_cc_size_limit,
        )
        self.repository.insert(sample)
        if instance.pu_ff_notification_enable:
            self.notification_service.notify_resource_limits(
                {
                    InstanceResourceMetric.CPU: sample.cpu_percent,
                    InstanceResourceMetric.RAM: sample.ram_percent,
                    InstanceResourceMetric.NETWORK_WIDTH: sample.network_width_percent,
                    InstanceResourceMetric.POSTGRESQL: sample.pg_disk_percent,
                    InstanceResourceMetric.CLICKHOUSE: sample.cc_disk_percent,
                },
                moment,
            )

    def history(
        self, start: datetime, end: datetime
    ) -> list[InstanceResourceSample]:
        self.access_service.authorization.check_access(
            [AgentType.USER], [UserRole.OWNER]
        )
        if start >= end:
            msg = "Start of the resource history period must precede its end"
            raise InstanceError(
                msg, code=ErrorCode.INSTANCE_RESOURCE_PERIOD_INVALID
            )
        now = datetime.now(UTC)
        start = max(start, now - self.RETENTION)
        end = min(end, now)
        return self.repository.list_samples(start, end) if start < end else []
