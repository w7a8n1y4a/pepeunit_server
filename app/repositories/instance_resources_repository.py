from datetime import datetime
from math import ceil

from clickhouse_driver import Client
from sqlalchemy import text
from sqlmodel import Session

from app.dto.clickhouse.orm import ClickhouseOrm
from app.schemas.pydantic.instance_resources import InstanceResourceSample


class InstanceResourcesRepository:
    def __init__(self, client: Client, db: Session) -> None:
        self.client = client
        self.db = db
        self.orm = ClickhouseOrm(client)

    def database_sizes(self) -> tuple[int, int]:
        pg_bytes = self.db.execute(
            text("SELECT pg_database_size(current_database())")
        ).scalar_one()
        [(cc_bytes,)] = self.client.execute(
            """
            SELECT sum(bytes_on_disk) FROM system.parts
            WHERE active AND database = currentDatabase()
            """
        )
        return pg_bytes, cc_bytes

    def insert(self, sample: InstanceResourceSample) -> None:
        self.orm.insert("instance_resources_samples", [sample])

    def list_samples(
        self, start: datetime, end: datetime
    ) -> list[InstanceResourceSample]:
        return self.orm.get_many(
            """
            SELECT create_datetime, cpu_percent, ram_percent,
                network_width_percent, pu_instance_network_speed_limit,
                pg_disk_percent, cc_disk_percent
            FROM instance_resources_samples FINAL
            WHERE create_datetime >= toDateTime(%(start)s, 'UTC')
                AND create_datetime < toDateTime(%(end)s, 'UTC')
            ORDER BY create_datetime
            """,
            {"start": ceil(start.timestamp()), "end": ceil(end.timestamp())},
            InstanceResourceSample,
        )

    def network_traffic(
        self, start: datetime, end: datetime
    ) -> tuple[int, int]:
        [(sampled_minutes, estimated_bytes)] = self.client.execute(
            """
            SELECT count(),
                sum(network_width_percent * pu_instance_network_speed_limit
                    * %(seconds)s / 100)
            FROM instance_resources_samples FINAL
            WHERE create_datetime > toDateTime(%(start)s, 'UTC')
                AND create_datetime <= toDateTime(%(end)s, 'UTC')
            """,
            {
                "start": int(start.timestamp()),
                "end": int(end.timestamp()),
                "seconds": InstanceResourceSample.INTERVAL_SECONDS,
            },
        )
        return sampled_minutes, round(estimated_bytes)
