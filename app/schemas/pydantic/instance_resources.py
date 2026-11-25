from datetime import datetime
from typing import ClassVar

from pydantic import BaseModel

from app.dto.mixin import ClickHouseBaseMixin


class InstanceResourceSample(BaseModel, ClickHouseBaseMixin):
    INTERVAL_SECONDS: ClassVar[int] = 60

    create_datetime: datetime
    cpu_percent: float
    ram_percent: float
    network_width_percent: float
    pu_instance_network_speed_limit: int
    pg_disk_percent: float
    cc_disk_percent: float


class InstanceNetworkTrafficRead(BaseModel):
    period_start: datetime
    period_end: datetime
    sampled_minutes: int
    elapsed_minutes: int
    estimated_bytes: int
    limit_bytes: int
    limit_percent: float
