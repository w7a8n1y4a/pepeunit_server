from dataclasses import dataclass

from app.dto.enum import ProcessingPolicyType, TypeInputValue


@dataclass(frozen=True)
class LoadTestConfig:
    api_url: str
    repo_url: str
    mqtt_api_url: str
    duration: int
    unit_count: int
    rps: int
    value_type: TypeInputValue
    duplicate_count: int
    message_size: int
    policy_type: ProcessingPolicyType
    workers: int
    mqtt_admin: str
    mqtt_password: str

    @property
    def output_name(self) -> str:
        if self.value_type == TypeInputValue.NUMBER:
            return "output_number"
        return "output_load"
