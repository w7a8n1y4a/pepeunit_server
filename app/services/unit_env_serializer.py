import json
import re

from pydantic import JsonValue


class UnitEnvSerializer:
    @classmethod
    def serialize(cls, env: dict[str, JsonValue]) -> str:
        return "".join(
            f"{name}={cls.serialize_value(value)}\n"
            for name, value in env.items()
        )

    @staticmethod
    def serialize_value(value: JsonValue) -> str:
        if value == "":
            return ""
        if isinstance(value, str) and re.fullmatch(r"[\w@%+:,./=-]+", value):
            try:
                json.loads(value)
            except ValueError:
                return value
        rendered = json.dumps(
            value, ensure_ascii=False, allow_nan=False, separators=(",", ":")
        )
        if isinstance(value, (dict, list)):
            escaped = rendered.replace("\\", "\\\\").replace("'", "\\'")
            return f"'{escaped}'"
        return rendered
