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

    @classmethod
    def serialize_value(cls, value: JsonValue) -> str:
        if value == "":
            return ""
        if isinstance(value, str) and re.fullmatch(r"[\w@%+:,./=-]+", value):
            try:
                json.loads(value)
            except ValueError:
                return value
        rendered = cls.encode_json(value)
        if isinstance(value, (dict, list)):
            return f"'{cls.escape_container(rendered)}'"
        return rendered

    @staticmethod
    def encode_json(value: JsonValue) -> str:
        return json.dumps(
            value, ensure_ascii=False, allow_nan=False, separators=(",", ":")
        )

    @staticmethod
    def escape_container(value: str) -> str:
        return value.replace("\\", "\\\\").replace("'", "\\'")
