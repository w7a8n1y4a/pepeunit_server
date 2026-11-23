from math import ceil, floor

from pydantic import JsonValue

from app.dto.enum import FieldFormat, FieldKey, FieldType
from app.schemas.pydantic.unit_manifest import (
    FieldDeclaration,
    FieldDeclarations,
)
from app.services.float_json_size import FloatJsonSize
from app.services.unit_env_serializer import UnitEnvSerializer


class UnitEnvSize:
    UUID_MIN_LENGTH = 32

    @classmethod
    def calculate(cls, fields: FieldDeclarations) -> dict[str, int | None]:
        return {name: cls.value_size(field) for name, field in fields.items()}

    @classmethod
    def value_size(
        cls, field: FieldDeclaration, *, nested: bool = False
    ) -> int | None:
        kind = FieldType(field[FieldKey.TYPE])
        if FieldKey.ENUM in field:
            size = cls.enum_size(field, nested=nested)
        else:
            match kind:
                case FieldType.INT | FieldType.FLOAT:
                    size = cls.number_size(field)
                case FieldType.STRING:
                    size = cls.string_size(field, nested=nested)
                case FieldType.ARRAY:
                    size = cls.array_size(field, nested=nested)
                case FieldType.OBJECT:
                    size = cls.object_size(field, nested=nested)
                case _:
                    msg = f"{kind} has no bounded .env representation"
                    raise ValueError(msg)
        if field[FieldKey.NULLABLE]:
            return max(size or 0, cls.literal_size(None, nested=nested))
        return size

    @staticmethod
    def literal_size(value: JsonValue, *, nested: bool) -> int:
        if nested:
            encoded = UnitEnvSerializer.escape_container(
                UnitEnvSerializer.encode_json(value)
            )
        else:
            encoded = UnitEnvSerializer.serialize_value(value)
        return len(encoded)

    @classmethod
    def enum_size(cls, field: FieldDeclaration, *, nested: bool) -> int:
        values = list(field[FieldKey.ENUM])
        if field[FieldKey.TYPE] == FieldType.FLOAT:
            for value in field[FieldKey.ENUM]:
                if isinstance(value, int):
                    try:
                        number = float(value)
                    except OverflowError:
                        continue
                    if number == value:
                        values.append(number)
                elif isinstance(value, float) and value.is_integer():
                    values.append(int(value))
                if value == 0:
                    values.append(-0.0)
        return max(cls.literal_size(value, nested=nested) for value in values)

    @classmethod
    def number_size(cls, field: FieldDeclaration) -> int | None:
        minimum, maximum = field[FieldKey.MIN], field[FieldKey.MAX]
        low, high = ceil(minimum), floor(maximum)
        integers = max(len(str(low)), len(str(high))) if low <= high else None
        if field[FieldKey.TYPE] == FieldType.INT:
            return integers
        return cls.maximum([integers, FloatJsonSize.maximum(minimum, maximum)])

    @classmethod
    def string_size(
        cls, field: FieldDeclaration, *, nested: bool
    ) -> int | None:
        limit = field[FieldKey.MAX_LENGTH]
        if field.get(FieldKey.FORMAT) == FieldFormat.UUID:
            if limit < cls.UUID_MIN_LENGTH:
                return None
            sample = "\v" * (cls.UUID_MIN_LENGTH - 1) + "0"
            return (
                cls.literal_size(sample, nested=nested)
                + limit
                - cls.UUID_MIN_LENGTH
            )
        if limit == 0:
            return cls.literal_size("", nested=nested)
        first = cls.literal_size("\0", nested=nested)
        following = cls.literal_size("\0\0", nested=nested) - first
        return first + (limit - 1) * following

    @classmethod
    def array_size(
        cls, field: FieldDeclaration, *, nested: bool
    ) -> int | None:
        empty = cls.literal_size([], nested=nested)
        limit = field[FieldKey.MAX_ITEMS]
        if limit == 0:
            return empty
        item = cls.value_size(field[FieldKey.ITEMS], nested=True)
        if item is None:
            return None if field.get(FieldKey.MIN_ITEMS, 0) else empty
        return empty + limit * (item + 1) - 1

    @classmethod
    def object_size(
        cls, field: FieldDeclaration, *, nested: bool
    ) -> int | None:
        empty = cls.literal_size({}, nested=nested)
        if FieldKey.VARIANTS in field:
            return cls.maximum(
                [
                    cls.object_fields_size(fields, empty, action=action)
                    for action, fields in field[FieldKey.VARIANTS].items()
                ]
            )
        return cls.object_fields_size(field.get(FieldKey.FIELDS, {}), empty)

    @classmethod
    def object_fields_size(
        cls,
        fields: FieldDeclarations,
        empty: int,
        *,
        action: str | None = None,
    ) -> int | None:
        lengths = []
        if action is not None:
            lengths.append(
                cls.literal_size(FieldKey.ACTION.value, nested=True)
                + 1
                + cls.literal_size(action, nested=True)
            )
        for name, field in fields.items():
            size = cls.value_size(field, nested=True)
            if size is not None:
                lengths.append(cls.literal_size(name, nested=True) + 1 + size)
            elif not field[FieldKey.OPTIONAL]:
                return None
        return empty + sum(lengths) + max(0, len(lengths) - 1)

    @staticmethod
    def maximum(sizes: list[int | None]) -> int | None:
        return max((size for size in sizes if size is not None), default=None)
