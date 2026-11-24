import json
import uuid as uuid_pkg
from collections.abc import Collection
from copy import deepcopy
from math import isfinite
from re import Pattern
from typing import NotRequired, Self, TypedDict

from pydantic import (
    BaseModel,
    Field,
    FiniteFloat,
    JsonValue,
    ValidationError,
    model_validator,
)
from pydantic_core import InitErrorDetails

from app.dto.enum import (
    FieldFormat,
    FieldKey,
    FieldType,
    MqttQos,
    OutputRouting,
    PayloadCodec,
    UnitExecutionType,
)


class FieldDeclaration(TypedDict, total=False):
    type: str
    min: int | float
    max: int | float
    max_bytes: int
    max_length: int
    min_items: int
    max_items: int
    enum: list[JsonValue]
    unit: str
    format: str
    codec: str
    qos: int
    optional: bool
    nullable: bool
    extra: bool
    secret: bool
    description: str
    example: JsonValue
    default: JsonValue
    fields: dict[str, FieldDeclaration]
    items: FieldDeclaration
    variants: dict[str, dict[str, FieldDeclaration]]
    routing: str


type FieldDeclarations = dict[str, FieldDeclaration]


class ManifestSearch(BaseModel):
    type: str
    execution_type: str
    name: str = Field(min_length=1)
    platforms: list[str]
    description: str


class ManifestLink(TypedDict):
    link: str


class ManifestVideo(ManifestLink):
    preview: NotRequired[str]


class ManifestDocs(TypedDict, total=False):
    video: ManifestVideo
    schema: ManifestLink
    images: dict[str, str]
    models: dict[str, str]
    algorithm: str
    installation: str


class ManifestContractHeader(BaseModel):
    contract_version: str = Field(pattern=r"^v[0-9]+(?:\.[0-9]+)?$")


class UnitDefinition(ManifestContractHeader):
    search: ManifestSearch
    docs: ManifestDocs = Field(default_factory=dict)
    env: FieldDeclarations = Field(default_factory=dict)
    input: FieldDeclarations = Field(default_factory=dict)
    output: FieldDeclarations = Field(default_factory=dict)

    @property
    def is_compilable(self) -> bool:
        return self.search.execution_type == UnitExecutionType.COMPILED

    def validate_search(self, dictionary: ManifestDictionary) -> None:
        self.require_choice(
            self.search.type,
            dictionary.unit_types,
            (FieldKey.SEARCH, FieldKey.TYPE),
        )
        self.require_choice(
            self.search.execution_type,
            dictionary.execution_types,
            (FieldKey.SEARCH, FieldKey.EXECUTION_TYPE),
        )
        for index, platform in enumerate(self.search.platforms):
            self.require_choice(
                platform,
                dictionary.platforms,
                (FieldKey.SEARCH, FieldKey.PLATFORMS, index),
            )

    def resolve_fields(self, dictionary: ManifestDictionary) -> None:
        reserved_names = dictionary.input.keys() | dictionary.output.keys()
        for section in (FieldKey.ENV, FieldKey.INPUT, FieldKey.OUTPUT):
            declarations = getattr(self, section)
            if section != FieldKey.ENV:
                definitions = getattr(dictionary, section)
                for name, field in declarations.items():
                    if name not in reserved_names:
                        continue
                    path = (section, name)
                    self.require_field(
                        name in definitions,
                        path,
                        "Reserved IO name is not allowed in this direction",
                    )
                    self.require_field(
                        not field,
                        path,
                        "Reserved IO declarations must be empty",
                    )
                declarations.update(deepcopy(definitions))
            for field in declarations.values():
                self.apply_field_defaults(field, dictionary, section)
        for name in self.output:
            self.require_field(
                name not in self.input,
                (FieldKey.OUTPUT, name),
                "IO names must be unique across Input and Output",
            )

    def apply_field_defaults(
        self,
        field: FieldDeclaration,
        dictionary: ManifestDictionary,
        section: FieldKey | None = None,
    ) -> None:
        for key in (FieldKey.OPTIONAL, FieldKey.NULLABLE):
            field.setdefault(key, dictionary.defaults[key])
        if (
            field.get(FieldKey.TYPE) == FieldType.OBJECT
            and section != FieldKey.ENV
        ):
            field.setdefault(
                FieldKey.EXTRA, dictionary.defaults[FieldKey.EXTRA]
            )
        if section in (FieldKey.INPUT, FieldKey.OUTPUT):
            kind = field.get(FieldKey.TYPE, "")
            codecs = dictionary.defaults[FieldKey.CODEC]
            codec = codecs.get(kind, codecs[FieldKey.DEFAULT])
            if kind == FieldType.STRING and field[FieldKey.NULLABLE]:
                codec = codecs[FieldKey.DEFAULT]
            field.setdefault(FieldKey.CODEC, codec)
            field.setdefault(FieldKey.QOS, dictionary.defaults[FieldKey.QOS])
        if section == FieldKey.OUTPUT:
            field.setdefault(
                FieldKey.ROUTING, dictionary.defaults[FieldKey.ROUTING]
            )
        nested_section = FieldKey.ENV if section == FieldKey.ENV else None
        for child in field.get(FieldKey.FIELDS, {}).values():
            self.apply_field_defaults(child, dictionary, nested_section)
        if FieldKey.ITEMS in field:
            self.apply_field_defaults(
                field[FieldKey.ITEMS], dictionary, nested_section
            )
        for declarations in field.get(FieldKey.VARIANTS, {}).values():
            for child in declarations.values():
                self.apply_field_defaults(child, dictionary, nested_section)

    def validate_fields(self, dictionary: ManifestDictionary) -> None:
        for section in (FieldKey.ENV, FieldKey.INPUT, FieldKey.OUTPUT):
            name_pattern = (
                dictionary.name_patterns.env
                if section == FieldKey.ENV
                else dictionary.name_patterns.io
            )
            for name, declaration in getattr(self, section).items():
                self.require_field(
                    name_pattern.fullmatch(name) is not None,
                    (section, name),
                    f"Name must match {name_pattern.pattern}",
                )
                if section == FieldKey.ENV:
                    try:
                        json.dumps(declaration, ensure_ascii=False).encode(
                            "utf-8"
                        )
                    except UnicodeEncodeError:
                        self.require_field(
                            False,
                            (section, name),
                            "Environment schema must be UTF-8",
                        )
                self.validate_field(declaration, dictionary, (section, name))

    def validate_field(
        self,
        field: FieldDeclaration,
        dictionary: ManifestDictionary,
        path: tuple[str | int, ...],
    ) -> None:
        self.require_field(
            FieldKey.TYPE in field, (*path, FieldKey.TYPE), "Type is required"
        )
        kind = field[FieldKey.TYPE]
        self.require_choice(kind, dictionary.types, (*path, FieldKey.TYPE))
        self.require_field(
            len(path) == 2 or kind != FieldType.BYTES,
            (*path, FieldKey.TYPE),
            "Raw bytes cannot be nested in a JSON value",
        )
        allowed = {
            FieldKey.MIN: (FieldType.INT, FieldType.FLOAT),
            FieldKey.MAX: (FieldType.INT, FieldType.FLOAT),
            FieldKey.UNIT: (FieldType.INT, FieldType.FLOAT),
            FieldKey.MAX_BYTES: (
                FieldType.STRING,
                FieldType.ANY,
                FieldType.BYTES,
            ),
            FieldKey.MAX_LENGTH: (FieldType.STRING,),
            FieldKey.MIN_ITEMS: (FieldType.ARRAY,),
            FieldKey.MAX_ITEMS: (FieldType.ARRAY,),
            FieldKey.ITEMS: (FieldType.ARRAY,),
            FieldKey.FIELDS: (FieldType.OBJECT,),
            FieldKey.VARIANTS: (FieldType.OBJECT,),
            FieldKey.EXTRA: (FieldType.OBJECT,),
            FieldKey.FORMAT: (FieldType.STRING,),
            FieldKey.ENUM: (
                FieldType.BOOL,
                FieldType.INT,
                FieldType.FLOAT,
                FieldType.STRING,
            ),
        }
        for key, kinds in allowed.items():
            if key in field:
                self.require_field(
                    kind in kinds, (*path, key), f"Not allowed for type {kind}"
                )
        if path[0] == FieldKey.ENV:
            self.validate_env_limits(field, kind, path)
        elif FieldKey.MAX_LENGTH in field:
            self.require_field(
                False, (*path, FieldKey.MAX_LENGTH), "max_length is env-only"
            )
        self.validate_limits(field, dictionary, path)
        if FieldKey.UNIT in field:
            self.require_field(
                bool(field[FieldKey.UNIT].strip()),
                (*path, FieldKey.UNIT),
                "Unit is empty",
            )
        if FieldKey.FORMAT in field:
            self.require_choice(
                field[FieldKey.FORMAT],
                dictionary.formats,
                (*path, FieldKey.FORMAT),
            )
        if kind == FieldType.ARRAY:
            self.require_field(
                FieldKey.ITEMS in field,
                (*path, FieldKey.ITEMS),
                "Array items are required",
            )
        if kind == FieldType.OBJECT:
            self.require_field(
                not (FieldKey.FIELDS in field and FieldKey.VARIANTS in field),
                path,
                "fields and variants are mutually exclusive",
            )
            self.require_field(
                FieldKey.VARIANTS not in field or not field[FieldKey.EXTRA],
                (*path, FieldKey.EXTRA),
                "variants forbid extra fields",
            )
        self.validate_transport(field, dictionary, path)
        self.validate_nested(field, dictionary, path)
        self.validate_literals(field, path)

    def validate_env_limits(
        self,
        field: FieldDeclaration,
        kind: str,
        path: tuple[str | int, ...],
    ) -> None:
        required = {
            FieldType.BOOL: (FieldKey.ENUM,),
            FieldType.INT: (FieldKey.MIN, FieldKey.MAX),
            FieldType.FLOAT: (FieldKey.MIN, FieldKey.MAX),
            FieldType.STRING: (FieldKey.MAX_LENGTH,),
            FieldType.ARRAY: (FieldKey.MAX_ITEMS,),
            FieldType.OBJECT: (FieldKey.EXTRA,),
        }
        self.require_choice(kind, required.keys(), (*path, FieldKey.TYPE))
        self.require_field(
            FieldKey.MAX_BYTES not in field,
            (*path, FieldKey.MAX_BYTES),
            "Use max_length for env strings",
        )
        keys = required[FieldType(kind)]
        if kind in (FieldType.INT, FieldType.FLOAT) and FieldKey.ENUM in field:
            keys = ()
        for key in keys:
            self.require_field(key in field, (*path, key), "Required in env")
        if kind == FieldType.OBJECT:
            self.require_field(
                field[FieldKey.EXTRA] is False,
                (*path, FieldKey.EXTRA),
                "Environment objects require extra: false",
            )
        if kind == FieldType.BOOL:
            self.require_field(
                all(isinstance(value, bool) for value in field[FieldKey.ENUM]),
                (*path, FieldKey.ENUM),
                "Boolean enum values must be true or false",
            )

    def validate_nested(
        self,
        field: FieldDeclaration,
        dictionary: ManifestDictionary,
        path: tuple[str | int, ...],
    ) -> None:
        for name, child in field.get(FieldKey.FIELDS, {}).items():
            self.validate_field(
                child, dictionary, (*path, FieldKey.FIELDS, name)
            )
        if FieldKey.ITEMS in field:
            self.validate_field(
                field[FieldKey.ITEMS], dictionary, (*path, FieldKey.ITEMS)
            )
        if FieldKey.VARIANTS not in field:
            return
        self.require_field(
            bool(field[FieldKey.VARIANTS]),
            (*path, FieldKey.VARIANTS),
            "Variants must not be empty",
        )
        for variant, declarations in field[FieldKey.VARIANTS].items():
            variant_path = (*path, FieldKey.VARIANTS, variant)
            self.require_field(
                FieldKey.ACTION not in declarations,
                (*variant_path, FieldKey.ACTION),
                "The action field is defined by variants",
            )
            for name, child in declarations.items():
                self.validate_field(child, dictionary, (*variant_path, name))

    def validate_literals(
        self, field: FieldDeclaration, path: tuple[str | int, ...]
    ) -> None:
        if FieldKey.ENUM in field:
            values = field[FieldKey.ENUM]
            self.require_field(
                bool(values), (*path, FieldKey.ENUM), "Enum must not be empty"
            )
            for index, value in enumerate(values):
                self.validate_literal(
                    field, value, (*path, FieldKey.ENUM, index)
                )
                self.require_field(
                    value not in values[:index],
                    (*path, FieldKey.ENUM, index),
                    "Enum values must be unique",
                )
        for key in (FieldKey.DEFAULT, FieldKey.EXAMPLE):
            if key not in field:
                continue
            value = field[key]
            self.validate_literal(field, value, (*path, key))

    def validate_limits(
        self,
        field: FieldDeclaration,
        dictionary: ManifestDictionary,
        path: tuple[str | int, ...],
    ) -> None:
        for key in (FieldKey.MIN, FieldKey.MAX):
            if key not in field:
                continue
            bound = field[key]
            self.require_field(
                not isinstance(bound, float) or isfinite(bound),
                (*path, key),
                "Numeric limits must be finite",
            )
            self.require_field(
                field[FieldKey.TYPE] != FieldType.INT or type(bound) is int,
                (*path, key),
                "Integer limits must be integers",
            )
        for key in (
            FieldKey.MAX_BYTES,
            FieldKey.MAX_LENGTH,
            FieldKey.MIN_ITEMS,
            FieldKey.MAX_ITEMS,
        ):
            if key in field:
                self.require_field(
                    field[key] >= 0, (*path, key), "Limit must be non-negative"
                )
        for lower, upper in (
            (FieldKey.MIN, FieldKey.MAX),
            (FieldKey.MIN_ITEMS, FieldKey.MAX_ITEMS),
        ):
            if lower in field and upper in field:
                self.require_field(
                    field[lower] <= field[upper],
                    (*path, upper),
                    "Upper limit must not be less than lower limit",
                )

        if path[0] in (FieldKey.INPUT, FieldKey.OUTPUT):
            self.apply_io_type_limits(field, dictionary.type_limits, path)

    def apply_io_type_limits(
        self,
        field: FieldDeclaration,
        type_limits: ManifestTypeLimits,
        path: tuple[str | int, ...],
    ) -> None:
        kind = field[FieldKey.TYPE]
        if kind not in (FieldType.INT, FieldType.FLOAT):
            return
        limits = getattr(type_limits, kind)
        for key, default in (
            (FieldKey.MIN, limits.min),
            (FieldKey.MAX, limits.max),
        ):
            bound = field.setdefault(key, default)
            self.require_field(
                limits.min <= bound <= limits.max,
                (*path, key),
                f"Numeric limit is outside the {kind} bounds",
            )

    def validate_transport(
        self,
        field: FieldDeclaration,
        dictionary: ManifestDictionary,
        path: tuple[str | int, ...],
    ) -> None:
        for key in (FieldKey.CODEC, FieldKey.QOS, FieldKey.ROUTING):
            if key in field:
                self.require_field(
                    len(path) == 2
                    and path[0] in (FieldKey.INPUT, FieldKey.OUTPUT),
                    (*path, key),
                    "Transport options are only allowed on top-level Input or Output",
                )
        kind = field[FieldKey.TYPE]
        codec = field.get(FieldKey.CODEC, PayloadCodec.JSON)
        self.require_field(
            (codec == PayloadCodec.JSON and kind != FieldType.BYTES)
            or (codec == PayloadCodec.UTF8 and kind == FieldType.STRING)
            or (codec == PayloadCodec.RAW and kind == FieldType.BYTES),
            (*path, FieldKey.CODEC),
            "Codec does not support this type",
        )
        self.require_field(
            codec == PayloadCodec.JSON or not field[FieldKey.NULLABLE],
            (*path, FieldKey.NULLABLE),
            "Nullable values require the JSON codec",
        )
        if FieldKey.QOS in field:
            self.require_field(
                field[FieldKey.QOS] in MqttQos,
                (*path, FieldKey.QOS),
                "QoS must be 0 or 1",
            )
        if FieldKey.ROUTING in field:
            self.require_field(
                path[0] == FieldKey.OUTPUT,
                (*path, FieldKey.ROUTING),
                "Routing is Output-only",
            )
            self.require_choice(
                field[FieldKey.ROUTING],
                dictionary.routing,
                (*path, FieldKey.ROUTING),
            )
            self.require_field(
                field[FieldKey.ROUTING] != OutputRouting.SELECTED_LINK
                or (kind == FieldType.OBJECT and codec == PayloadCodec.JSON),
                (*path, FieldKey.ROUTING),
                "selected-link requires a JSON object",
            )
        if FieldKey.SECRET in field:
            self.require_field(
                path[0] == FieldKey.ENV,
                (*path, FieldKey.SECRET),
                "Secret is env-only",
            )

    def validate_literal(
        self,
        field: FieldDeclaration,
        value: JsonValue | bytes,
        path: tuple[str | int, ...],
    ) -> None:
        kind = field[FieldKey.TYPE]
        if value is None:
            self.require_field(
                kind == FieldType.ANY or field[FieldKey.NULLABLE],
                path,
                "Null requires nullable: true",
            )
            if kind != FieldType.ANY:
                return
        types = {
            FieldType.BOOL: (bool,),
            FieldType.INT: (int,),
            FieldType.FLOAT: (int, float),
            FieldType.STRING: (str,),
            FieldType.OBJECT: (dict,),
            FieldType.ARRAY: (list,),
            FieldType.ANY: (type(None), bool, int, float, str, dict, list),
            FieldType.BYTES: (bytes,),
        }
        self.require_field(
            type(value) in types.get(kind, ()),
            path,
            "Value does not match the declared type",
        )
        if value is not None and FieldKey.ENUM in field:
            self.require_field(
                value in field[FieldKey.ENUM],
                path,
                "Value must belong to enum",
            )
        self.validate_nested_literal(field, value, path)
        if type(value) in (int, float):
            self.require_field(
                not isinstance(value, float) or isfinite(value),
                path,
                "Numeric values must be finite",
            )
            self.require_field(
                (FieldKey.MIN not in field or value >= field[FieldKey.MIN])
                and (
                    FieldKey.MAX not in field or value <= field[FieldKey.MAX]
                ),
                path,
                "Value is outside numeric limits",
            )
        if isinstance(value, list):
            self.require_field(
                len(value) >= field.get(FieldKey.MIN_ITEMS, 0)
                and (
                    FieldKey.MAX_ITEMS not in field
                    or len(value) <= field[FieldKey.MAX_ITEMS]
                ),
                path,
                "Value is outside item limits",
            )
        self.validate_literal_size(field, value, path)
        if field.get(FieldKey.FORMAT) == FieldFormat.UUID:
            try:
                uuid_pkg.UUID(value)
            except ValueError:
                self.require_field(False, path, "Value must be a UUID")

    def validate_literal_size(
        self,
        field: FieldDeclaration,
        value: JsonValue | bytes,
        path: tuple[str | int, ...],
    ) -> None:
        if isinstance(value, str) and FieldKey.MAX_LENGTH in field:
            self.require_field(
                len(value) <= field[FieldKey.MAX_LENGTH],
                path,
                "Value exceeds max_length",
            )
        if FieldKey.MAX_BYTES not in field:
            return
        if isinstance(value, bytes):
            size = len(value)
        elif field[FieldKey.TYPE] == FieldType.ANY or isinstance(value, str):
            try:
                text = (
                    json.dumps(
                        value,
                        ensure_ascii=False,
                        allow_nan=False,
                        separators=(",", ":"),
                    )
                    if field[FieldKey.TYPE] == FieldType.ANY
                    else value
                )
                size = len(text.encode("utf-8"))
            except ValueError, UnicodeEncodeError:
                self.require_field(False, path, "Value cannot be encoded")
                return
        else:
            return
        self.require_field(
            size <= field[FieldKey.MAX_BYTES],
            path,
            "Value exceeds max_bytes",
        )

    def validate_nested_literal(
        self,
        field: FieldDeclaration,
        value: JsonValue | bytes,
        path: tuple[str | int, ...],
    ) -> None:
        if field[FieldKey.TYPE] == FieldType.ARRAY:
            for index, item in enumerate(value):
                self.validate_literal(
                    field[FieldKey.ITEMS], item, (*path, index)
                )
            return
        if field[FieldKey.TYPE] != FieldType.OBJECT:
            return
        declarations = field.get(FieldKey.FIELDS, {})
        if FieldKey.VARIANTS in field:
            action = value.get(FieldKey.ACTION)
            self.require_field(
                isinstance(action, str) and action in field[FieldKey.VARIANTS],
                (*path, FieldKey.ACTION),
                "Action must select a declared variant",
            )
            declarations = field[FieldKey.VARIANTS][action]
        for name, child in declarations.items():
            if name not in value:
                self.require_field(
                    child[FieldKey.OPTIONAL],
                    (*path, name),
                    "Required field is missing",
                )
                continue
            self.validate_literal(child, value[name], (*path, name))
        for name in value:
            if name == FieldKey.ACTION and FieldKey.VARIANTS in field:
                continue
            self.require_field(
                name in declarations or field[FieldKey.EXTRA],
                (*path, name),
                "Unexpected field",
            )

    @classmethod
    def require_field(
        cls, valid: bool, path: tuple[str | int, ...], message: str
    ) -> None:
        if valid:
            return
        issue = InitErrorDetails(
            type="value_error", loc=path, input=None, ctx={"error": message}
        )
        raise ValidationError.from_exception_data(
            cls.__name__, [issue], hide_input=True
        )

    @classmethod
    def require_choice(
        cls, value: str, allowed: Collection[str], path: tuple[str | int, ...]
    ) -> None:
        if value in allowed:
            return
        issue = InitErrorDetails(
            type="enum",
            loc=path,
            input=None,
            ctx={"expected": ", ".join(allowed)},
        )
        raise ValidationError.from_exception_data(
            cls.__name__, [issue], hide_input=True
        )


class MeasurementStandard(BaseModel):
    standard: str
    version: str


class ManifestDefaults(TypedDict):
    codec: dict[str, str]
    optional: bool
    nullable: bool
    extra: bool
    routing: str
    qos: int


class ManifestNamePatterns(BaseModel):
    io: Pattern[str]
    env: Pattern[str]


class NumericTypeLimits[T: (int, float)](BaseModel):
    min: T
    max: T

    @model_validator(mode="after")
    def validate_order(self) -> Self:
        if self.min > self.max:
            msg = "Upper limit must not be less than lower limit"
            raise ValueError(msg)
        return self


class ManifestTypeLimits(BaseModel):
    int: NumericTypeLimits[int]
    float: NumericTypeLimits[FiniteFloat]


class ManifestDictionary(ManifestContractHeader):
    name_patterns: ManifestNamePatterns
    type_limits: ManifestTypeLimits
    unit_types: list[str]
    execution_types: list[str]
    types: list[str]
    platforms: list[str]
    formats: list[str]
    routing: list[str]
    units_of_measurement: MeasurementStandard
    env: FieldDeclarations
    input: FieldDeclarations
    output: FieldDeclarations
    defaults: ManifestDefaults
