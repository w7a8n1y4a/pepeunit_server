import json
import uuid as uuid_pkg
from collections.abc import Collection
from math import isfinite
from typing import NotRequired, TypedDict

from pydantic import BaseModel, Field, JsonValue, ValidationError
from pydantic_core import InitErrorDetails


class FieldDeclaration(TypedDict, total=False):
    type: str
    base: str
    min: int | float
    max: int | float
    max_bytes: int
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

    def validate_search(self, dictionary: ManifestDictionary) -> None:
        self.require_choice(
            self.search.type, dictionary.unit_types, ("search", "type")
        )
        self.require_choice(
            self.search.execution_type,
            dictionary.execution_types,
            ("search", "execution_type"),
        )
        for index, platform in enumerate(self.search.platforms):
            self.require_choice(
                platform, dictionary.platforms, ("search", "platforms", index)
            )

    def validate_fields(self, dictionary: ManifestDictionary) -> None:
        for section in ("env", "input", "output"):
            for name, declaration in getattr(self, section).items():
                self.validate_field(declaration, dictionary, (section, name))

    def validate_field(
        self,
        field: FieldDeclaration,
        dictionary: ManifestDictionary,
        path: tuple[str | int, ...],
    ) -> None:
        if "base" in field:
            self.require_field(
                len(path) == 2
                and path[0] != "env"
                and bool(field["base"])
                and len(field) == 1,
                path,
                "A base declaration must contain only a non-empty base in Input or Output",
            )
            return
        self.require_field(
            "type" in field, (*path, "type"), "Type is required"
        )
        kind = field["type"]
        self.require_choice(kind, dictionary.types, (*path, "type"))
        self.require_field(
            len(path) == 2 or kind != "bytes",
            (*path, "type"),
            "Raw bytes cannot be nested in a JSON value",
        )
        allowed = {
            "min": ("int", "float"),
            "max": ("int", "float"),
            "unit": ("int", "float"),
            "max_bytes": ("string", "any", "bytes"),
            "min_items": ("array",),
            "max_items": ("array",),
            "items": ("array",),
            "fields": ("object",),
            "variants": ("object",),
            "extra": ("object",),
            "format": ("string",),
            "enum": ("bool", "int", "float", "string"),
        }
        for key, kinds in allowed.items():
            if key in field:
                self.require_field(
                    kind in kinds, (*path, key), f"Not allowed for type {kind}"
                )
        self.validate_limits(field, path)
        if "unit" in field:
            self.require_field(
                bool(field["unit"].strip()), (*path, "unit"), "Unit is empty"
            )
        if "format" in field:
            self.require_choice(
                field["format"], dictionary.formats, (*path, "format")
            )
        if kind == "array":
            self.require_field(
                "items" in field, (*path, "items"), "Array items are required"
            )
        if kind == "object":
            self.require_field(
                not ("fields" in field and "variants" in field),
                path,
                "fields and variants are mutually exclusive",
            )
            self.require_field(
                "variants" not in field or not field.get("extra", False),
                (*path, "extra"),
                "variants forbid extra fields",
            )
        self.validate_transport(field, dictionary, path)
        self.validate_nested(field, dictionary, path)
        self.validate_literals(field, path)

    def validate_nested(
        self,
        field: FieldDeclaration,
        dictionary: ManifestDictionary,
        path: tuple[str | int, ...],
    ) -> None:
        for name, child in field.get("fields", {}).items():
            self.validate_field(child, dictionary, (*path, "fields", name))
        if "items" in field:
            self.validate_field(field["items"], dictionary, (*path, "items"))
        if "variants" not in field:
            return
        self.require_field(
            bool(field["variants"]),
            (*path, "variants"),
            "Variants must not be empty",
        )
        for variant, declarations in field["variants"].items():
            variant_path = (*path, "variants", variant)
            self.require_field(
                "action" not in declarations,
                (*variant_path, "action"),
                "The action field is defined by variants",
            )
            for name, child in declarations.items():
                self.validate_field(child, dictionary, (*variant_path, name))

    def validate_literals(
        self, field: FieldDeclaration, path: tuple[str | int, ...]
    ) -> None:
        if "enum" in field:
            values = field["enum"]
            self.require_field(
                bool(values), (*path, "enum"), "Enum must not be empty"
            )
            for index, value in enumerate(values):
                self.validate_literal(field, value, (*path, "enum", index))
                self.require_field(
                    value not in values[:index],
                    (*path, "enum", index),
                    "Enum values must be unique",
                )
        for key in ("default", "example"):
            if key not in field:
                continue
            value = field[key]
            self.validate_literal(field, value, (*path, key))

    def validate_limits(
        self, field: FieldDeclaration, path: tuple[str | int, ...]
    ) -> None:
        for key in ("min", "max"):
            if key not in field:
                continue
            bound = field[key]
            self.require_field(
                not isinstance(bound, float) or isfinite(bound),
                (*path, key),
                "Numeric limits must be finite",
            )
            self.require_field(
                field["type"] != "int" or type(bound) is int,
                (*path, key),
                "Integer limits must be integers",
            )
        for key in ("max_bytes", "min_items", "max_items"):
            if key in field:
                self.require_field(
                    field[key] >= 0, (*path, key), "Limit must be non-negative"
                )
        for lower, upper in (("min", "max"), ("min_items", "max_items")):
            if lower in field and upper in field:
                self.require_field(
                    field[lower] <= field[upper],
                    (*path, upper),
                    "Upper limit must not be less than lower limit",
                )

    def validate_transport(
        self,
        field: FieldDeclaration,
        dictionary: ManifestDictionary,
        path: tuple[str | int, ...],
    ) -> None:
        for key in ("codec", "qos", "routing"):
            if key in field:
                self.require_field(
                    len(path) == 2 and path[0] in ("input", "output"),
                    (*path, key),
                    "Transport options are only allowed on top-level Input or Output",
                )
        kind = field["type"]
        codec = field.get(
            "codec", "raw" if kind == "bytes" else dictionary.defaults["codec"]
        )
        self.require_field(
            (codec == "json" and kind != "bytes")
            or (codec == "utf8" and kind == "string")
            or (codec == "raw" and kind == "bytes"),
            (*path, "codec"),
            "Codec does not support this type",
        )
        self.require_field(
            codec == "json" or not field.get("nullable", False),
            (*path, "nullable"),
            "Nullable values require the JSON codec",
        )
        if "qos" in field:
            self.require_field(
                field["qos"] in (0, 1), (*path, "qos"), "QoS must be 0 or 1"
            )
        if "routing" in field:
            self.require_field(
                path[0] == "output",
                (*path, "routing"),
                "Routing is Output-only",
            )
            self.require_choice(
                field["routing"], dictionary.routing, (*path, "routing")
            )
            self.require_field(
                field["routing"] != "selected-link"
                or (kind == "object" and codec == "json"),
                (*path, "routing"),
                "selected-link requires a JSON object",
            )
        if "secret" in field:
            self.require_field(
                path[0] == "env", (*path, "secret"), "Secret is env-only"
            )

    def validate_literal(
        self,
        field: FieldDeclaration,
        value: JsonValue,
        path: tuple[str | int, ...],
    ) -> None:
        kind = field["type"]
        if value is None:
            self.require_field(
                kind == "any" or field.get("nullable", False),
                path,
                "Null requires nullable: true",
            )
            if kind != "any":
                return
        types = {
            "bool": (bool,),
            "int": (int,),
            "float": (int, float),
            "string": (str,),
            "object": (dict,),
            "array": (list,),
            "bytes": (),
        }
        self.require_field(
            kind == "any" or type(value) in types.get(kind, ()),
            path,
            "Value does not match the declared type",
        )
        if value is not None and "enum" in field:
            self.require_field(
                value in field["enum"], path, "Value must belong to enum"
            )
        self.validate_nested_literal(field, value, path)
        if type(value) in (int, float):
            self.require_field(
                not isinstance(value, float) or isfinite(value),
                path,
                "Numeric values must be finite",
            )
            self.require_field(
                ("min" not in field or value >= field["min"])
                and ("max" not in field or value <= field["max"]),
                path,
                "Value is outside numeric limits",
            )
        if isinstance(value, list):
            self.require_field(
                len(value) >= field.get("min_items", 0)
                and (
                    "max_items" not in field
                    or len(value) <= field["max_items"]
                ),
                path,
                "Value is outside item limits",
            )
        if "max_bytes" in field and (kind == "any" or isinstance(value, str)):
            try:
                text = (
                    json.dumps(
                        value,
                        ensure_ascii=False,
                        allow_nan=False,
                        separators=(",", ":"),
                    )
                    if kind == "any"
                    else value
                )
                size = len(text.encode("utf-8"))
            except ValueError, UnicodeEncodeError:
                self.require_field(False, path, "Value cannot be encoded")
                return
            self.require_field(
                size <= field["max_bytes"],
                path,
                "Value exceeds max_bytes",
            )
        if field.get("format") == "uuid":
            try:
                uuid_pkg.UUID(value)
            except ValueError:
                self.require_field(False, path, "Value must be a UUID")

    def validate_nested_literal(
        self,
        field: FieldDeclaration,
        value: JsonValue,
        path: tuple[str | int, ...],
    ) -> None:
        if field["type"] == "array":
            for index, item in enumerate(value):
                self.validate_literal(field["items"], item, (*path, index))
            return
        if field["type"] != "object":
            return
        declarations = field.get("fields", {})
        if "variants" in field:
            action = value.get("action")
            self.require_field(
                isinstance(action, str) and action in field["variants"],
                (*path, "action"),
                "Action must select a declared variant",
            )
            declarations = field["variants"][action]
        for name, child in declarations.items():
            if name not in value:
                self.require_field(
                    child.get("optional", False),
                    (*path, name),
                    "Required field is missing",
                )
                continue
            self.validate_literal(child, value[name], (*path, name))
        for name in value:
            if name == "action" and "variants" in field:
                continue
            self.require_field(
                name in declarations or field.get("extra", False),
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


class ManifestDictionary(ManifestContractHeader):
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
    defaults: dict[str, JsonValue]
