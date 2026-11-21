from collections.abc import Collection
from typing import NotRequired, TypedDict

from pydantic import BaseModel, Field, JsonValue, ValidationError
from pydantic_core import InitErrorDetails

type FieldDeclarations = dict[str, dict[str, JsonValue]]


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
