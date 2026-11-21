from app.repositories.unit_manifest_repository import UnitManifestRepository
from app.schemas.pydantic.unit_manifest import UnitDefinition


class UnitManifestService:
    def __init__(
        self, unit_manifest_repository: UnitManifestRepository
    ) -> None:
        self.unit_manifest_repository = unit_manifest_repository

    def parse(self, content: str | bytes) -> UnitDefinition:
        unit, dictionary = self.unit_manifest_repository.read_manifest(content)
        unit.validate_search(dictionary)
        unit.resolve_fields(dictionary)
        unit.validate_fields(dictionary)
        return unit
