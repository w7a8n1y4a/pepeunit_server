from importlib.resources import files

from app.dto.enum import FieldKey, ManifestContractVersion, SchemaFileName
from app.repositories.base_yaml_repository import BaseYamlRepository
from app.schemas import unit_manifest
from app.schemas.pydantic.unit_manifest import (
    ManifestContractHeader,
    ManifestDictionary,
    UnitDefinition,
)


class UnitManifestRepository(BaseYamlRepository):
    def read_manifest(
        self, content: str | bytes
    ) -> tuple[UnitDefinition, ManifestDictionary]:
        document = self.load_yaml(content, UnitDefinition)
        header = self.validate_model(
            ManifestContractHeader, document, extra="ignore"
        )
        dictionary = self.get_dictionary(header.contract_version)
        return self.validate_model(UnitDefinition, document), dictionary

    def get_dictionary(self, contract_version: str) -> ManifestDictionary:
        UnitDefinition.require_choice(
            contract_version,
            ManifestContractVersion,
            (FieldKey.CONTRACT_VERSION,),
        )
        source = files(unit_manifest).joinpath(
            contract_version, SchemaFileName.DICTIONARY.value
        )
        document = self.load_yaml(source.read_bytes(), ManifestDictionary)
        dictionary = self.validate_model(ManifestDictionary, document)
        if dictionary.contract_version != contract_version:
            msg = f"Dictionary {source} must declare contract_version {contract_version}"
            raise RuntimeError(msg)
        return dictionary
