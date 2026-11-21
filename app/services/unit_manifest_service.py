from pydantic import ValidationError

from app.configs.error_codes import ErrorCode
from app.configs.errors import GitRepoError
from app.repositories.unit_manifest_repository import UnitManifestRepository
from app.schemas.pydantic.unit_manifest import UnitDefinition
from app.schemas.pydantic.unit_manifest_validation import (
    UnitManifestValidationErrorRead,
)
from app.validators.content_size import is_valid_content_size


class UnitManifestService:
    MAX_UPLOAD_BYTES = 1024 * 1024

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

    def validate(
        self, content: bytes
    ) -> list[UnitManifestValidationErrorRead]:
        try:
            is_valid_content_size(
                content,
                self.MAX_UPLOAD_BYTES,
                code=ErrorCode.GIT_REPO_UNIT_MANIFEST_TOO_LARGE,
                message=f"Unit manifest exceeds {self.MAX_UPLOAD_BYTES} bytes",
                error_type=GitRepoError,
            )
            self.parse(content)
        except GitRepoError as error:
            return [
                UnitManifestValidationErrorRead(
                    path=[],
                    message=error.raw_message,
                    error_code=error.error_code,
                )
            ]
        except ValidationError as error:
            issues = error.errors(
                include_input=False, include_context=False, include_url=False
            )
            return [
                UnitManifestValidationErrorRead(
                    path=list(issue["loc"]),
                    message=issue["msg"],
                    error_code=int(
                        ErrorCode.code_from_validation(
                            issue, ErrorCode.GIT_REPO_UNIT_MANIFEST_INVALID
                        )
                    ),
                )
                for issue in issues
            ]
        return []
