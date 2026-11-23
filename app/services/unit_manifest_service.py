from fastapi import Depends
from pydantic import ValidationError

from app.configs.error_codes import ErrorCode
from app.configs.errors import GitRepoError
from app.domain.repo_model import Repo
from app.dto.enum import AgentType, StaticRepoFileName
from app.repositories.git_repo_repository import GitRepoRepository
from app.repositories.unit_manifest_repository import UnitManifestRepository
from app.schemas.pydantic.unit_manifest import (
    ManifestDictionary,
    UnitDefinition,
)
from app.schemas.pydantic.unit_manifest_validation import (
    UnitManifestValidationErrorRead,
)
from app.services.access_service import AccessService
from app.services.unit_manifest_readme import UnitManifestReadme
from app.validators.content_size import is_valid_content_size


class UnitManifestService:
    MAX_UPLOAD_BYTES = 1024 * 1024

    def __init__(
        self,
        unit_manifest_repository: UnitManifestRepository,
        access_service: AccessService = Depends(),
    ) -> None:
        self.unit_manifest_repository = unit_manifest_repository
        self.access_service = access_service
        self.git_repo_repository = GitRepoRepository()

    def read_from_commit(
        self, repo: Repo, selected_commit: str
    ) -> tuple[UnitDefinition, ManifestDictionary]:
        content = self.git_repo_repository.get_file_bytes(
            repo, selected_commit, StaticRepoFileName.UNIT_MANIFEST.value
        )
        try:
            return self.parse(content)
        except ValidationError as error:
            raise GitRepoError(
                str(error), code=ErrorCode.GIT_REPO_UNIT_MANIFEST_INVALID
            ) from error

    def parse(
        self, content: str | bytes
    ) -> tuple[UnitDefinition, ManifestDictionary]:
        source = (
            content.encode("utf-8") if isinstance(content, str) else content
        )
        is_valid_content_size(
            source,
            self.MAX_UPLOAD_BYTES,
            code=ErrorCode.GIT_REPO_UNIT_MANIFEST_TOO_LARGE,
            message=f"Unit manifest exceeds {self.MAX_UPLOAD_BYTES} bytes",
            error_type=GitRepoError,
        )
        unit, dictionary = self.unit_manifest_repository.read_manifest(source)
        unit.validate_search(dictionary)
        unit.resolve_fields(dictionary)
        unit.validate_fields(dictionary)
        return unit, dictionary

    def validate(
        self, content: bytes
    ) -> list[UnitManifestValidationErrorRead]:
        try:
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

    def readme(self, content: bytes) -> str:
        self.access_service.authorization.check_access([AgentType.USER])
        try:
            unit, _ = self.parse(content)
        except ValidationError as error:
            raise GitRepoError(
                str(error), code=ErrorCode.GIT_REPO_UNIT_MANIFEST_INVALID
            ) from None
        return UnitManifestReadme(unit).generate()
