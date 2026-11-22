from fastapi import APIRouter, Depends, UploadFile
from starlette.responses import PlainTextResponse

from app.configs.rest import get_unit_manifest_service
from app.schemas.pydantic.unit_manifest_validation import (
    UnitManifestValidationErrorRead,
)
from app.services.unit_manifest_service import UnitManifestService

router = APIRouter()


@router.post("/validate", response_model=list[UnitManifestValidationErrorRead])
def validate(
    file: UploadFile,
    service: UnitManifestService = Depends(get_unit_manifest_service),
) -> list[UnitManifestValidationErrorRead]:
    content = file.file.read(UnitManifestService.MAX_UPLOAD_BYTES + 1)
    return service.validate(content)


@router.post("/readme", response_class=PlainTextResponse)
def readme(
    file: UploadFile,
    service: UnitManifestService = Depends(get_unit_manifest_service),
) -> PlainTextResponse:
    content = file.file.read(UnitManifestService.MAX_UPLOAD_BYTES + 1)
    return PlainTextResponse(
        service.readme(content), media_type="text/markdown"
    )
