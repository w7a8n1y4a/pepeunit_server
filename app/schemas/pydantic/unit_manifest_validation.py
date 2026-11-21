from pydantic import BaseModel


class UnitManifestValidationErrorRead(BaseModel):
    path: list[str | int]
    message: str
    error_code: int
