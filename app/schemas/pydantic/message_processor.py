import uuid as uuid_pkg

from pydantic import Base64Bytes, BaseModel


class UnitVersionSelection(BaseModel):
    unit_uuid: uuid_pkg.UUID
    repo_uuid: uuid_pkg.UUID
    target_branch: str
    target_commit: str | None
    is_auto_update_unit: bool
    is_compilable_unit: bool
    is_only_tag_update: bool


class SnapshotManifestSource(BaseModel):
    repo_uuid: uuid_pkg.UUID
    commit: str
    content: Base64Bytes
