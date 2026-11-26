import uuid as uuid_pkg
from typing import TypedDict

from pydantic import Base64Bytes, BaseModel

from app.schemas.pydantic.unit_manifest import FieldDeclarations


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


class SnapshotCommitResult(TypedDict, total=False):
    commit: str
    error: str


class SnapshotManifestFields(TypedDict):
    input: FieldDeclarations
    output: FieldDeclarations


class SnapshotManifestResult(TypedDict, total=False):
    manifest: SnapshotManifestFields
    error: str
