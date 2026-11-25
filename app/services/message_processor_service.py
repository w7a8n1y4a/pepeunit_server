import argparse
import json
import sys

from git.exc import GitCommandError
from pydantic import TypeAdapter

from app.configs.errors import GitRepoError
from app.domain.repo_model import Repo
from app.domain.unit_model import Unit
from app.repositories.git_repo_repository import GitRepoRepository
from app.repositories.unit_manifest_repository import UnitManifestRepository
from app.schemas.pydantic.message_processor import (
    SnapshotManifestSource,
    UnitVersionSelection,
)
from app.schemas.pydantic.unit_manifest import FieldDeclarations
from app.services.unit_manifest_service import UnitManifestService


class MessageProcessorService:
    def __init__(self) -> None:
        self.git_repo_repository = GitRepoRepository(local_only=True)

    def select_commits(
        self, units: list[UnitVersionSelection]
    ) -> dict[str, str]:
        commits = {}
        selected = {}
        for unit in units:
            version = (
                unit.repo_uuid,
                unit.target_branch,
                unit.target_commit if not unit.is_auto_update_unit else None,
                unit.is_auto_update_unit,
                unit.is_compilable_unit,
                unit.is_only_tag_update,
            )
            if version not in commits:
                try:
                    selected_commit, _ = (
                        self.git_repo_repository.get_selected_commit_and_tag(
                            Repo(uuid=unit.repo_uuid),
                            Unit(
                                uuid=unit.unit_uuid,
                                repo_uuid=unit.repo_uuid,
                                target_branch=unit.target_branch,
                                target_commit=unit.target_commit,
                                is_auto_update_unit=unit.is_auto_update_unit,
                                is_compilable_unit=unit.is_compilable_unit,
                                is_only_tag_update=unit.is_only_tag_update,
                            ),
                        )
                    )
                except (GitRepoError, GitCommandError) as error:
                    message = f"Unit {unit.unit_uuid}, repo {unit.repo_uuid}: {error}"
                    raise ValueError(message) from error
                commits[version] = selected_commit
            selected[str(unit.unit_uuid)] = commits[version]
        return selected

    def resolve_manifests(
        self, sources: list[SnapshotManifestSource]
    ) -> list[dict[str, FieldDeclarations]]:
        service = UnitManifestService(UnitManifestRepository())
        manifests = []
        for source in sources:
            try:
                manifest, _ = service.parse_repo_manifest(source.content)
            except GitRepoError as error:
                message = (
                    f"Repo {source.repo_uuid} at {source.commit}: {error}"
                )
                raise ValueError(message) from error
            manifests.append(
                {"input": manifest.input, "output": manifest.output}
            )
        return manifests

    def run(self) -> None:
        parser = argparse.ArgumentParser()
        parser.add_argument("--manifests", action="store_true")
        options = parser.parse_args()
        body = sys.stdin.buffer.read()
        try:
            if options.manifests:
                sources = TypeAdapter(
                    list[SnapshotManifestSource]
                ).validate_json(body)
                result = self.resolve_manifests(sources)
            else:
                units = TypeAdapter(list[UnitVersionSelection]).validate_json(
                    body
                )
                result = self.select_commits(units)
        except ValueError as error:
            raise SystemExit(str(error)) from error
        json.dump(result, sys.stdout, allow_nan=False)


if __name__ == "__main__":
    MessageProcessorService().run()
