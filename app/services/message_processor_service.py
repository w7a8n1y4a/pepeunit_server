import json
import sys

from git.exc import GitCommandError
from pydantic import TypeAdapter

from app.configs.errors import GitRepoError
from app.domain.repo_model import Repo
from app.domain.unit_model import Unit
from app.repositories.git_repo_repository import GitRepoRepository
from app.schemas.pydantic.message_processor import UnitVersionSelection


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

    def run(self) -> None:
        try:
            units = TypeAdapter(list[UnitVersionSelection]).validate_json(
                sys.stdin.buffer.read()
            )
            selected_commits = self.select_commits(units)
        except ValueError as error:
            raise SystemExit(str(error)) from error
        json.dump(selected_commits, sys.stdout)


if __name__ == "__main__":
    MessageProcessorService().run()
