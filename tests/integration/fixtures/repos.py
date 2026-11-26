import json
import logging
import os
from dataclasses import dataclass

import pytest
from sqlmodel import Session

from app import settings
from app.configs.errors import GitRepoError
from app.domain.repo_model import Repo
from app.dto.enum import FieldKey, GitPlatform, RepoStatus
from app.repositories.git_repo_repository import GitRepoRepository
from app.schemas.pydantic.repo import (
    CommitRead,
    Credentials,
    RepoCreate,
)
from app.services.repo_service import RepoService
from app.utils.utils import ensure_timezone_aware
from tests.integration.helpers.http import patch_update_local_repository
from tests.integration.helpers.private_repos import load_private_repo_specs
from tests.integration.helpers.services import all_branch_commits, repo_service
from tests.integration.helpers.wait import wait_until


def _drop_known_repo(database, url: str) -> None:
    """The repo could return to the database through instance discovery, so it is recreated"""
    git_repo_repository = GitRepoRepository()
    repos = database.query(Repo).where(Repo.repository_url == url).all()
    for repo in repos:
        git_repo_repository.delete_repo(repo)
    database.query(Repo).where(Repo.repository_url == url).delete()
    database.commit()


def _create_repo(database, token, spec: dict, *, require_clone: bool = True):
    service = repo_service(database, token)
    _drop_known_repo(database, spec["link"])
    credentials = None
    if not spec["is_public"]:
        credentials = Credentials(
            username=spec["username"], pat_token=spec["pat_token"]
        )

    logging.info(spec["link"])
    repo = service.create(
        RepoCreate(
            repository_url=spec["link"],
            is_public_repository=spec["is_public"],
            platform=spec["platform"],
            credentials=credentials,
        )
    )
    wait_repo_sync(database, token, repo)

    clone_path = service.git_repo_repository.get_path_physic_repository(repo)
    cloned = os.path.exists(clone_path)
    if require_clone:
        synced = service.get(repo.uuid)
        assert synced.sync_status == RepoStatus.UPDATED, synced.sync_error
        assert cloned, f"Repository cache missing for {repo.repository_url}"
    return repo, cloned


def _sync_stamp(value) -> float | None:
    if value is None:
        return None
    return ensure_timezone_aware(value).timestamp()


def _sync_repos_on_backend(database, token, repos) -> None:
    """The test process already cloned these repos. The backend must clone them too."""
    service = repo_service(database, token)
    stamps = {
        repo.uuid: _sync_stamp(service.get(repo.uuid).sync_last_datetime)
        for repo in repos
    }
    for repo in repos:
        assert patch_update_local_repository(token, repo.uuid) < 400

    def backend_caught_up() -> bool:
        for repo in repos:
            current = service.get(repo.uuid)
            stamp = _sync_stamp(current.sync_last_datetime)
            if stamp == stamps[repo.uuid]:
                return False
            if current.sync_status not in (
                RepoStatus.UPDATED,
                RepoStatus.ERROR,
            ):
                return False
        return True

    wait_until(
        backend_caught_up,
        timeout=180,
        message="backend did not finish repo sync",
        session=database,
    )
    failed = []
    for repo in repos:
        current = service.get(repo.uuid)
        if current.sync_status != RepoStatus.UPDATED:
            failed.append(f"{current.repository_url}: {current.sync_error}")
    assert not failed, "; ".join(failed)


def wait_repo_sync(database, token, repo, *, timeout: float = 180) -> None:
    """create schedules the sync in the background, the clone does not appear immediately"""
    service = repo_service(database, token)
    wait_until(
        lambda: (
            service.get(repo.uuid).sync_status
            in (
                RepoStatus.UPDATED,
                RepoStatus.ERROR,
            )
        ),
        timeout=timeout,
        message=f"repo {repo.repository_url} not synced",
        session=database,
    )


@pytest.fixture(scope="session")
def github_public_repo(regular_user_token, database) -> object:
    repo, _ = _create_repo(
        database,
        regular_user_token,
        {
            "link": settings.pu_test_integration_github_public_repo_url,
            "is_public": True,
            "platform": GitPlatform.GITHUB,
        },
    )
    return repo


@pytest.fixture(scope="session")
def gitlab_public_repo(regular_user_token, database) -> object:
    repo, _ = _create_repo(
        database,
        regular_user_token,
        {
            "link": settings.pu_test_integration_gitlab_public_repo_url,
            "is_public": True,
            "platform": GitPlatform.GITLAB,
        },
    )
    return repo


@pytest.fixture(scope="session")
def universal_repo(regular_user_token, database) -> object:
    repo, _ = _create_repo(
        database,
        regular_user_token,
        {
            "link": settings.pu_test_integration_universal_repo_url,
            "is_public": True,
            "platform": GitPlatform.GITLAB,
        },
    )
    _sync_repos_on_backend(database, regular_user_token, [repo])
    return repo


@pytest.fixture(scope="session")
def public_repos(
    github_public_repo,
    gitlab_public_repo,
    universal_repo,
    regular_user_token,
    database,
) -> list:
    repos = [
        github_public_repo,
        gitlab_public_repo,
        universal_repo,
    ]
    _sync_repos_on_backend(
        database, regular_user_token, [github_public_repo, gitlab_public_repo]
    )
    return repos


@pytest.fixture(scope="session")
def private_repos(private_repo_enabled, regular_user_token, database) -> list:
    created = []
    for spec in private_repo_enabled:
        try:
            repo, cloned = _create_repo(
                database, regular_user_token, spec, require_clone=False
            )
        except Exception as exc:
            logging.warning("skip private repo %s: %s", spec.get("link"), exc)
            continue
        if not cloned:
            logging.warning(
                "skip private repo %s: clone failed (%s)",
                spec.get("link"),
                getattr(repo, "sync_error", None),
            )
            continue
        created.append(repo)

    if not created:
        pytest.skip("no private repos could be cloned")

    _sync_repos_on_backend(database, regular_user_token, created)
    return created


@pytest.fixture(scope="session")
def private_repo(private_repos):
    return private_repos[0]


@dataclass(frozen=True)
class CompiledRepo:
    repo: Repo
    branch: str
    tagged_commit: str
    untagged_commit: str
    tag: str
    asset_url: str


def _compiled_branch(
    service: RepoService,
    repo: Repo,
    branch: str,
    commits: list[CommitRead],
    assets: dict[str, str],
) -> CompiledRepo | None:
    tagged = untagged = None
    for commit in commits:
        if commit.tag:
            if tagged is not None or commit.tag not in assets:
                continue
        elif untagged is not None:
            continue

        try:
            manifest, _ = service.unit_manifest_service.read_from_commit(
                repo, commit.commit
            )
        except GitRepoError:
            continue
        probe_pin = manifest.env.get("PROBE_PIN")
        if (
            not manifest.is_compilable
            or probe_pin is None
            or probe_pin.get(FieldKey.OPTIONAL)
            or FieldKey.DEFAULT in probe_pin
        ):
            continue
        if commit.tag:
            tagged = commit
        else:
            untagged = commit
        if tagged and tagged.tag and untagged:
            return CompiledRepo(
                repo,
                branch,
                tagged.commit,
                untagged.commit,
                tagged.tag,
                assets[tagged.tag],
            )
    return None


def _find_compiled_repo(
    repos: list[Repo], token: str, database: Session
) -> CompiledRepo | None:
    service = repo_service(database, token)
    for repo in repos:
        releases = json.loads(repo.releases_data or "{}")
        assets = {
            tag: url
            for tag, release_assets in releases.items()
            for platform, url in release_assets
            if platform == "esp32" and url
        }
        if not assets:
            continue
        for branch in service.git_repo_repository.get_branches(repo):
            candidate = _compiled_branch(
                service,
                repo,
                branch,
                all_branch_commits(database, token, repo.uuid, branch),
                assets,
            )
            if candidate is not None:
                return candidate
    return None


@pytest.fixture(scope="session")
def compiled_repo(
    request: pytest.FixtureRequest,
    public_repos: list[Repo],
    regular_user_token: str,
    database: Session,
) -> CompiledRepo:
    candidate = _find_compiled_repo(public_repos, regular_user_token, database)
    if candidate is not None:
        return candidate

    if (
        settings.pu_test_integration_private_repo_enable
        and load_private_repo_specs()
    ):
        candidate = _find_compiled_repo(
            request.getfixturevalue("private_repos"),
            regular_user_token,
            database,
        )
        if candidate is not None:
            return candidate

    pytest.skip(
        "Configured test repositories have no branch with an esp32 release asset "
        "and an untagged compiled commit that both require PROBE_PIN"
    )
