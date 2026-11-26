import json
import logging
import os
import uuid as uuid_pkg
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from shutil import rmtree

import pytest
from git import Actor
from git import Repo as GitRepo

from app import settings
from app.domain.repo_model import Repo
from app.dto.enum import GitPlatform, RepoStatus, StaticRepoFileName
from app.repositories.git_repo_repository import GitRepoRepository
from app.repositories.icon_repository import IconRepository
from app.schemas.pydantic.repo import (
    Credentials,
    RepoCreate,
)
from app.services.icon_image import IconImage
from app.utils.utils import ensure_timezone_aware
from tests.integration.helpers.http import patch_update_local_repository
from tests.integration.helpers.services import repo_service
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
    tagged_commit: str
    untagged_commit: str
    tag: str
    asset_url: str


@pytest.fixture
def compiled_repo(tmp_path, regular_user, database) -> CompiledRepo:
    manifest = Path("tests/data/unit_manifest/unit.yml")
    origin = GitRepo.init(tmp_path / "origin", initial_branch="main")
    manifest_name = StaticRepoFileName.UNIT_MANIFEST.value
    (Path(origin.working_tree_dir) / manifest_name).write_bytes(
        manifest.read_bytes()
    )
    author = Actor("Integration tests", "integration@example.test")
    origin.index.add([manifest_name])
    tagged = origin.index.commit(
        "Compiled manifest", author=author, committer=author
    )
    tag = origin.create_tag("fixture-release").name
    untagged = origin.index.commit(
        "Untagged version", author=author, committer=author
    )

    url = f"https://repo.pepeunit.test/{uuid_pkg.uuid4()}.git"
    asset = "https://repo.pepeunit.test/releases/esp32.bin"
    icon = IconRepository(database).stage(IconImage.generate(url))
    now = datetime.now(UTC)
    repo = Repo(
        creator_uuid=regular_user.uuid,
        icon_uuid=icon.uuid,
        platform=GitPlatform.GITLAB,
        repository_url=url,
        sync_status=RepoStatus.UPDATED,
        create_datetime=now,
        last_update_datetime=now,
        releases_data=json.dumps({tag: [["esp32", asset]]}),
    )
    path = Path(GitRepoRepository.get_path_physic_repository(repo))
    GitRepo.clone_from(origin.working_tree_dir, path)
    database.add(repo)
    database.commit()

    try:
        yield CompiledRepo(repo, tagged.hexsha, untagged.hexsha, tag, asset)
    finally:
        database.delete(repo)
        database.commit()
        rmtree(path, ignore_errors=True)
