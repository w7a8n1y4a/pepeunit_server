import pytest
from sqlmodel import Session

from app.configs.error_codes import ErrorCode
from app.configs.errors import RepoError
from app.domain.operation_task_model import OperationTask
from app.domain.repo_model import Repo
from app.dto.enum import RepoStatus
from tests.integration.helpers.services import repo_service
from tests.integration.helpers.tasks import drop_task
from tests.integration.helpers.wait import wait_task_finish, wait_until


@pytest.fixture(scope="session")
def repo_sync_task(
    github_public_repo: Repo,
    regular_user_token: str,
    database: Session,
    request: pytest.FixtureRequest,
) -> tuple[OperationTask, OperationTask]:
    service = repo_service(database, regular_user_token)
    repo_uuid = github_public_repo.uuid
    service.git_repo_repository.delete_repo(github_public_repo)
    task = service.schedule_update(repo_uuid).model_copy()
    try:
        wait_until(
            lambda: (
                service.get(repo_uuid).sync_status == RepoStatus.PROCESSING
            ),
            interval=0.05,
            message="The scheduled Git sync did not start",
            session=database,
        )
        with pytest.raises(RepoError) as error:
            service.update_local_repository(repo_uuid)
        assert error.value.error_code == ErrorCode.REPO_SYNC_TOO_SOON
    finally:
        finished = wait_task_finish(database, task, timeout=300)
        request.addfinalizer(lambda: drop_task(database, task.uuid))
    return task, finished
