import pytest

from app.configs.errors import NoAccessError
from app.dto.enum import OperationTaskStatus, OperationTaskType
from app.schemas.pydantic.operation_task import OperationTaskCreate
from tests.integration.helpers.services import operation_task_service


@pytest.mark.notification
def test_operation_task_alerts(repo_task_alerts, repo_sync_task) -> None:
    started, stored = repo_task_alerts
    _, finished = repo_sync_task

    assert started.user_uuid == stored.user_uuid == finished.creator_uuid
    assert started.is_read is False
    assert stored.is_read is False
    assert started.is_processed is True
    assert stored.is_processed is True
    assert started.big_text is None
    assert stored.big_text is None
    assert started.small_text == "UpdateRepo: Running"
    assert stored.small_text == "UpdateRepo: Repo updated"
    assert (
        "Update Repo" in started.table_text and "Running" in started.table_text
    )
    assert (
        "Update Repo" in stored.table_text and "Success" in stored.table_text
    )
    assert stored.data["status"] == OperationTaskStatus.SUCCESS
    assert "Repo updated" in stored.table_text


def test_operation_task_anonymous(database) -> None:
    service = operation_task_service(database, None)
    with pytest.raises(NoAccessError):
        service.create(
            OperationTaskCreate(task_type=OperationTaskType.UPDATE_REPO)
        )
