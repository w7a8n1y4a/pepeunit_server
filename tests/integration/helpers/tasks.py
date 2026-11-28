from app.domain.operation_task_model import OperationTask
from app.dto.enum import OperationTaskType
from app.repositories.operation_task_repository import OperationTaskRepository


def latest_task(
    database,
    task_type: OperationTaskType,
) -> OperationTask | None:
    return OperationTaskRepository(database).get_latest_by_type(task_type)


def drop_task(database, uuid) -> None:
    try:
        OperationTaskRepository(database).delete(OperationTask(uuid=uuid))
    except Exception:
        pass
