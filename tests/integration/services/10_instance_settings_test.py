import pytest

from app.configs.errors import InstanceError, NoAccessError, UserError
from app.domain.operation_task_model import OperationTask
from app.dto.enum import OperationTaskStatus, OperationTaskType
from app.repositories.operation_task_repository import OperationTaskRepository
from app.repositories.user_repository import UserRepository
from app.schemas.pydantic.instance_settings import InstanceSettingsUpdate
from app.schemas.pydantic.operation_task import OperationTaskCreate
from app.schemas.pydantic.user import UserCreate
from app.services.instance_settings_service import InstanceSettingsService
from tests.integration.helpers.instance_settings import wait_runtime
from tests.integration.helpers.keys import fresh_key
from tests.integration.helpers.owner import as_owner
from tests.integration.helpers.services import (
    instance_settings_service,
    operation_task_service,
    user_service,
)


def test_settings_are_owner_only(
    database, extra_user, extra_user_token, admin_user_token
) -> None:
    with pytest.raises(NoAccessError):
        instance_settings_service(database, extra_user_token).get()
    with pytest.raises(NoAccessError):
        instance_settings_service(database, admin_user_token).update(
            InstanceSettingsUpdate(pu_instance_user_max_group=8)
        )

    with as_owner(database, extra_user):
        instance_settings_service(database, extra_user_token).get()


def test_user_count_limit(database, cc, extra_user, extra_user_token) -> None:
    with as_owner(database, extra_user):
        settings_service = instance_settings_service(
            database, extra_user_token
        )
        current = settings_service.get().pu_instance_user_max_count
        limit = UserRepository(database).get_all_count()
        try:
            settings_service.update(
                InstanceSettingsUpdate(pu_instance_user_max_count=limit)
            )
            wait_runtime("pu_instance_user_max_count", limit)
            with pytest.raises(UserError):
                user_service(database, cc, None).create(
                    UserCreate(public_key=fresh_key().public_key)
                )
        finally:
            settings_service.update(
                InstanceSettingsUpdate(pu_instance_user_max_count=current)
            )
            wait_runtime("pu_instance_user_max_count", current)


def test_group_limit_range(database, extra_user, extra_user_token) -> None:
    with as_owner(database, extra_user):
        service = instance_settings_service(database, extra_user_token)
        current = service.get().pu_instance_user_max_group
        try:
            with pytest.raises(InstanceError):
                service.update(
                    InstanceSettingsUpdate(pu_instance_user_max_group=7)
                )
            with pytest.raises(InstanceError):
                service.update(
                    InstanceSettingsUpdate(pu_instance_user_max_group=33)
                )
            assert (
                service.update(
                    InstanceSettingsUpdate(pu_instance_user_max_group=8)
                ).pu_instance_user_max_group
                == 8
            )
            assert (
                service.update(
                    InstanceSettingsUpdate(pu_instance_user_max_group=32)
                ).pu_instance_user_max_group
                == 32
            )
        finally:
            service.update(
                InstanceSettingsUpdate(pu_instance_user_max_group=current)
            )
            wait_runtime("pu_instance_user_max_group", current)


def test_reboot_is_marked_on_start(
    database, extra_user, extra_user_token
) -> None:
    with as_owner(database, extra_user):
        task = operation_task_service(database, extra_user_token).create(
            OperationTaskCreate(task_type=OperationTaskType.REBOOT_INSTANCE)
        )
        assert task.task_type == OperationTaskType.REBOOT_INSTANCE.value
        assert task.status == OperationTaskStatus.RUNNING.value

        InstanceSettingsService.mark_started(database)
        database.expire_all()
        finished = OperationTaskRepository(database).get(
            OperationTask(uuid=task.uuid)
        )
        assert finished.status == OperationTaskStatus.SUCCESS.value
        assert finished.result == "Instance started"
        assert finished.finish_datetime is not None
