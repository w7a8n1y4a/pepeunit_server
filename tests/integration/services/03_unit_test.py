import copy
import json
import logging
import os
import re
import uuid as uuid_pkg
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from pepeunit_client.file_manager import FileManager

from app import runtime, settings
from app.configs.clickhouse import get_clickhouse_client
from app.configs.errors import (
    CipherError,
    GitRepoError,
    MqttError,
    NoAccessError,
    RepoError,
    UnitError,
    ValidationError,
)
from app.dto.agent.abc import AgentBackend
from app.dto.clickhouse.log import UnitLog
from app.dto.enum import (
    BackendTopicCommand,
    LogLevel,
    OperationTaskStatus,
    OperationTaskType,
    OrderByDate,
    ReservedEnvVariableName,
    StaticRepoFileName,
    UnitIOTypeEnum,
    VisibilityLevel,
)
from app.repositories.unit_log_repository import UnitLogRepository
from app.schemas.pydantic.unit import (
    EnvJsonString,
    UnitCreate,
    UnitFilter,
    UnitLogFilter,
    UnitUpdate,
)
from app.services.utils import get_topic_name
from app.utils.utils import aes_gcm_encode
from tests.client.mqtt import (
    COUNTER_INPUT,
    COUNTER_OUTPUT,
    UNIT_ROOT,
    CounterUnit,
)
from tests.integration.helpers.http import (
    api_request,
    get_unit_logs_file,
    mqtt_authorization,
    patch_unit_commit,
    post_bulk_update_units,
    post_unit_command,
)
from tests.integration.helpers.names import unique_name
from tests.integration.helpers.services import (
    branch_commits,
    find_unit_io,
    repo_service,
    unit_io_service,
    unit_service,
)
from tests.integration.helpers.tasks import latest_task
from tests.integration.helpers.wait import wait_until


def test_create_unit(live_units) -> None:
    assert len(live_units.all()) == 9


def test_create_unit_duplicate_name(
    live_units, universal_repo, regular_user_token, database, cc
) -> None:
    service = unit_service(database, cc, regular_user_token)
    with pytest.raises(UnitError, match="Name is not unique"):
        service.create(
            UnitCreate(
                repo_uuid=universal_repo.uuid,
                visibility_level=VisibilityLevel.PUBLIC,
                name=live_units.universal_manual_unit.name,
                target_branch=live_units.universal_manual_unit.target_branch,
            )
        )


def test_create_unit_same_name_other_user(
    live_units, universal_repo, extra_user_token, database, cc
) -> None:
    service = unit_service(database, cc, extra_user_token)
    unit = service.create(
        UnitCreate(
            repo_uuid=universal_repo.uuid,
            visibility_level=VisibilityLevel.PUBLIC,
            name=live_units.universal_manual_unit.name,
            target_branch=live_units.universal_manual_unit.target_branch,
        )
    )
    try:
        assert (
            unit.creator_uuid != live_units.universal_manual_unit.creator_uuid
        )
    finally:
        service.delete(unit.uuid)


def test_create_unit_unknown_branch(
    universal_repo, regular_user_token, database, cc
) -> None:
    service = unit_service(database, cc, regular_user_token)
    with pytest.raises(GitRepoError):
        service.create(
            UnitCreate(
                repo_uuid=universal_repo.uuid,
                visibility_level=VisibilityLevel.PUBLIC,
                name=unique_name("nobr"),
                target_branch="branch-does-not-exist",
            )
        )


def test_delete_repo_with_unit(
    universal_repo, auxiliary_unit, regular_user_token, database
) -> None:
    repo_svc = repo_service(database, regular_user_token)
    with pytest.raises(RepoError, match="Repo still has units"):
        repo_svc.delete(universal_repo.uuid)


def test_update_unit_name(crud_unit, regular_user_token, database, cc) -> None:
    service = unit_service(database, cc, regular_user_token)
    original = crud_unit.name
    new_name = unique_name("unm")
    service.update(crud_unit.uuid, UnitUpdate(name=new_name))
    assert service.get(crud_unit.uuid).name == new_name
    service.update(crud_unit.uuid, UnitUpdate(name=original))


def test_update_unit_name_exists(
    live_units, regular_user_token, database, cc
) -> None:
    service = unit_service(database, cc, regular_user_token)
    with pytest.raises(UnitError, match="Name is not unique"):
        service.update(
            live_units.universal_auto_unit.uuid,
            UnitUpdate(name=live_units.universal_manual_unit.name),
        )


def test_update_unit_visibility(
    universal_manual_unit, regular_user_token, database, cc
) -> None:
    service = unit_service(database, cc, regular_user_token)
    logging.info(universal_manual_unit.uuid)

    service.update(
        universal_manual_unit.uuid,
        UnitUpdate(visibility_level=VisibilityLevel.INTERNAL),
    )
    assert (
        service.get(universal_manual_unit.uuid).visibility_level
        == VisibilityLevel.INTERNAL
    )

    service.update(
        universal_manual_unit.uuid,
        UnitUpdate(visibility_level=VisibilityLevel.PUBLIC),
    )
    assert (
        service.get(universal_manual_unit.uuid).visibility_level
        == VisibilityLevel.PUBLIC
    )


def test_update_unit_auto_flags(
    universal_auto_unit, regular_user_token, database, cc
) -> None:
    service = unit_service(database, cc, regular_user_token)
    with pytest.raises(UnitError):
        service.update(
            universal_auto_unit.uuid, UnitUpdate(is_auto_update_unit=False)
        )

    _read, commits = branch_commits(
        database,
        regular_user_token,
        universal_auto_unit.repo_uuid,
    )
    service.update(
        universal_auto_unit.uuid,
        UnitUpdate(
            is_auto_update_unit=False,
            target_branch=universal_auto_unit.target_branch,
            target_commit=commits[0].commit,
        ),
    )
    service.update(
        universal_auto_unit.uuid, UnitUpdate(is_auto_update_unit=True)
    )


def test_update_unit_not_creator(
    universal_auto_unit, admin_user_token, database, cc
) -> None:
    service = unit_service(database, cc, admin_user_token)
    with pytest.raises(NoAccessError):
        service.update(
            universal_auto_unit.uuid, UnitUpdate(is_auto_update_unit=True)
        )


def test_env_unit(
    universal_auto_unit, live_units, regular_user_token, database, cc
) -> None:
    service = unit_service(database, cc, regular_user_token)
    count = len(service.get_env(universal_auto_unit.uuid).keys())
    logging.info(f"{count}")
    assert count > 0

    service.set_env(universal_auto_unit.uuid, json.dumps({"test": ""}))
    current_env = service.get_env(universal_auto_unit.uuid)
    assert "test" not in current_env.keys()

    for unit in live_units.all():
        logging.info(unit.uuid)
        current_env = service.get_env(unit.uuid)
        count_before = len(current_env.keys())
        service.set_env(unit.uuid, json.dumps(current_env))
        count_after = len(service.get_env(unit.uuid).keys())
        assert count_before <= count_after


def test_reset_env(
    universal_auto_unit, regular_user_token, database, cc
) -> None:
    service = unit_service(database, cc, regular_user_token)
    current_env = service.get_env(universal_auto_unit.uuid)
    old_env = copy.deepcopy(current_env)

    service.reset_env(universal_auto_unit.uuid)
    service.set_env(
        universal_auto_unit.uuid,
        json.dumps(service.get_env(universal_auto_unit.uuid)),
    )
    new_env = service.get_env(universal_auto_unit.uuid)
    assert old_env.get(
        ReservedEnvVariableName.PU_SECRET_KEY.value
    ) != new_env.get(ReservedEnvVariableName.PU_SECRET_KEY.value)


def test_get_firmware(
    unpacked_firmwares, auxiliary_unit, regular_user_token, database, cc
) -> None:
    service = unit_service(database, cc, regular_user_token)
    with pytest.raises(UnitError):
        service.get_unit_firmware_tgz(auxiliary_unit.uuid, 35, 9)
    with pytest.raises(UnitError):
        service.get_unit_firmware_tgz(auxiliary_unit.uuid, 9, 13)

    target_version = service.get_selected_version(auxiliary_unit.uuid)
    assert target_version.commit != ""


def test_state_storage(
    universal_auto_unit, regular_user_token, database, cc
) -> None:
    service = unit_service(database, cc, regular_user_token)
    state = "test"
    service.set_state_storage(universal_auto_unit.uuid, state)
    assert state == service.get_state_storage(universal_auto_unit.uuid)

    with pytest.raises(CipherError):
        service.set_state_storage(
            universal_auto_unit.uuid,
            "t" * (runtime.get().pu_instance_cipher_max_length + 1),
        )


def test_run_infrastructure_contour(running_units) -> None:
    assert len(running_units.all()) == 9


def test_hand_update_firmware_unit(
    running_units, client_emulator, regular_user_token, database, cc
) -> None:
    service = unit_service(database, cc, regular_user_token)
    target_units = running_units.firmware_manual()
    startups = {
        unit.uuid: client_emulator.last_startup_log(unit.uuid)
        for unit in target_units
    }
    assert all(startups.values())
    expected_ssid = "archive-update"
    for unit in target_units:
        selected = service.get_selected_version(unit.uuid)
        service.set_env(
            unit.uuid, json.dumps({"PUC_WIFI_SSID": expected_ssid})
        )
        assert (
            patch_unit_commit(regular_user_token, unit, selected.commit) == 200
        )

    def all_updated() -> bool:
        client_emulator.require_running()
        return all(
            (startup := client_emulator.last_startup_log(unit.uuid))
            and startup != startups[unit.uuid]
            and FileManager.read_env(
                str(UNIT_ROOT / str(unit.uuid) / StaticRepoFileName.ENV)
            )["PUC_WIFI_SSID"]
            == expected_ssid
            for unit in target_units
        )

    wait_until(
        all_updated,
        timeout=40,
        message="firmware archive did not update the emulators' .env files",
    )


def test_hand_update_bad_commit(
    running_units, chain_source_unit, regular_user_token
) -> None:
    assert (
        patch_unit_commit(regular_user_token, chain_source_unit, "test") >= 400
    )
    assert (
        patch_unit_commit(
            regular_user_token,
            chain_source_unit,
            "6506d44fd80a895a57f2b34055521405d0f22860",
        )
        >= 400
    )


def test_hand_update_with_bad_env(
    running_units, universal_manual_unit, regular_user_token, database, cc
) -> None:
    token = regular_user_token
    service = unit_service(database, cc, token)

    env_dict = service.get_env(universal_manual_unit.uuid)
    del env_dict["PU_SECRET_KEY"]

    update_unit = service.get(universal_manual_unit.uuid)
    update_unit.cipher_env_dict = aes_gcm_encode(json.dumps(env_dict))
    service.unit_repository.update(universal_manual_unit.uuid, update_unit)

    _, commits = branch_commits(
        database, token, universal_manual_unit.repo_uuid
    )
    assert (
        patch_unit_commit(token, universal_manual_unit, commits[0].commit)
        == 200
    )


def test_auto_update_firmware_unit(
    running_units, admin_user_token, regular_user_token, database, cc
) -> None:
    service = unit_service(database, cc, regular_user_token)
    target_units = running_units.chain()[1:]
    expected_ssid = "bulk-update"
    for unit, tagged in zip(target_units, (False, True), strict=True):
        service.update(
            unit.uuid,
            UnitUpdate(is_auto_update_unit=True, is_only_tag_update=tagged),
        )
        selected = service.get_selected_version(unit.uuid)
        _, commits = branch_commits(
            database, regular_user_token, unit.repo_uuid, only_tag=tagged
        )
        assert selected.commit == commits[0].commit
        if tagged:
            assert selected.tag is not None
            assert selected.tag == commits[0].tag
        service.set_env(
            unit.uuid, json.dumps({"PUC_WIFI_SSID": expected_ssid})
        )

    assert post_bulk_update_units(admin_user_token) < 400

    def bulk_reached() -> bool:
        return all(
            FileManager.read_env(
                str(UNIT_ROOT / str(unit.uuid) / StaticRepoFileName.ENV)
            )["PUC_WIFI_SSID"]
            == expected_ssid
            for unit in target_units
        )

    wait_until(
        bulk_reached,
        timeout=40,
        message="bulk firmware update did not reach both auto-update emulators",
    )


def test_env_update_command(
    running_units,
    client_emulator,
    chain_sink_unit,
    regular_user_token,
    database,
    cc,
) -> None:
    input_io = find_unit_io(
        unit_io_service(database, cc, regular_user_token),
        chain_sink_unit.uuid,
        COUNTER_INPUT,
    )
    client_emulator.wait_for_counter_input(
        regular_user_token, chain_sink_unit.uuid, input_io.uuid
    )
    profile = {"label": "sdk-env", "limits": [2, 7]}
    updated = EnvJsonString(
        env_json_string=json.dumps(
            {
                ReservedEnvVariableName.PU_MIN_LOG_LEVEL: LogLevel.INFO,
                "TEST_PROFILE": profile,
            }
        )
    )
    response = api_request(
        "PATCH",
        f"/units/env/{chain_sink_unit.uuid}",
        regular_user_token,
        json=updated.model_dump(),
    )
    assert response.status_code == 204, response.text

    assert (
        post_unit_command(
            regular_user_token, chain_sink_unit, BackendTopicCommand.ENV_UPDATE
        )
        < 400
    )

    filepath = UNIT_ROOT / str(chain_sink_unit.uuid) / StaticRepoFileName.ENV

    def env_updated() -> bool:
        env = FileManager.read_env(filepath)
        return (
            env[ReservedEnvVariableName.PU_MIN_LOG_LEVEL] == LogLevel.INFO
            and env.get("TEST_PROFILE") == profile
        )

    wait_until(
        env_updated,
        timeout=30,
        interval=2,
        message=".env was not updated on unit",
    )


def test_log_sync_command(
    running_units, client_emulator, chain_middle_unit, regular_user_token
) -> None:
    token = regular_user_token
    client = next(get_clickhouse_client())
    try:
        unit_log_repository = UnitLogRepository(client)
        expected = client_emulator.last_startup_log(chain_middle_unit.uuid)
        assert expected is not None
        logs_filter = UnitLogFilter.unlimited(
            uuid=chain_middle_unit.uuid, level=[LogLevel.INFO.value]
        )
        assert all(
            entry.text != expected
            for entry in unit_log_repository.list(logs_filter)[1]
        )
        assert (
            post_unit_command(
                token, chain_middle_unit, BackendTopicCommand.LOG_SYNC
            )
            < 400
        )

        wait_until(
            lambda: any(
                entry.text == expected
                for entry in unit_log_repository.list(logs_filter)[1]
            ),
            timeout=30,
            interval=2,
            message="LogSync did not upload the startup log from the SDK file",
        )
    finally:
        client.disconnect()


def test_reset_command(
    running_units,
    client_emulator,
    chain_sink_unit,
    regular_user_token,
    database,
    cc,
) -> None:
    previous = client_emulator.last_startup_log(chain_sink_unit.uuid)
    assert previous is not None
    assert (
        post_unit_command(
            regular_user_token, chain_sink_unit, BackendTopicCommand.RESET
        )
        == 204
    )

    def restarted() -> bool:
        client_emulator.require_running()
        current = client_emulator.last_startup_log(chain_sink_unit.uuid)
        return current is not None and current != previous

    wait_until(restarted, timeout=30, message="Reset did not restart the Unit")
    input_io = find_unit_io(
        unit_io_service(database, cc, regular_user_token),
        chain_sink_unit.uuid,
        COUNTER_INPUT,
    )
    client_emulator.wait_for_counter_input(
        regular_user_token, chain_sink_unit.uuid, input_io.uuid
    )
    log_path = UNIT_ROOT / str(chain_sink_unit.uuid) / CounterUnit.LOG_FILENAME
    assert any(
        "Reset command received" in entry["text"]
        for entry in FileManager.iter_ndjson(log_path)
    )


def test_get_many_unit(
    live_units,
    universal_auto_unit,
    regular_user,
    regular_user_token,
    database,
    cc,
    test_hash,
) -> None:
    service = unit_service(database, cc, regular_user_token)
    count, units = service.list(
        UnitFilter.unlimited(
            creator_uuid=regular_user.uuid,
            repo_uuid=universal_auto_unit.repo_uuid,
            search_string=test_hash,
            is_auto_update_unit=True,
        )
    )
    assert count >= 1
    assert any(unit[0].uuid == universal_auto_unit.uuid for unit in units)


def test_get_unit_logs(
    running_units, chain_middle_unit, regular_user_token, database, cc
) -> None:
    service = unit_service(database, cc, regular_user_token)
    count, logs = service.log_list(
        UnitLogFilter(
            uuid=chain_middle_unit.uuid,
            offset=0,
            limit=runtime.get().pu_instance_max_pagination_size,
        )
    )
    assert len(logs) > 0


def test_download_unit_logs(
    live_units, regular_user_token, database, cc
) -> None:
    unit = live_units.universal_auto_unit
    service = unit_service(database, cc, regular_user_token)
    unit_log_repository = UnitLogRepository(cc)

    first_datetime = datetime(2026, 9, 6, 3, 15, 58, 214000)
    second_datetime = first_datetime + timedelta(seconds=1)
    expiration = datetime.now(UTC) + timedelta(
        seconds=runtime.get().pu_instance_unit_log_expiration
    )
    first_text = (
        "uvicorn.access - 192.168.0.10:48422 - "
        '"GET /pepeunit/api/v1/instances/current HTTP/1.0" 200'
    )
    second_text = "gmqtt.client - [SUBACK] N (0,)"

    unit_log_repository.bulk_create(
        [
            UnitLog(
                uuid=uuid_pkg.uuid4(),
                level=LogLevel.INFO,
                unit_uuid=unit.uuid,
                text=first_text,
                create_datetime=first_datetime,
                expiration_datetime=expiration,
            ),
            UnitLog(
                uuid=uuid_pkg.uuid4(),
                level=LogLevel.ERROR,
                unit_uuid=unit.uuid,
                text=second_text,
                create_datetime=second_datetime,
                expiration_datetime=expiration,
            ),
        ]
    )

    assert (
        UnitLog(
            uuid=uuid_pkg.uuid4(),
            level=LogLevel.INFO,
            unit_uuid=unit.uuid,
            text=first_text,
            create_datetime=first_datetime,
            expiration_datetime=expiration,
        ).to_log_line()
        == f"INFO - 2026-09-06 03:15:58,214 - {first_text}"
    )

    log_path, filename = service.get_unit_logs_file(unit.uuid)
    try:
        assert filename == f"{unit.name}.log"
        assert log_path.endswith(".log")
        with open(log_path, encoding="utf-8") as handle:
            content = handle.read()
    finally:
        os.remove(log_path)

    lines = content.splitlines()
    first_line = next(
        line for line in lines if line.endswith(f" - {first_text}")
    )
    second_line = next(
        line for line in lines if line.endswith(f" - {second_text}")
    )
    assert first_line.startswith("INFO - ")
    assert second_line.startswith("ERROR - ")
    assert lines.index(first_line) < lines.index(second_line)

    # the file holds the whole history, log_list gives only the first page of it
    stored_count, first_page = service.log_list(
        UnitLogFilter(
            uuid=unit.uuid,
            level=[item.value for item in LogLevel],
            order_by_create_date=OrderByDate.asc,
        )
    )
    assert len(lines) == stored_count
    assert lines[: len(first_page)] == [
        log.to_log_line() for log in first_page
    ]

    log_line_pattern = re.compile(
        r"^(DEBUG|INFO|WARNING|ERROR|CRITICAL) - "
        r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} - .+$"
    )
    assert all(log_line_pattern.match(line) for line in lines)

    response = get_unit_logs_file(regular_user_token, unit)
    assert response.status_code == 200
    assert (
        f'filename="{unit.name}.log"'
        in response.headers["content-disposition"]
    )
    assert response.text == content


@pytest.mark.asyncio
def test_get_firmware_archives(
    unpacked_firmwares, auxiliary_unit, regular_user_token, database, cc
) -> None:
    service = unit_service(database, cc, regular_user_token)
    zip_path = service.get_unit_firmware_zip(auxiliary_unit.uuid)
    tar_path = service.get_unit_firmware_tar(auxiliary_unit.uuid)
    try:
        assert os.path.exists(zip_path)
        assert os.path.exists(tar_path)
        assert os.path.getsize(zip_path) > 0
        assert os.path.getsize(tar_path) > 0
    finally:
        os.remove(zip_path)
        os.remove(tar_path)


def test_list_units_with_output_nodes(
    live_units, regular_user, regular_user_token, database, cc
) -> None:
    count, units = unit_service(database, cc, regular_user_token).list(
        UnitFilter.unlimited(
            creator_uuid=regular_user.uuid,
            unit_io_type=[item.value for item in UnitIOTypeEnum],
            unit_io_uuids=[],
        ),
        is_include_output_unit_ios=True,
    )
    assert count >= 1
    assert any(nodes for _, nodes in units)


def test_list_units_with_output_nodes_only_outputs(
    live_units, regular_user, regular_user_token, database, cc
) -> None:
    _, units = unit_service(database, cc, regular_user_token).list(
        UnitFilter.unlimited(
            creator_uuid=regular_user.uuid,
            unit_io_type=[UnitIOTypeEnum.OUTPUT.value],
            unit_io_uuids=[],
        ),
        is_include_output_unit_ios=True,
    )
    assert all(nodes for _, nodes in units)


def test_update_units_firmware(
    running_units, admin_user_token, database
) -> None:
    task_type = OperationTaskType.UPDATE_ALL_UNITS_FIRMWARE
    previous_task = latest_task(database, task_type)

    assert post_bulk_update_units(admin_user_token) < 400

    def is_finish() -> bool:
        task = latest_task(database, task_type)
        return (
            task is not None
            and (previous_task is None or task.uuid != previous_task.uuid)
            and task.status != OperationTaskStatus.RUNNING.value
        )

    wait_until(
        is_finish,
        timeout=300,
        message="units firmware update task not finished",
        session=database,
    )

    finished = latest_task(database, task_type)
    logging.info(finished.result)
    assert finished.status == OperationTaskStatus.SUCCESS.value
    assert finished.result.startswith("Updated ")


def test_mqtt_auth_checks_action_and_ownership(
    auxiliary_unit, auxiliary_unit_token, regular_user_token, database, cc
) -> None:
    service = unit_io_service(database, cc, regular_user_token)
    input_node = find_unit_io(service, auxiliary_unit.uuid, COUNTER_INPUT)
    output_node = find_unit_io(service, auxiliary_unit.uuid, COUNTER_OUTPUT)
    input_topic = get_topic_name(input_node.uuid)
    output_topic = get_topic_name(output_node.uuid)
    token = auxiliary_unit_token

    with httpx.Client(timeout=runtime.get().http_timeout()) as client:
        initial = mqtt_authorization(client, token, "subscribe", input_topic)
        repeated = mqtt_authorization(client, token, "subscribe", input_topic)
        on_output = mqtt_authorization(client, token, "publish", output_topic)

        assert initial == repeated == on_output == "allow"
        assert (
            mqtt_authorization(client, token, "publish", input_topic) == "deny"
        )
        assert (
            mqtt_authorization(client, token, "subscribe", output_topic)
            == "deny"
        )
        assert (
            mqtt_authorization(
                client, regular_user_token, "subscribe", input_topic
            )
            == "deny"
        )


def test_mqtt_auth_denies_input_of_another_unit(
    auxiliary_unit_token,
    chain_sink_unit,
    universal_manual_unit,
    regular_user_token,
    database,
    cc,
) -> None:
    service = unit_service(database, cc, auxiliary_unit_token)
    unit_ios = unit_io_service(database, cc, regular_user_token)

    for unit in (chain_sink_unit, universal_manual_unit):
        node = find_unit_io(unit_ios, unit.uuid, COUNTER_INPUT)
        with pytest.raises(NoAccessError):
            service.get_mqtt_auth(get_topic_name(node.uuid), is_subscribe=True)


def test_unit_mqtt_auth_denies_invalid_topics(
    auxiliary_unit, auxiliary_unit_token, regular_user_token, database, cc
) -> None:
    service = unit_service(database, cc, auxiliary_unit_token)
    unit_ios = unit_io_service(database, cc, regular_user_token)
    node = find_unit_io(unit_ios, auxiliary_unit.uuid, COUNTER_INPUT)
    topic = get_topic_name(node.uuid)

    with pytest.raises(ValidationError):
        service.get_mqtt_auth(f"{settings.pu_domain}/+", is_subscribe=True)

    for invalid in (
        f"{topic}/pepeunit",
        f"foreign/{node.uuid}",
        topic.upper(),
    ):
        with pytest.raises(MqttError):
            service.get_mqtt_auth(invalid, is_subscribe=True)


def test_mqtt_auth_backend_wildcard_and_input(
    auxiliary_unit, regular_user_token, database, cc
) -> None:
    backend_token = AgentBackend(
        name=settings.pu_domain
    ).generate_agent_token()
    service = unit_service(database, cc, backend_token)
    unit_ios = unit_io_service(database, cc, regular_user_token)
    input_node = find_unit_io(unit_ios, auxiliary_unit.uuid, COUNTER_INPUT)
    output_node = find_unit_io(unit_ios, auxiliary_unit.uuid, COUNTER_OUTPUT)
    input_topic = get_topic_name(input_node.uuid)

    service.get_mqtt_auth(f"{settings.pu_domain}/+", is_subscribe=True)
    service.get_mqtt_auth(input_topic, is_subscribe=False)

    with pytest.raises(NoAccessError):
        service.get_mqtt_auth(input_topic, is_subscribe=True)
    with pytest.raises(NoAccessError):
        service.get_mqtt_auth(
            get_topic_name(output_node.uuid), is_subscribe=False
        )


def test_mqtt_auth_rejects_unknown_action(auxiliary_unit_token) -> None:
    response = api_request(
        "POST",
        "/units/auth",
        params={"action": "connect"},
        json={
            "token": auxiliary_unit_token,
            "topic": f"{settings.pu_domain}/+",
        },
    )

    assert response.status_code == 422
