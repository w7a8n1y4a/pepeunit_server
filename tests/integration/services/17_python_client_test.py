import json
import uuid as uuid_pkg
from pathlib import Path
from queue import Queue

from clickhouse_driver import Client
from paho.mqtt.client import MQTTMessage
from pepeunit_client import AesGcmCipher, PepeunitClient
from pepeunit_client.file_manager import FileManager
from pepeunit_client.io_topics import IOTopic
from sqlmodel import Session

from app.domain.unit_model import Unit
from app.dto.enum import (
    BackendTopicCommand,
    BackendTopicCommandKey,
    LogLevel,
    StaticRepoFileName,
)
from app.schemas.pydantic.unit import EnvJsonString, UnitLogFilter
from app.schemas.pydantic.unit_io import LinkCreate
from tests.client.mqtt import COUNTER_INPUT, COUNTER_OUTPUT
from tests.integration.fixtures.units import PREVIOUS_MANIFEST_COMMIT
from tests.integration.helpers.http import (
    api_request,
    patch_input_state,
    patch_unit_commit,
    post_unit_command,
)
from tests.integration.helpers.services import unit_service
from tests.integration.helpers.wait import wait_until


def receive_unit_input(
    client: PepeunitClient, topic: IOTopic, token: str, value: int
) -> MQTTMessage:
    inbox: Queue[MQTTMessage] = Queue()
    payload = str(value)
    input_uuid = uuid_pkg.UUID(IOTopic.uuid_from_topic(topic.topic))

    def receive(unit: PepeunitClient, message: MQTTMessage) -> None:
        if (
            message.topic == topic.topic
            and message.payload == payload.encode()
        ):
            inbox.put(message)

    def delivered() -> bool:
        if not inbox.empty():
            return True
        assert patch_input_state(token, input_uuid, payload) == 200
        return not inbox.empty()

    client.set_mqtt_input_handler(receive)
    wait_until(
        delivered, interval=0.5, message=f"SDK did not receive {topic.topic}"
    )
    return inbox.get_nowait()


def test_io_topics_update_command(
    sdk_unit: PepeunitClient,
    crud_unit: Unit,
    regular_user_token: str,
    tmp_path: Path,
) -> None:
    latest = sdk_unit.settings.PU_COMMIT_VERSION
    assert (
        patch_unit_commit(
            regular_user_token, crud_unit, PREVIOUS_MANIFEST_COMMIT
        )
        == 200
    )
    archive = tmp_path / "previous.tgz"
    rest = sdk_unit.rest_client
    assert rest is not None
    rest.download_update(str(archive))
    sdk_unit.update_device_program(str(archive))
    assert sdk_unit.settings.PU_COMMIT_VERSION == PREVIOUS_MANIFEST_COMMIT
    assert (tmp_path / StaticRepoFileName.UNIT_MANIFEST).is_file()

    updates: Queue[dict[str, str]] = Queue()
    sdk_unit.set_custom_update_handler(lambda _, payload: updates.put(payload))
    sdk_unit.connect()
    receive_unit_input(
        sdk_unit,
        sdk_unit.io_topics.input[COUNTER_INPUT],
        regular_user_token,
        1,
    )
    assert "bounded_counter" not in sdk_unit.io_topics.input

    assert patch_unit_commit(regular_user_token, crud_unit, latest) == 200
    assert updates.get(timeout=10) == {
        BackendTopicCommandKey.COMMAND: BackendTopicCommand.UPDATE.value,
        BackendTopicCommandKey.COMMIT_VERSION: latest,
    }
    assert "bounded_counter" not in sdk_unit.io_topics.input
    assert (
        post_unit_command(
            regular_user_token, crud_unit, BackendTopicCommand.IO_TOPICS_UPDATE
        )
        == 204
    )
    wait_until(
        lambda: "bounded_counter" in sdk_unit.io_topics.input,
        message="IOTopicsUpdate did not load the new manifest's input",
    )

    topic = sdk_unit.io_topics.input["bounded_counter"]
    message = receive_unit_input(sdk_unit, topic, regular_user_token, 4)
    assert message.qos == topic.qos == 1
    assert sdk_unit.settings.PU_COMMIT_VERSION == PREVIOUS_MANIFEST_COMMIT


def test_sdk_downloads_env_and_persists_encrypted_state(
    sdk_unit: PepeunitClient,
    crud_unit: Unit,
    regular_user_token: str,
    tmp_path: Path,
) -> None:
    profile = {"label": "sdk-profile", "limits": [5, 42]}
    patch = EnvJsonString(
        env_json_string=json.dumps({"TEST_PROFILE": profile})
    )
    response = api_request(
        "PATCH",
        f"/units/env/{crud_unit.uuid}",
        regular_user_token,
        json=patch.model_dump(),
    )
    assert response.status_code == 204, response.text

    download = tmp_path / "downloaded.env"
    sdk_unit.download_env(str(download))
    assert FileManager.read_env(download)["TEST_PROFILE"] == profile
    sdk_unit.download_env(sdk_unit.env_file_path)
    assert sdk_unit.settings.TEST_PROFILE == profile

    key = sdk_unit.settings.PU_ENCRYPT_KEY
    cipher = AesGcmCipher()
    value = json.dumps(profile, ensure_ascii=False)
    encrypted = cipher.aes_gcm_encode(value, key)
    sdk_unit.set_state_storage(encrypted)

    response = api_request(
        "GET", f"/units/get_state_storage/{crud_unit.uuid}", regular_user_token
    )
    response.raise_for_status()
    assert response.json() == encrypted
    assert cipher.aes_gcm_decode(sdk_unit.get_state_storage(), key) == value


def test_sdk_discovers_linked_unit(
    sdk_unit: PepeunitClient,
    auxiliary_unit: Unit,
    regular_user_token: str,
    database: Session,
    cc: Client,
) -> None:
    sdk_unit.refresh_io_topics()
    output = sdk_unit.io_topics.output[COUNTER_OUTPUT]
    target = unit_service(database, cc, regular_user_token).get_io_topics(
        auxiliary_unit.uuid
    )
    link = LinkCreate(
        output_uuid=uuid_pkg.UUID(IOTopic.uuid_from_topic(output.topic)),
        input_uuid=uuid_pkg.UUID(
            IOTopic.uuid_from_topic(target.input[COUNTER_INPUT].topic)
        ),
    )
    response = api_request(
        "POST",
        "/unit_ios/create_link",
        regular_user_token,
        json=link.model_dump(mode="json"),
    )
    assert response.status_code == 201, response.text

    rest = sdk_unit.rest_client
    assert rest is not None
    linked = rest.get_input_by_output(output.topic, limit=1)
    assert linked["count"] == 1
    input_uuids = [item["uuid"] for item in linked["unit_ios"]]
    assert input_uuids == [str(link.input_uuid)]
    units = rest.get_units_by_ios(input_uuids, limit=1)
    assert units["count"] == 1
    assert [unit["uuid"] for unit in units["units"]] == [
        str(auxiliary_unit.uuid)
    ]


def test_sdk_syncs_every_log_level(
    sdk_unit: PepeunitClient,
    crud_unit: Unit,
    regular_user_token: str,
    database: Session,
    cc: Client,
) -> None:
    sdk_unit.connect()
    receive_unit_input(
        sdk_unit,
        sdk_unit.io_topics.input[COUNTER_INPUT],
        regular_user_token,
        2,
    )
    logger = sdk_unit.logger
    service = unit_service(database, cc, regular_user_token)
    filters = UnitLogFilter.unlimited(
        uuid=crud_unit.uuid, level=[level.value for level in LogLevel]
    )
    online_log = f"SDK connected to the log pipeline {crud_unit.uuid}"

    def live_log_stored() -> bool:
        if any(
            entry.text == online_log for entry in service.log_list(filters)[1]
        ):
            return True
        logger.info(online_log)
        return False

    wait_until(
        live_log_stored,
        timeout=30,
        message="MessageProcessor did not store a live SDK log",
    )
    logger.reset_log()
    assert logger.get_full_log() == []
    expected = {}
    for level in LogLevel:
        text = f"SDK counter test {level.value} {crud_unit.uuid}"
        getattr(logger, level.value.lower())(text, file_only=True)
        expected[text] = level.value
    assert {
        entry["text"]: entry["level"] for entry in logger.iter_log()
    } == expected

    assert not any(
        entry.text in expected for entry in service.log_list(filters)[1]
    )
    assert (
        post_unit_command(
            regular_user_token, crud_unit, BackendTopicCommand.LOG_SYNC
        )
        == 204
    )

    def synced() -> bool:
        _, entries = service.log_list(filters)
        return {
            entry.text: entry.level.value
            for entry in entries
            if entry.text in expected
        } == expected

    wait_until(
        synced, timeout=30, message="SDK did not upload its NDJSON logs"
    )
