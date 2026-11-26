import base64
import json
import uuid as uuid_pkg
from datetime import timedelta
from pathlib import Path

import pytest

from app import runtime
from app.configs.error_codes import ErrorCode
from app.configs.errors import (
    CustomJSONDecodeError,
    DataPipeError,
    NoAccessError,
    UnitIOError,
)
from app.dto.enum import (
    AggregationFunctions,
    FieldType,
    MqttQos,
    ProcessingPolicyType,
    TypeInputValue,
    UnitIOCompatibilityIssue,
)
from app.schemas.pydantic.unit_io import (
    DataPipeFilter,
    LinkCreate,
    UnitIOSetState,
)
from app.utils.utils import create_upload_file_from_path
from tests.client.mqtt import COUNTER_OUTPUT
from tests.integration.helpers.data_pipe import activate_pipe
from tests.integration.helpers.http import (
    patch_input_state,
    preview_input_state,
)
from tests.integration.helpers.mqtt import (
    link_between,
    publish_and_receive,
    topic_uuid,
)
from tests.integration.helpers.services import find_unit_io, unit_io_service
from tests.integration.helpers.wait import wait_until

PIPE_CONFIGS = "tests/data/yaml/integra"
BOUNDED_PIPE = f"{PIPE_CONFIGS}/data_pipe_bounded_counter.yaml"
RAW_INPUT_PAYLOAD = b"\x00\xff\xc2pepe"


def test_preview_does_not_publish_or_store_raw_input(
    mqtt_unit, regular_user_token, database, cc
) -> None:
    service = unit_io_service(database, cc, regular_user_token)
    topic = mqtt_unit.topics.input["binary_input"]
    uuid = topic_uuid(topic)
    state = base64.b64encode(RAW_INPUT_PAYLOAD).decode()

    with mqtt_unit.connected():
        mqtt_unit.subscribe(topic)
        status = preview_input_state(regular_user_token, uuid, state)

        assert status == 204
        assert service.get(uuid).state is None
        assert mqtt_unit.receive() is None


def test_set_input_state_publishes_raw_payload(
    mqtt_unit, regular_user_token, database, cc
) -> None:
    service = unit_io_service(database, cc, regular_user_token)
    topic = mqtt_unit.topics.input["binary_input"]
    uuid = topic_uuid(topic)
    state = base64.b64encode(RAW_INPUT_PAYLOAD).decode()

    with mqtt_unit.connected():
        mqtt_unit.subscribe(topic)
        assert patch_input_state(regular_user_token, uuid, state) == 200
        received = mqtt_unit.receive(timeout=5)

    assert received is not None
    assert received.topic == topic.topic
    assert received.payload == RAW_INPUT_PAYLOAD
    assert received.qos == MqttQos.AT_LEAST_ONCE.value
    database.expire_all()
    assert service.get(uuid).state == state


def test_set_input_state_rejects_malformed_base64(
    crud_unit, regular_user_token, database, cc
) -> None:
    service = unit_io_service(database, cc, regular_user_token)
    node = find_unit_io(service, crud_unit.uuid, "binary_input")

    with pytest.raises(UnitIOError) as error:
        service.set_state_input(node.uuid, UnitIOSetState(state="not base64!"))

    assert error.value.error_code == ErrorCode.UNIT_IO_STATE_INVALID


def test_preview_rejects_oversized_raw_input(
    crud_unit, regular_user_token, database, cc
) -> None:
    service = unit_io_service(database, cc, regular_user_token)
    node = find_unit_io(service, crud_unit.uuid, "binary_input")
    state = base64.b64encode(b"x" * 17).decode()

    with pytest.raises(UnitIOError) as error:
        service.check_input_state(node.uuid, UnitIOSetState(state=state))

    assert error.value.error_code == ErrorCode.UNIT_IO_STATE_INVALID


@pytest.mark.parametrize(
    "state",
    ['{"action":"reset"}', '{"action":"set","value":5}'],
    ids=["reset", "set"],
)
def test_preview_accepts_known_json_variants(
    crud_unit, regular_user_token, database, cc, state
) -> None:
    service = unit_io_service(database, cc, regular_user_token)
    node = find_unit_io(service, crud_unit.uuid, "control")

    _, payload, _ = service.check_input_state(
        node.uuid, UnitIOSetState(state=state)
    )

    assert payload == state.encode()


def test_preview_rejects_malformed_json(
    crud_unit, regular_user_token, database, cc
) -> None:
    service = unit_io_service(database, cc, regular_user_token)
    node = find_unit_io(service, crud_unit.uuid, "control")

    with pytest.raises(CustomJSONDecodeError) as error:
        service.check_input_state(node.uuid, UnitIOSetState(state="{"))

    assert error.value.error_code == ErrorCode.UNIT_IO_STATE_JSON_INVALID


@pytest.mark.parametrize(
    "state",
    ['{"action":"set","value":11}', '{"action":"missing"}'],
    ids=["out-of-range", "unknown-action"],
)
def test_preview_rejects_invalid_json_variants(
    crud_unit, regular_user_token, database, cc, state
) -> None:
    service = unit_io_service(database, cc, regular_user_token)
    node = find_unit_io(service, crud_unit.uuid, "control")

    with pytest.raises(UnitIOError) as error:
        service.check_input_state(node.uuid, UnitIOSetState(state=state))

    assert error.value.error_code == ErrorCode.UNIT_IO_STATE_INVALID


def test_link_compatibility_can_depend_on_value(
    crud_unit, private_crud_unit, regular_user_token, database, cc
) -> None:
    service = unit_io_service(database, cc, regular_user_token)
    output = find_unit_io(service, crud_unit.uuid, "variable_counter")
    input_ = find_unit_io(service, private_crud_unit.uuid, "bounded_counter")

    result = service.check_link_compatibility(
        LinkCreate(output_uuid=output.uuid, input_uuid=input_.uuid)
    )

    assert result.compatible
    assert result.depends_on_value
    assert result.issue is None
    assert result.path == ()


@pytest.mark.parametrize(
    ("output", "issue"),
    [
        ("text_output", UnitIOCompatibilityIssue.TYPE),
        (COUNTER_OUTPUT, UnitIOCompatibilityIssue.RANGE),
    ],
    ids=["wrong-type", "disjoint-ranges"],
)
def test_link_compatibility_reports_incompatible_fields(
    crud_unit,
    private_crud_unit,
    regular_user_token,
    database,
    cc,
    output,
    issue,
) -> None:
    service = unit_io_service(database, cc, regular_user_token)
    output_node = find_unit_io(service, crud_unit.uuid, output)
    input_node = find_unit_io(
        service, private_crud_unit.uuid, "bounded_counter"
    )
    link = LinkCreate(output_uuid=output_node.uuid, input_uuid=input_node.uuid)

    result = service.check_link_compatibility(link)

    assert not result.compatible
    assert result.issue == issue
    with pytest.raises(UnitIOError) as error:
        service.create_link(link)
    assert error.value.error_code == ErrorCode.LINK_INCOMPATIBLE


def test_link_compatibility_requires_access_to_both_units(
    crud_unit,
    private_crud_unit,
    regular_user_token,
    extra_user_token,
    database,
    cc,
) -> None:
    owner = unit_io_service(database, cc, regular_user_token)
    output = find_unit_io(owner, crud_unit.uuid, "variable_counter")
    input_ = find_unit_io(owner, private_crud_unit.uuid, "bounded_counter")
    service = unit_io_service(database, cc, extra_user_token)

    with pytest.raises(NoAccessError):
        service.check_link_compatibility(
            LinkCreate(output_uuid=output.uuid, input_uuid=input_.uuid)
        )


def test_broadcast_converts_nested_temperature_for_each_link(
    mqtt_unit, private_mqtt_unit, regular_user_token, database, cc
) -> None:
    service = unit_io_service(database, cc, regular_user_token)
    sender, receiver = mqtt_unit, private_mqtt_unit
    output = sender.topics.output["temperature_celsius"]
    local_input = sender.topics.input["temperature_kelvin"]
    remote_input = receiver.topics.input["temperature_kelvin"]
    service.create_link(link_between(output, local_input))
    service.create_link(link_between(output, remote_input))
    messages = {}

    def delivered() -> bool:
        sender.publish(output, b'{"readings":[0,25.5]}')
        for probe in (sender, receiver):
            if message := probe.receive():
                messages[message.topic] = json.loads(message.payload)
        return len(messages) == 2

    with sender.connected(), receiver.connected():
        sender.subscribe(local_input)
        receiver.subscribe(remote_input)
        wait_until(
            delivered,
            interval=0.25,
            message="temperature did not reach both Inputs",
        )

    assert set(messages) == {local_input.topic, remote_input.topic}
    for value in messages.values():
        assert value["readings"] == pytest.approx([273.15, 298.65])


@pytest.mark.parametrize(
    ("payload", "source_error"),
    [(b"{", True), (b"11", False)],
    ids=["invalid-source-json", "destination-range"],
)
def test_routing_records_errors_for_source_and_destination(
    mqtt_unit,
    private_mqtt_unit,
    regular_user_token,
    database,
    cc,
    payload,
    source_error,
) -> None:
    service = unit_io_service(database, cc, regular_user_token)
    sender, receiver = mqtt_unit, private_mqtt_unit
    output = sender.topics.output["variable_counter"]
    input_ = receiver.topics.input["bounded_counter"]
    link = service.create_link(link_between(output, input_))
    rejected_node = link.output_uuid if source_error else link.input_uuid

    def rejected() -> bool:
        sender.publish(output, payload)
        return bool(service.get(rejected_node).last_message_error)

    with sender.connected(), receiver.connected():
        receiver.subscribe(input_)
        wait_until(
            rejected,
            interval=0.25,
            session=database,
            message="routing rejection was not recorded for the UnitIO",
        )
        assert receiver.receive() is None

        received = publish_and_receive(sender, output, b"3", receiver)

    assert received.topic == input_.topic
    assert received.payload == b"3"
    assert received.qos == MqttQos.AT_LEAST_ONCE.value


@pytest.mark.parametrize("local", [True, False], ids=["local", "remote"])
def test_selected_link_routes_only_to_addressed_input(
    mqtt_unit, private_mqtt_unit, selected_link_route, local
) -> None:
    sender, receiver = mqtt_unit, private_mqtt_unit
    output, local_input, remote_input = selected_link_route
    chosen, other = (sender, receiver) if local else (receiver, sender)
    target = local_input if local else remote_input
    payload = json.dumps(
        {"input_uuid": str(topic_uuid(target)), "data": 4}
    ).encode()

    with sender.connected(), receiver.connected():
        sender.subscribe(local_input)
        receiver.subscribe(remote_input)
        received = publish_and_receive(sender, output, payload, chosen)

        assert received.topic == target.topic
        assert received.payload == b"4"
        assert other.receive() is None


def test_selected_link_rejects_unlinked_input(
    mqtt_unit,
    private_mqtt_unit,
    selected_link_route,
    regular_user_token,
    database,
    cc,
) -> None:
    service = unit_io_service(database, cc, regular_user_token)
    sender, receiver = mqtt_unit, private_mqtt_unit
    output, local_input, remote_input = selected_link_route
    output_uuid = topic_uuid(output)
    payload = json.dumps(
        {"input_uuid": str(uuid_pkg.uuid4()), "data": 4}
    ).encode()

    def rejected() -> bool:
        sender.publish(output, payload)
        error = service.get(output_uuid).last_message_error
        return error is not None and "not linked" in error

    with sender.connected(), receiver.connected():
        sender.subscribe(local_input)
        receiver.subscribe(remote_input)
        wait_until(
            rejected,
            interval=0.25,
            session=database,
            message="Message Processor accepted an unlinked Input",
        )

        assert sender.receive() is None
        assert receiver.receive() is None


def test_raw_payload_survives_mqtt_forwarding(
    mqtt_unit, private_mqtt_unit, regular_user_token, database, cc
) -> None:
    service = unit_io_service(database, cc, regular_user_token)
    sender, receiver = mqtt_unit, private_mqtt_unit
    output = sender.topics.output["binary_output"]
    input_ = receiver.topics.input["binary_input"]
    service.create_link(link_between(output, input_))
    packet = b"\x00\xff\x80\r\n"

    with sender.connected(), receiver.connected():
        receiver.subscribe(input_)
        received = publish_and_receive(sender, output, packet, receiver)

    assert received.topic == input_.topic
    assert received.payload == packet
    assert received.qos == MqttQos.AT_LEAST_ONCE.value


@pytest.mark.datapipe
def test_datapipe_options_follow_io_contract(
    crud_unit, regular_user_token, database, cc
) -> None:
    service = unit_io_service(database, cc, regular_user_token)
    bounded = find_unit_io(service, crud_unit.uuid, "bounded_counter").uuid
    text = find_unit_io(service, crud_unit.uuid, "text_output").uuid

    numbers = service.get_data_pipe_config_options(bounded)
    strings = service.get_data_pipe_config_options(text)

    assert numbers.value_type == FieldType.INT
    assert numbers.numeric_filter_range is not None
    assert numbers.numeric_filter_range.min == 0
    assert numbers.numeric_filter_range.max == 10
    assert (
        ProcessingPolicyType.AGGREGATION
        in numbers.modes[TypeInputValue.NUMBER].policy_types
    )
    assert strings.value_type == FieldType.STRING
    assert strings.numeric_filter_range is None
    assert set(strings.modes) == {TypeInputValue.TEXT}


@pytest.mark.datapipe
async def test_datapipe_preview_checks_threshold_against_input_range(
    crud_unit, regular_user_token, database, cc, tmp_path
) -> None:
    service = unit_io_service(database, cc, regular_user_token)
    bounded = find_unit_io(service, crud_unit.uuid, "bounded_counter").uuid
    invalid = tmp_path / "out_of_range.yaml"
    invalid.write_text(
        Path(BOUNDED_PIPE)
        .read_text()
        .replace("threshold_max: 10", "threshold_max: 11")
    )

    errors = await service.check_data_pipe_config(
        await create_upload_file_from_path(str(invalid)), uuid=bounded
    )
    accepted = await service.check_data_pipe_config(
        await create_upload_file_from_path(BOUNDED_PIPE), uuid=bounded
    )

    assert ErrorCode.DATAPIPE_IO_RANGE_MISMATCH in {
        error.error_code for error in errors
    }
    assert accepted == []


@pytest.mark.datapipe
async def test_datapipe_rejects_numeric_config_for_text_output(
    crud_unit, regular_user_token, database, cc
) -> None:
    service = unit_io_service(database, cc, regular_user_token)
    text = find_unit_io(service, crud_unit.uuid, "text_output").uuid
    await activate_pipe(service, text, None)

    errors = await service.check_data_pipe_config(
        await create_upload_file_from_path(BOUNDED_PIPE), uuid=text
    )

    assert ErrorCode.DATAPIPE_IO_TYPE_MISMATCH in {
        error.error_code for error in errors
    }
    with pytest.raises(DataPipeError) as error:
        await service.set_data_pipe_config(
            text, await create_upload_file_from_path(BOUNDED_PIPE)
        )
    assert error.value.error_code == ErrorCode.DATAPIPE_IO_TYPE_MISMATCH


@pytest.mark.datapipe
async def test_mqtt_messages_feed_datapipe_and_resource_windows(
    mqtt_unit, regular_user_token, database, cc
) -> None:
    service = unit_io_service(database, cc, regular_user_token)
    output = mqtt_unit.topics.output[COUNTER_OUTPUT]
    uuid = topic_uuid(output)
    payload = b"123"
    await activate_pipe(
        service, uuid, f"{PIPE_CONFIGS}/data_pipe_n_records.yaml"
    )

    def recorded() -> bool:
        mqtt_unit.publish(output, payload)
        return any(
            window.total_datapipe_bytes_increment > 0
            for window in service.get_resource_windows(uuid)
        )

    with mqtt_unit.connected():
        wait_until(
            recorded,
            interval=0.5,
            timeout=20 + 2 * runtime.get().pu_datapipe_buffer_flush_interval,
            message="MQTT and Data Pipe resource counters did not reach ClickHouse",
        )

    windows = service.get_resource_windows(uuid)
    for window in windows:
        assert window.requests_per_second > 0
        assert window.bytes_per_second == pytest.approx(
            window.requests_per_second * len(payload)
        )
        assert (
            window.end_window_datetime - window.start_window_datetime
            == timedelta(hours=1)
        )


def test_link_changes_take_effect_without_restarting_mqtt(
    mqtt_unit, private_mqtt_unit, regular_user_token, database, cc
) -> None:
    service = unit_io_service(database, cc, regular_user_token)
    sender, receiver = mqtt_unit, private_mqtt_unit
    output = sender.topics.output["text_output"]
    removed = sender.topics.input["text_input"]
    remaining = receiver.topics.input["text_input"]
    local_link = service.create_link(link_between(output, removed))
    service.create_link(link_between(output, remaining))

    def reaches_both(text: str) -> bool:
        sender.publish(output, text.encode())
        payload = json.dumps(text).encode()
        local = sender.received_payload(removed, payload)
        remote = receiver.received_payload(remaining, payload)
        return local and remote

    def reaches_only_remaining() -> bool:
        text = uuid_pkg.uuid4().hex
        sender.publish(output, text.encode())
        payload = json.dumps(text).encode()
        remote = receiver.received_payload(remaining, payload)
        local = sender.received_payload(removed, payload)
        return remote and not local

    with sender.connected(), receiver.connected():
        sender.subscribe(removed)
        receiver.subscribe(remaining)
        wait_until(
            lambda: reaches_both("before-unlink"),
            interval=0.25,
            message="both linked Inputs should receive the broadcast",
        )

        service.delete_link(local_link.input_uuid, local_link.output_uuid)

        wait_until(
            reaches_only_remaining,
            interval=0.25,
            message="broadcast should only reach the remaining linked Input",
        )

        service.create_link(link_between(output, removed))

        wait_until(
            lambda: reaches_both("after-relink"),
            interval=0.25,
            message="both Inputs should receive again after the link is restored",
        )


@pytest.mark.datapipe
async def test_live_numeric_aggregation_uses_transformations(
    mqtt_unit, regular_user_token, database, cc
) -> None:
    service = unit_io_service(database, cc, regular_user_token)
    output = mqtt_unit.topics.output[COUNTER_OUTPUT]
    uuid = topic_uuid(output)
    await activate_pipe(
        service, uuid, f"{PIPE_CONFIGS}/data_pipe_mqtt_aggregation.yaml"
    )
    filters = DataPipeFilter.unlimited(
        uuid=uuid, type=ProcessingPolicyType.AGGREGATION, time_window_size=60
    )

    def aggregated() -> bool:
        mqtt_unit.publish(output, b"14")
        _, rows = service.get_data_pipe_data(filters)
        return any(
            row.aggregation_type == AggregationFunctions.AVG and row.state == 7
            for row in rows
        )

    with mqtt_unit.connected():
        wait_until(
            aggregated,
            interval=1,
            timeout=65 + 2 * runtime.get().pu_datapipe_buffer_flush_interval,
            message="transformed MQTT messages did not reach the aggregation window",
        )
