import json
import os
import time
import uuid as uuid_pkg

import pytest

from app import runtime
from app.configs.errors import DataPipeError, UnitIOError, ValidationError
from app.dto.enum import (
    DataPipeStage,
    ProcessingPolicyType,
    UnitIOTypeEnum,
    VisibilityLevel,
)
from app.schemas.pydantic.unit_io import (
    DataPipeFilter,
    LinkCreate,
    UnitIOFilter,
    UnitIOUpdate,
)
from app.utils.utils import create_upload_file_from_path
from tests.client.mqtt import COUNTER_INPUT, COUNTER_OUTPUT, UNIT_ROOT
from tests.integration.helpers.data_pipe import upload_pipe_csv
from tests.integration.helpers.http import patch_input_state
from tests.integration.helpers.services import (
    find_unit_io,
    unit_io_service,
    unit_service,
)
from tests.integration.helpers.wait import wait_until


async def test_update_unit_io(
    chain_sink_unit, regular_user_token, database, cc
) -> None:
    service = unit_io_service(database, cc, regular_user_token)
    input_node = find_unit_io(service, chain_sink_unit.uuid, COUNTER_INPUT)

    assert input_node.max_connections == 10

    update_unit_io = await service.update(
        input_node.uuid,
        UnitIOUpdate(visibility_level=VisibilityLevel.PRIVATE),
    )
    assert update_unit_io.visibility_level == VisibilityLevel.PRIVATE

    update_unit_io = await service.update(
        input_node.uuid, UnitIOUpdate(is_rewritable_input=True)
    )
    assert update_unit_io.is_rewritable_input

    update_unit_io = await service.update(
        input_node.uuid, UnitIOUpdate(max_connections=5)
    )
    assert update_unit_io.max_connections == 5

    output_node = find_unit_io(service, chain_sink_unit.uuid, COUNTER_OUTPUT)
    update_unit_io = await service.update(
        output_node.uuid, UnitIOUpdate(max_connections=3)
    )
    assert update_unit_io.max_connections == 3

    with pytest.raises(UnitIOError):
        await service.update(
            output_node.uuid, UnitIOUpdate(is_rewritable_input=True)
        )

    with pytest.raises(UnitIOError):
        await service.update(input_node.uuid, UnitIOUpdate(max_connections=0))


@pytest.mark.datapipe
async def test_set_data_pipe(
    piped_units, regular_user_token, database, cc
) -> None:
    service = unit_io_service(database, cc, regular_user_token)
    output_node = find_unit_io(
        service, piped_units.universal_manual_unit.uuid, COUNTER_OUTPUT
    )
    assert output_node.is_data_pipe_active

    bad_yml = "tests/data/yaml/integra/data_pipe_bad.yaml"
    data = await service.check_data_pipe_config(
        await create_upload_file_from_path(bad_yml)
    )
    assert len(data) == 2

    data = await service.check_data_pipe_config(
        await create_upload_file_from_path(
            "tests/data/yaml/integra/data_pipe_aggregation.yaml"
        )
    )
    assert len(data) == 0


@pytest.mark.datapipe
async def test_check_data_pipe_alerts_config(
    regular_user_token, database, cc
) -> None:
    service = unit_io_service(database, cc, regular_user_token)

    for valid_yml in (
        "tests/data/yaml/integra/data_pipe_alerts.yaml",
        "tests/data/yaml/integra/data_pipe_alerts_text.yaml",
        "tests/data/yaml/integra/data_pipe_alerts_any.yaml",
    ):
        data = await service.check_data_pipe_config(
            await create_upload_file_from_path(valid_yml)
        )
        assert len(data) == 0

    data = await service.check_data_pipe_config(
        await create_upload_file_from_path(
            "tests/data/yaml/integra/data_pipe_alerts_bad.yaml"
        )
    )
    assert len(data) == 2
    assert all(error.stage == DataPipeStage.ALERTS for error in data)

    # filtering values do not fit filters.type_input_value
    data = await service.check_data_pipe_config(
        await create_upload_file_from_path(
            "tests/data/yaml/integra/data_pipe_alerts_mismatch.yaml"
        )
    )
    assert len(data) == 1
    assert data[0].stage == DataPipeStage.ALERTS


@pytest.mark.datapipe
async def test_get_data_pipe_config(
    piped_units,
    universal_manual_unit,
    auxiliary_unit,
    regular_user_token,
    database,
    cc,
) -> None:
    service = unit_io_service(database, cc, regular_user_token)

    output_node = find_unit_io(
        service, universal_manual_unit.uuid, COUNTER_OUTPUT
    )
    config_path = service.get_data_pipe_config(output_node.uuid)
    assert len(config_path) > 0
    os.remove(config_path)

    with pytest.raises(DataPipeError):
        output_node = find_unit_io(
            service, auxiliary_unit.uuid, COUNTER_OUTPUT
        )
        target_unit_io = output_node
        target_unit_io.is_data_pipe_active = True
        target_unit_io.data_pipe_yml = None
        service.unit_io_repository.update(target_unit_io.uuid, target_unit_io)
        service.get_data_pipe_config(target_unit_io.uuid)

    with pytest.raises(DataPipeError):
        output_node = find_unit_io(
            service, auxiliary_unit.uuid, COUNTER_OUTPUT
        )
        target_unit_io = output_node
        target_unit_io.is_data_pipe_active = False
        service.unit_io_repository.update(target_unit_io.uuid, target_unit_io)
        service.get_data_pipe_config(output_node.uuid)


def test_create_link(running_units, chain_links, regular_user_token) -> None:
    token = regular_user_token
    target_units = running_units.chain()
    source_input_uuid = chain_links[0][0].uuid
    source_output_uuid = chain_links[0][1].uuid

    state = "0"
    sent_at = time.time()
    wait_until(
        lambda: patch_input_state(token, source_input_uuid, state) < 400,
        timeout=20,
        message="chain source input rejected set_state",
    )
    assert patch_input_state(token, source_output_uuid, state) >= 400

    def chain_arrived() -> bool:
        ready = True
        for unit, (input_io, _) in zip(target_units, chain_links, strict=True):
            filepath = UNIT_ROOT / str(unit.uuid) / "log_state.json"
            if not os.path.exists(filepath):
                ready = False
                continue
            with open(filepath) as handle:
                received = json.load(handle)
                if received["value"] != 0 or received["timestamp"] < sent_at:
                    ready = False
                assert received["input_topic"].endswith(f"/{input_io.uuid}")
        if not ready:
            patch_input_state(token, source_input_uuid, state)
        return ready

    wait_until(
        chain_arrived,
        timeout=30,
        interval=2,
        message="MQTT chain did not write log_state.json",
    )


def test_duplicate_link(
    running_units, chain_links, regular_user_token, database, cc
) -> None:
    service = unit_io_service(database, cc, regular_user_token)
    with pytest.raises(UnitIOError):
        service.create_link(
            LinkCreate(
                output_uuid=chain_links[0][1].uuid,
                input_uuid=chain_links[1][0].uuid,
            )
        )


async def test_set_state_input_unit_io(
    running_units, auxiliary_unit, regular_user_token, database, cc
) -> None:
    service = unit_io_service(database, cc, regular_user_token)
    unit_svc = unit_service(database, cc, regular_user_token)
    unit_token = unit_svc.generate_token(auxiliary_unit.uuid)

    input_node = find_unit_io(service, auxiliary_unit.uuid, COUNTER_INPUT)
    io_uuid = input_node.uuid
    state = "1"
    assert patch_input_state(unit_token, io_uuid, state) >= 400

    await service.update(io_uuid, UnitIOUpdate(is_rewritable_input=True))
    wait_until(
        lambda: patch_input_state(unit_token, io_uuid, state) < 400,
        timeout=15,
        message="unit token still cannot set input after is_rewritable_input=True",
    )


def test_get_link(
    running_units,
    chain_links,
    chain_middle_unit,
    regular_user_token,
    database,
    cc,
) -> None:
    service = unit_io_service(database, cc, regular_user_token)
    count, target_links = service.get_links(chain_middle_unit.uuid)
    assert len(target_links) == 2


async def test_max_connections_blocks_link(
    running_units,
    last_value_unit,
    n_records_unit,
    time_window_unit,
    regular_user_token,
    database,
    cc,
) -> None:
    service = unit_io_service(database, cc, regular_user_token)

    def _io(unit):
        inputs = find_unit_io(service, unit.uuid, COUNTER_INPUT)
        outputs = find_unit_io(service, unit.uuid, COUNTER_OUTPUT)
        return inputs, outputs

    sink_input, _ = _io(n_records_unit)
    _, source_a = _io(last_value_unit)
    _, source_b = _io(time_window_unit)
    original_max = sink_input.max_connections
    created = None
    try:
        await service.update(sink_input.uuid, UnitIOUpdate(max_connections=1))
        created = service.create_link(
            LinkCreate(
                output_uuid=source_a.uuid,
                input_uuid=sink_input.uuid,
            )
        )
        with pytest.raises(UnitIOError):
            service.create_link(
                LinkCreate(
                    output_uuid=source_b.uuid,
                    input_uuid=sink_input.uuid,
                )
            )
    finally:
        if created:
            service.delete_link(sink_input.uuid, source_a.uuid)
        await service.update(
            sink_input.uuid, UnitIOUpdate(max_connections=original_max)
        )


def test_delete_link(
    running_units,
    chain_links,
    chain_middle_unit,
    regular_user_token,
    database,
    cc,
) -> None:
    service = unit_io_service(database, cc, regular_user_token)
    count, target_links = service.get_links(chain_middle_unit.uuid)
    target_link = target_links[0]
    service.delete_link(target_link.input_uuid, target_link.output_uuid)

    with pytest.raises(ValidationError):
        service.delete_link(uuid_pkg.uuid4(), uuid_pkg.uuid4())


def test_get_many_unit_io(
    live_units, regular_user_token, database, cc
) -> None:
    service = unit_io_service(database, cc, regular_user_token)
    count, units_io = service.list(
        UnitIOFilter(
            search_string="input",
            type=[UnitIOTypeEnum.INPUT],
            offset=0,
            limit=runtime.get().pu_instance_max_pagination_size,
        )
    )
    assert count >= 8
    assert len(units_io) == min(
        count, runtime.get().pu_instance_max_pagination_size
    )


@pytest.mark.datapipe
async def test_data_pipe_can_activate_an_unsuffixed_output(
    crud_unit, regular_user_token, database, cc
) -> None:
    service = unit_io_service(database, cc, regular_user_token)
    output = find_unit_io(service, crud_unit.uuid, COUNTER_OUTPUT)
    updated = await service.update(
        output.uuid, UnitIOUpdate(is_data_pipe_active=True)
    )

    assert updated.is_data_pipe_active is True
    assert updated.topic_name == COUNTER_OUTPUT


def test_delete_unit(crud_unit, regular_user_token, database, cc) -> None:
    service = unit_service(database, cc, regular_user_token)
    service.delete(crud_unit.uuid)
    with pytest.raises(ValidationError):
        service.get(crud_unit.uuid)


@pytest.mark.datapipe
@pytest.mark.last_value
async def test_get_data_pipe_last_value(
    piped_units, regular_user_token, database, cc
) -> None:
    service = unit_io_service(database, cc, regular_user_token)
    last_output = find_unit_io(
        service, piped_units.last_value_unit.uuid, COUNTER_OUTPUT
    )
    wait_until(
        lambda: service.get(uuid=last_output.uuid).state is not None,
        timeout=20,
        message="last_value pipe did not write unit_io.state",
        session=database,
    )


@pytest.mark.datapipe
async def test_get_data_pipe_data(
    piped_units, regular_user_token, database, cc
) -> None:
    service = unit_io_service(database, cc, regular_user_token)

    output_n = find_unit_io(
        service, piped_units.n_records_unit.uuid, COUNTER_OUTPUT
    )
    wait_until(
        lambda: (
            service.get_data_pipe_data(
                DataPipeFilter(
                    uuid=output_n.uuid, type=ProcessingPolicyType.N_RECORDS
                )
            )[0]
            > 0
        ),
        timeout=20,
        message="n_records pipe did not write ClickHouse rows",
    )

    output_tw = find_unit_io(
        service, piped_units.time_window_unit.uuid, COUNTER_OUTPUT
    )
    wait_until(
        lambda: (
            service.get_data_pipe_data(
                DataPipeFilter(
                    uuid=output_tw.uuid, type=ProcessingPolicyType.TIME_WINDOW
                )
            )[0]
            > 0
        ),
        timeout=20,
        message="time_window pipe did not write ClickHouse rows",
    )


@pytest.mark.datapipe
async def test_get_data_pipe_data_csv(
    piped_units, n_records_unit, regular_user_token, database, cc
) -> None:
    service = unit_io_service(database, cc, regular_user_token)
    output_node = find_unit_io(service, n_records_unit.uuid, COUNTER_OUTPUT)
    file_path = service.get_data_pipe_data_csv(output_node.uuid)
    os.remove(file_path)

    last_output = find_unit_io(
        service, piped_units.last_value_unit.uuid, COUNTER_OUTPUT
    )
    with pytest.raises(DataPipeError):
        service.get_data_pipe_data_csv(last_output.uuid)


@pytest.mark.datapipe
async def test_delete_data_pipe_data(
    piped_units, regular_user_token, database, cc
) -> None:
    service = unit_io_service(database, cc, regular_user_token)
    clickhouse_units = [
        (piped_units.universal_manual_unit, ProcessingPolicyType.AGGREGATION),
        (piped_units.n_records_unit, ProcessingPolicyType.N_RECORDS),
        (piped_units.time_window_unit, ProcessingPolicyType.TIME_WINDOW),
    ]
    for target_unit, policy in clickhouse_units:
        # emulators publish to the counter output only, so an input node gets no
        # rows while ClickHouse applies the delete mutation
        input_node = find_unit_io(service, target_unit.uuid, COUNTER_INPUT)
        io_uuid = input_node.uuid

        await upload_pipe_csv(service, io_uuid, policy)
        filled_count, _ = service.get_data_pipe_data(
            DataPipeFilter(uuid=io_uuid, type=policy)
        )
        assert filled_count > 0

        service.delete_data_pipe_data(io_uuid)
        wait_until(
            lambda io_uuid=io_uuid, policy=policy: (
                service.get_data_pipe_data(
                    DataPipeFilter(uuid=io_uuid, type=policy)
                )[0]
                == 0
            ),
            timeout=15,
            interval=0.5,
            message=f"{policy} rows were not deleted from ClickHouse",
        )

    last_output = find_unit_io(
        service, piped_units.last_value_unit.uuid, COUNTER_OUTPUT
    )
    service.delete_data_pipe_data(last_output.uuid)
    count, _ = service.get_data_pipe_data(
        DataPipeFilter(
            uuid=last_output.uuid, type=ProcessingPolicyType.LAST_VALUE
        )
    )
    assert count == 1
