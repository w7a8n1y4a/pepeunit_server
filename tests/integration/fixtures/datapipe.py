import asyncio

import pytest

from tests.client.mqtt import COUNTER_INPUT, COUNTER_OUTPUT
from tests.integration.helpers.data_pipe import activate_pipe
from tests.integration.helpers.services import find_unit_io, unit_io_service

PIPE_YAMLS = {
    "universal_manual_unit": "tests/data/yaml/integra/data_pipe_aggregation.yaml",
    "last_value_unit": "tests/data/yaml/integra/data_pipe_last_value.yaml",
    "n_records_unit": "tests/data/yaml/integra/data_pipe_n_records.yaml",
    "time_window_unit": "tests/data/yaml/integra/data_pipe_time_window.yaml",
}


async def _activate_and_set_pipe(service, unit, yaml_path: str | None) -> None:
    for topic in (COUNTER_OUTPUT, COUNTER_INPUT):
        node = find_unit_io(service, unit.uuid, topic)
        await activate_pipe(service, node.uuid, yaml_path)


@pytest.fixture(scope="session")
def piped_units(running_units, regular_user_token, database, cc):
    service = unit_io_service(database, cc, regular_user_token)

    async def _setup() -> None:
        mapping = [
            (
                running_units.universal_manual_unit,
                PIPE_YAMLS["universal_manual_unit"],
            ),
            (running_units.last_value_unit, PIPE_YAMLS["last_value_unit"]),
            (running_units.n_records_unit, PIPE_YAMLS["n_records_unit"]),
            (running_units.time_window_unit, PIPE_YAMLS["time_window_unit"]),
        ]
        for unit, yaml_path in mapping:
            await _activate_and_set_pipe(service, unit, yaml_path)

    asyncio.run(_setup())
    return running_units
