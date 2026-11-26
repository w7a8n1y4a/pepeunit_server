import json
import logging
import os
import shutil
import zlib
from dataclasses import dataclass, fields

import pytest
from pepeunit_client.file_manager import FileManager

from app import runtime
from app.domain.unit_model import Unit
from app.dto.enum import StaticRepoFileName, VisibilityLevel
from app.schemas.pydantic.unit import UnitCreate
from app.schemas.pydantic.unit_io import UnitIOTopicRead
from tests.client.mqtt import COUNTER_INPUT, COUNTER_OUTPUT, UNIT_ROOT
from tests.integration.helpers.mqtt import MQTTProbe, link_between
from tests.integration.helpers.names import entity_name, unique_name
from tests.integration.helpers.services import (
    branch_commits,
    find_unit_io,
    repo_read,
    unit_io_service,
    unit_service,
)
from tests.integration.helpers.wait import wait_until


def _create_manual_unit(
    database,
    cc,
    token,
    repo,
    name: str,
    visibility_level: VisibilityLevel,
    *,
    is_only_tag_update: bool = False,
):
    service = unit_service(database, cc, token)
    read, commits = branch_commits(database, token, repo.uuid)
    return service.create(
        UnitCreate(
            repo_uuid=repo.uuid,
            visibility_level=visibility_level,
            name=name,
            target_branch=read.branches[0],
            is_auto_update_unit=False,
            is_only_tag_update=is_only_tag_update,
            target_commit=commits[0].commit,
        )
    )


def _create_auto_unit(
    database,
    cc,
    token,
    repo,
    name: str,
    visibility_level: VisibilityLevel,
    *,
    is_only_tag_update: bool = False,
):
    read = repo_read(database, token, repo)
    return unit_service(database, cc, token).create(
        UnitCreate(
            repo_uuid=repo.uuid,
            visibility_level=visibility_level,
            name=name,
            target_branch=read.branches[0],
            is_auto_update_unit=True,
            is_only_tag_update=is_only_tag_update,
            is_compilable_unit=False,
        )
    )


@dataclass
class LiveUnits:
    universal_auto_unit: object
    universal_manual_unit: object
    last_value_unit: object
    n_records_unit: object
    time_window_unit: object
    chain_source_unit: object
    chain_middle_unit: object
    chain_sink_unit: object
    auxiliary_unit: object

    def all(self) -> list:
        return [getattr(self, item.name) for item in fields(self)]

    def firmware_manual(self) -> list:
        return [
            unit for unit in self.all() if unit is not self.universal_auto_unit
        ]

    def chain(self) -> list:
        return [
            self.chain_source_unit,
            self.chain_middle_unit,
            self.chain_sink_unit,
        ]

    def piped(self) -> list:
        return [
            self.universal_manual_unit,
            self.last_value_unit,
            self.n_records_unit,
            self.time_window_unit,
        ]


@pytest.fixture(scope="session")
def live_units(universal_repo, regular_user_token, database, cc) -> LiveUnits:
    token = regular_user_token
    service = unit_service(database, cc, token)
    repo = universal_repo

    units = LiveUnits(
        universal_auto_unit=_create_auto_unit(
            database,
            cc,
            token,
            repo,
            entity_name("u_auto"),
            VisibilityLevel.PRIVATE,
        ),
        universal_manual_unit=_create_manual_unit(
            database,
            cc,
            token,
            repo,
            entity_name("u_man"),
            VisibilityLevel.PUBLIC,
        ),
        last_value_unit=_create_manual_unit(
            database,
            cc,
            token,
            repo,
            entity_name("u_last"),
            VisibilityLevel.PUBLIC,
        ),
        n_records_unit=_create_manual_unit(
            database,
            cc,
            token,
            repo,
            entity_name("u_nrec"),
            VisibilityLevel.INTERNAL,
            is_only_tag_update=False,
        ),
        time_window_unit=_create_manual_unit(
            database,
            cc,
            token,
            repo,
            entity_name("u_twin"),
            VisibilityLevel.PRIVATE,
        ),
        chain_source_unit=_create_manual_unit(
            database,
            cc,
            token,
            repo,
            entity_name("u_csrc"),
            VisibilityLevel.PUBLIC,
        ),
        chain_middle_unit=_create_manual_unit(
            database,
            cc,
            token,
            repo,
            entity_name("u_cmid"),
            VisibilityLevel.INTERNAL,
            is_only_tag_update=False,
        ),
        chain_sink_unit=_create_manual_unit(
            database,
            cc,
            token,
            repo,
            entity_name("u_csnk"),
            VisibilityLevel.PRIVATE,
        ),
        auxiliary_unit=_create_manual_unit(
            database,
            cc,
            token,
            repo,
            entity_name("u_aux"),
            VisibilityLevel.PUBLIC,
        ),
    )

    for unit in units.all():
        current_env = service.get_env(unit.uuid)
        service.set_env(unit.uuid, json.dumps(current_env))

    return units


@pytest.fixture(scope="session")
def universal_auto_unit(live_units):
    return live_units.universal_auto_unit


@pytest.fixture(scope="session")
def universal_manual_unit(live_units):
    return live_units.universal_manual_unit


@pytest.fixture(scope="session")
def last_value_unit(live_units):
    return live_units.last_value_unit


@pytest.fixture(scope="session")
def n_records_unit(live_units):
    return live_units.n_records_unit


@pytest.fixture(scope="session")
def time_window_unit(live_units):
    return live_units.time_window_unit


@pytest.fixture(scope="session")
def chain_source_unit(live_units):
    return live_units.chain_source_unit


@pytest.fixture(scope="session")
def chain_middle_unit(live_units):
    return live_units.chain_middle_unit


@pytest.fixture(scope="session")
def chain_sink_unit(live_units):
    return live_units.chain_sink_unit


@pytest.fixture(scope="session")
def auxiliary_unit(live_units):
    return live_units.auxiliary_unit


@pytest.fixture(scope="session")
def unpacked_firmwares(
    live_units, regular_user_token, database, cc
) -> LiveUnits:
    service = unit_service(database, cc, regular_user_token)
    methods = [
        service.get_unit_firmware_zip,
        service.get_unit_firmware_tar,
        service.get_unit_firmware_tgz,
    ]
    leftovers = []

    for inc, unit in enumerate(live_units.all()):
        logging.info(unit.uuid)
        method_idx = inc % 3
        if method_idx == 2:
            tgz_path = methods[method_idx](unit.uuid, 9, 9)
            leftovers.append(tgz_path)
            with open(tgz_path, "rb") as handle:
                producer = zlib.decompressobj(wbits=9)
                tar_data = (
                    producer.decompress(handle.read()) + producer.flush()
                )
            archive_path = f"tmp/{unit.uuid}.tar"
            with open(archive_path, "wb") as tar_file:
                tar_file.write(tar_data)
        else:
            archive_path = methods[method_idx](unit.uuid)

        leftovers.append(archive_path)
        unpack_path = UNIT_ROOT / str(unit.uuid)
        shutil.unpack_archive(
            archive_path, unpack_path, "zip" if method_idx == 0 else "tar"
        )

        env = FileManager.read_env(str(unpack_path / StaticRepoFileName.ENV))
        assert len(env["PU_AUTH_TOKEN"]) > 100

    for path in leftovers:
        os.remove(path)

    return live_units


@pytest.fixture(scope="session")
def running_units(
    unpacked_firmwares, client_emulator, regular_user_token, database, cc
) -> LiveUnits:
    client_emulator.start_units(unpacked_firmwares.all())
    service = unit_service(database, cc, regular_user_token)

    def all_reported() -> bool:
        client_emulator.require_running()
        return all(
            service.get(unit.uuid).current_commit_version
            for unit in unpacked_firmwares.all()
        )

    wait_until(
        all_reported,
        timeout=runtime.get().pu_instance_unit_state_send_interval * 2 + 10,
        message="emulators did not report current_commit_version",
        session=database,
    )
    return unpacked_firmwares


@pytest.fixture(scope="session")
def chain_links(running_units, regular_user_token, database, cc):
    from app.schemas.pydantic.unit_io import LinkCreate
    from tests.integration.helpers.services import unit_io_service

    service = unit_io_service(database, cc, regular_user_token)
    io_units_list = [
        [
            find_unit_io(service, unit.uuid, COUNTER_INPUT),
            find_unit_io(service, unit.uuid, COUNTER_OUTPUT),
        ]
        for unit in running_units.chain()
    ]

    service.create_link(
        LinkCreate(
            output_uuid=io_units_list[0][1].uuid,
            input_uuid=io_units_list[1][0].uuid,
        )
    )
    service.create_link(
        LinkCreate(
            output_uuid=io_units_list[1][1].uuid,
            input_uuid=io_units_list[2][0].uuid,
        )
    )
    return io_units_list


def _create_crud_unit(database, cc, token, repo, visibility_level, name: str):
    return _create_manual_unit(
        database, cc, token, repo, name, visibility_level
    )


@pytest.fixture
def crud_unit(universal_repo, regular_user_token, database, cc):
    token = regular_user_token
    unit = _create_crud_unit(
        database,
        cc,
        token,
        universal_repo,
        VisibilityLevel.PUBLIC,
        unique_name("ucrud"),
    )
    yield unit
    try:
        unit_service(database, cc, token).delete(unit.uuid)
    except Exception:
        pass


@pytest.fixture
def private_crud_unit(universal_repo, regular_user_token, database, cc):
    token = regular_user_token
    unit = _create_crud_unit(
        database,
        cc,
        token,
        universal_repo,
        VisibilityLevel.PRIVATE,
        unique_name("upriv"),
    )
    yield unit
    try:
        unit_service(database, cc, token).delete(unit.uuid)
    except Exception:
        pass


@pytest.fixture
def compiled_unit(compiled_repo, regular_user_token, database, cc) -> Unit:
    service = unit_service(database, cc, regular_user_token)
    unit = service.create(
        UnitCreate(
            repo_uuid=compiled_repo.repo.uuid,
            name=unique_name("cfw"),
            visibility_level=VisibilityLevel.PRIVATE,
            target_branch="main",
            target_commit=compiled_repo.tagged_commit,
            target_firmware_platform="esp32",
            is_auto_update_unit=False,
            is_only_tag_update=False,
        )
    )
    try:
        yield unit
    finally:
        service.delete(unit.uuid)


@pytest.fixture
def mqtt_unit(crud_unit, regular_user_token, database, cc) -> MQTTProbe:
    service = unit_service(database, cc, regular_user_token)
    return MQTTProbe.from_unit(service, crud_unit.uuid)


@pytest.fixture
def private_mqtt_unit(
    private_crud_unit, regular_user_token, database, cc
) -> MQTTProbe:
    service = unit_service(database, cc, regular_user_token)
    return MQTTProbe.from_unit(service, private_crud_unit.uuid)


@pytest.fixture
def auxiliary_unit_token(
    auxiliary_unit, regular_user_token, database, cc
) -> str:
    service = unit_service(database, cc, regular_user_token)
    return service.generate_token(auxiliary_unit.uuid)


@pytest.fixture
def selected_link_route(
    mqtt_unit, private_mqtt_unit, regular_user_token, database, cc
) -> tuple[UnitIOTopicRead, UnitIOTopicRead, UnitIOTopicRead]:
    service = unit_io_service(database, cc, regular_user_token)
    output = mqtt_unit.topics.output["selected_counter"]
    local_input = mqtt_unit.topics.input["bounded_counter"]
    remote_input = private_mqtt_unit.topics.input["bounded_counter"]
    service.create_link(link_between(output, local_input))
    service.create_link(link_between(output, remote_input))
    return output, local_input, remote_input
