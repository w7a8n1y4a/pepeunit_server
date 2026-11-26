import io
import json
import zipfile
from pathlib import Path

import pytest
import yaml
from pepeunit_client.file_manager import FileManager

from app.configs.error_codes import ErrorCode
from app.configs.errors import (
    GitRepoError,
    NoAccessError,
    UnitError,
    ValidationError,
)
from app.dto.enum import (
    BackendTopicCommand,
    MqttQos,
    ReservedInputBaseTopic,
    StaticRepoFileName,
    UnitExecutionType,
    UnitSchemaWarningKind,
    VisibilityLevel,
)
from app.schemas.pydantic.repo import RepoFilter
from app.schemas.pydantic.unit import UnitCreate, UnitFilter, UnitUpdate
from tests.client.mqtt import COUNTER_INPUT, COUNTER_OUTPUT
from tests.integration.helpers.http import (
    api_request,
    generate_manifest_readme,
    get_unit_env_file,
    post_unit_command,
    validate_manifest,
)
from tests.integration.helpers.mqtt import MQTTProbe
from tests.integration.helpers.names import UNIVERSAL_FIRST_COMMIT, unique_name
from tests.integration.helpers.services import (
    branch_commits,
    find_unit_io,
    repo_service,
    unit_io_service,
    unit_service,
)

MANIFESTS = Path("tests/data/unit_manifest")
PREVIOUS_MANIFEST_COMMIT = "33f1f77f31a60d108611608deb8b51f448f66aa4"


@pytest.mark.parametrize(
    "name",
    ["unit.yml", "base_io.yml", "field_declarations.yml", "nested_fields.yml"],
)
def test_validate_manifest_upload(name, regular_user_token) -> None:
    content = (MANIFESTS / name).read_bytes()

    errors = validate_manifest(regular_user_token, name, content)

    assert errors == []


@pytest.mark.parametrize(
    ("section", "name", "declaration", "path"),
    [
        ("input", "not/a/topic", {"type": "int"}, ["input", "not/a/topic"]),
        ("env", "PU_DOMAIN", {"type": "string"}, ["env", "PU_DOMAIN"]),
        (
            "output",
            "temperature",
            {"type": "string", "unit": "Cel"},
            ["output", "temperature", "unit"],
        ),
    ],
    ids=["topic-name", "reserved-env", "measurement-unit"],
)
def test_validate_manifest_returns_field_path(
    regular_user_token, section, name, declaration, path
) -> None:
    manifest = yaml.safe_load((MANIFESTS / "unit.yml").read_bytes())
    manifest.setdefault(section, {})[name] = declaration

    errors = validate_manifest(
        regular_user_token, "unit.yml", yaml.safe_dump(manifest).encode()
    )

    assert any(error.path[: len(path)] == path for error in errors)
    assert all(error.error_code for error in errors)


@pytest.mark.parametrize(
    ("content", "code"),
    [
        (b"contract_version: [\n", ErrorCode.GIT_REPO_UNIT_MANIFEST_INVALID),
        (
            b"contract_version: v1\ncontract_version: v1\n",
            ErrorCode.GIT_REPO_UNIT_MANIFEST_INVALID,
        ),
        (b" " * (1024 * 1024 + 1), ErrorCode.GIT_REPO_UNIT_MANIFEST_TOO_LARGE),
    ],
    ids=["invalid-yaml", "duplicate-key", "size-limit"],
)
def test_validate_manifest_rejects_invalid_document(
    regular_user_token, content, code
) -> None:
    errors = validate_manifest(regular_user_token, "unit.yml", content)

    assert [error.error_code for error in errors] == [code]


def test_generate_manifest_readme(regular_user_token) -> None:
    response = generate_manifest_readme(
        regular_user_token, (MANIFESTS / "unit.yml").read_bytes()
    )

    assert response.status_code == 200, response.text
    assert response.headers["content-type"].startswith("text/markdown")
    assert response.text.startswith("# room-thermometer")
    for text in (
        "## Installation",
        "## Work algorithm",
        "## Environment",
        "## Output",
        "Cel",
    ):
        assert text in response.text


def test_manifest_readme_requires_user() -> None:
    response = generate_manifest_readme(
        None, (MANIFESTS / "unit.yml").read_bytes()
    )

    assert response.status_code == 403


def test_manifest_readme_rejects_unsupported_contract(
    regular_user_token,
) -> None:
    response = generate_manifest_readme(
        regular_user_token, b"contract_version: unsupported"
    )

    assert response.status_code == 422


def test_repository_search_exposes_front_manifest(
    universal_repo, regular_user_token, database
) -> None:
    service = repo_service(database, regular_user_token)

    repo = service.read(universal_repo.uuid)

    assert repo.is_have_front_unit_yml is True
    assert repo.front_unit_yml_search.name == "universal-test-unit"
    assert (
        repo.front_unit_yml_search.execution_type
        == UnitExecutionType.INTERPRETED
    )
    assert repo.front_unit_yml_search.platforms == ["linux"]


@pytest.mark.parametrize(
    "has_manifest", [True, False], ids=["with", "without"]
)
def test_repository_search_filters_by_front_manifest(
    universal_repo, regular_user_token, database, has_manifest
) -> None:
    service = repo_service(database, regular_user_token)
    filters = RepoFilter.unlimited(
        search_string=universal_repo.repository_url,
        is_have_front_unit_yml=has_manifest,
    )

    count, repos = service.list(filters)

    assert count == int(has_manifest)
    assert [repo.uuid for repo, _ in repos] == (
        [universal_repo.uuid] if has_manifest else []
    )


def test_unit_environment_uses_manifest_fields(
    crud_unit, regular_user_token, database, cc
) -> None:
    service = unit_service(database, cc, regular_user_token)
    original = service.get_env(crud_unit.uuid)
    assert original["TEST_PROFILE"] == {"label": "station", "limits": [0, 100]}
    assert "PUC_WIFI_PASS" not in original

    profile = {"label": "a\"'\\\nβ", "limits": [2, 5]}
    service.set_env(
        crud_unit.uuid,
        json.dumps(
            {
                "TEST_PROFILE": profile,
                "PU_DOMAIN": "ignored.invalid",
                "UNKNOWN": 5,
            }
        ),
    )

    saved = service.get_env(crud_unit.uuid)
    assert saved["TEST_PROFILE"] == profile
    assert saved["PU_DOMAIN"] == original["PU_DOMAIN"]
    assert "UNKNOWN" not in saved


def test_unit_environment_rejects_value_outside_manifest_range(
    crud_unit, regular_user_token, database, cc
) -> None:
    service = unit_service(database, cc, regular_user_token)
    profile = {"label": "station", "limits": [2, 5]}
    service.set_env(crud_unit.uuid, json.dumps({"TEST_PROFILE": profile}))

    with pytest.raises(UnitError) as error:
        service.set_env(
            crud_unit.uuid,
            json.dumps({"TEST_PROFILE": {"label": "bad", "limits": [101]}}),
        )

    assert error.value.error_code == ErrorCode.GIT_REPO_ENV_JSON_INVALID
    database.rollback()
    assert service.get_env(crud_unit.uuid)["TEST_PROFILE"] == profile


def test_unit_token_downloads_typed_environment(
    crud_unit, regular_user_token, database, cc
) -> None:
    service = unit_service(database, cc, regular_user_token)
    profile = {"label": "a\"'\\\nβ", "limits": [2, 5]}
    service.set_env(crud_unit.uuid, json.dumps({"TEST_PROFILE": profile}))
    env = service.get_env(crud_unit.uuid)

    response = get_unit_env_file(env["PU_AUTH_TOKEN"], crud_unit.uuid)

    assert response.headers["content-type"].startswith("text/plain")
    assert FileManager.parse_env(response.text) == env
    assert service.get_env_max_size(crud_unit.uuid).max_chars >= len(
        response.text
    )


def test_unit_token_downloads_io_topics_yaml(
    crud_unit, regular_user_token, database, cc
) -> None:
    service = unit_service(database, cc, regular_user_token)
    env = service.get_env(crud_unit.uuid)

    response = api_request(
        "GET", f"/units/io_topics/{crud_unit.uuid}", env["PU_AUTH_TOKEN"]
    )

    assert response.status_code == 200, response.text
    assert response.headers["content-type"].startswith("application/yaml")
    assert (
        StaticRepoFileName.IO_TOPICS.value
        in response.headers["content-disposition"]
    )
    topics = yaml.safe_load(response.content)
    assert topics["version"] == 1
    assert topics["input"][COUNTER_INPUT]["qos"] == MqttQos.AT_LEAST_ONCE.value
    assert "qos" not in topics["output"][COUNTER_OUTPUT]
    assert topics["input"][COUNTER_INPUT]["topic"].startswith(
        f"{env['PU_DOMAIN']}/"
    )
    assert {command.value for command in ReservedInputBaseTopic} <= topics[
        "input"
    ].keys()


def test_interpreted_firmware_includes_manifest_and_typed_environment(
    crud_unit, regular_user_token, database, cc
) -> None:
    service = unit_service(database, cc, regular_user_token)
    service.set_env(
        crud_unit.uuid,
        json.dumps({"TEST_PROFILE": {"label": "station β", "limits": [2, 5]}}),
    )
    token = service.generate_token(crud_unit.uuid)
    env_file = get_unit_env_file(token, crud_unit.uuid)

    response = api_request(
        "GET", f"/units/firmware/zip/{crud_unit.uuid}", token
    )

    assert response.status_code == 200, response.text
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        assert archive.read(StaticRepoFileName.ENV.value) == env_file.content
        assert StaticRepoFileName.UNIT_MANIFEST.value in archive.namelist()
        assert StaticRepoFileName.IO_TOPICS.value not in archive.namelist()


def test_manual_update_preview_and_io_reconciliation(
    crud_unit, regular_user_token, database, cc
) -> None:
    units = unit_service(database, cc, regular_user_token)
    unit_ios = unit_io_service(database, cc, regular_user_token)
    removed = find_unit_io(unit_ios, crud_unit.uuid, "binary_output")
    selected = units.get_selected_version(crud_unit.uuid)
    before = units.get_io_topics(crud_unit.uuid)
    update = UnitUpdate(target_commit=PREVIOUS_MANIFEST_COMMIT)

    preview = units.check_manual_update(crud_unit.uuid, update)

    assert preview.selected_commit == selected.commit
    assert preview.target_commit == PREVIOUS_MANIFEST_COMMIT
    warnings = {(warning.kind, warning.path) for warning in preview.warnings}
    assert (UnitSchemaWarningKind.REMOVED, ("env", "TEST_PROFILE")) in warnings
    assert (
        UnitSchemaWarningKind.REMOVED,
        ("output", "binary_output"),
    ) in warnings
    assert units.get_selected_version(crud_unit.uuid) == selected
    assert units.get_io_topics(crud_unit.uuid) == before

    units.update(crud_unit.uuid, update)

    assert (
        units.get_selected_version(crud_unit.uuid).commit
        == PREVIOUS_MANIFEST_COMMIT
    )
    after = units.get_io_topics(crud_unit.uuid)
    assert (
        after.input[COUNTER_INPUT].topic == before.input[COUNTER_INPUT].topic
    )
    assert after.output[COUNTER_OUTPUT] == before.output[COUNTER_OUTPUT]
    assert "binary_output" not in after.output
    with pytest.raises(ValidationError):
        unit_ios.get(removed.uuid)


def test_manual_update_preview_requires_unit_owner(
    crud_unit, extra_user_token, database, cc
) -> None:
    service = unit_service(database, cc, extra_user_token)
    update = UnitUpdate(target_commit=PREVIOUS_MANIFEST_COMMIT)

    with pytest.raises(NoAccessError):
        service.check_manual_update(crud_unit.uuid, update)


def test_manual_update_preview_rejects_auto_update(
    crud_unit, regular_user_token, database, cc
) -> None:
    service = unit_service(database, cc, regular_user_token)

    with pytest.raises(UnitError) as error:
        service.check_manual_update(
            crud_unit.uuid, UnitUpdate(is_auto_update_unit=True)
        )

    assert error.value.error_code == ErrorCode.UNIT_UPDATE_PREVIEW_MANUAL_ONLY


def test_unit_creation_uses_manifest_and_rejects_legacy_commit(
    universal_repo, regular_user_token, database, cc
) -> None:
    service = unit_service(database, cc, regular_user_token)
    repo, commits = branch_commits(
        database, regular_user_token, universal_repo.uuid
    )
    latest = commits[0].commit
    data = UnitCreate(
        repo_uuid=universal_repo.uuid,
        visibility_level=VisibilityLevel.PRIVATE,
        name=unique_name("mfest"),
        target_branch=repo.branches[0],
        target_commit=UNIVERSAL_FIRST_COMMIT,
        is_auto_update_unit=False,
        is_only_tag_update=False,
        is_compilable_unit=True,
    )

    with pytest.raises(GitRepoError) as error:
        service.create(data)

    assert error.value.error_code == ErrorCode.GIT_REPO_FILE_NOT_FOUND
    count, _ = service.list(UnitFilter(search_string=data.name))
    assert count == 0

    created = service.create(data.model_copy(update={"target_commit": latest}))
    try:
        assert created.target_commit == latest
        assert created.is_compilable_unit is False
        assert COUNTER_OUTPUT in service.get_io_topics(created.uuid).output
    finally:
        service.delete(created.uuid)


@pytest.mark.parametrize(
    ("tagged", "platform", "code"),
    [
        (False, "esp32", ErrorCode.GIT_REPO_COMMIT_WITHOUT_TAG),
        (True, "unknown", ErrorCode.GIT_REPO_PLATFORM_NOT_FOUND),
    ],
    ids=["untagged", "missing-asset"],
)
def test_compiled_unit_requires_tag_and_platform(
    compiled_repo, regular_user_token, database, cc, tagged, platform, code
) -> None:
    service = unit_service(database, cc, regular_user_token)
    commit = (
        compiled_repo.tagged_commit
        if tagged
        else compiled_repo.untagged_commit
    )
    data = UnitCreate(
        repo_uuid=compiled_repo.repo.uuid,
        target_branch="main",
        visibility_level=VisibilityLevel.PRIVATE,
        is_auto_update_unit=False,
        is_only_tag_update=False,
        name=unique_name("cfw"),
        target_commit=commit,
        target_firmware_platform=platform,
    )

    with pytest.raises(GitRepoError) as error:
        service.create(data)

    assert error.value.error_code == code


def test_compiled_firmware_requires_resolved_environment(
    compiled_unit, regular_user_token, database, cc
) -> None:
    service = unit_service(database, cc, regular_user_token)

    with pytest.raises(UnitError) as error:
        service.get_unit_firmware_zip(compiled_unit.uuid)

    assert error.value.error_code == ErrorCode.GIT_REPO_ENV_UNRESOLVED


def test_compiled_firmware_uses_manifest_tag_and_release(
    compiled_repo, compiled_unit, regular_user_token, database, cc
) -> None:
    service = unit_service(database, cc, regular_user_token)
    service.set_env(compiled_unit.uuid, '{"PROBE_PIN": 7}')

    archive_path = Path(service.get_unit_firmware_zip(compiled_unit.uuid))
    try:
        with zipfile.ZipFile(archive_path) as archive:
            env_file = StaticRepoFileName.ENV.value
            assert archive.namelist() == [env_file]
            env = FileManager.parse_env(archive.read(env_file).decode())
    finally:
        archive_path.unlink()

    assert compiled_unit.is_compilable_unit is True
    assert (
        service.get_selected_version(compiled_unit.uuid).tag
        == compiled_repo.tag
    )
    assert env["PROBE_PIN"] == 7
    assert env["PU_COMMIT_VERSION"] == compiled_repo.tagged_commit


def test_compiled_update_command_points_to_release_asset(
    compiled_repo, compiled_unit, regular_user_token, database, cc
) -> None:
    service = unit_service(database, cc, regular_user_token)
    service.set_env(compiled_unit.uuid, '{"PROBE_PIN": 7}')
    probe = MQTTProbe.from_unit(service, compiled_unit.uuid)
    topic = probe.topics.input[ReservedInputBaseTopic.UPDATE.value]

    with probe.connected():
        probe.subscribe(topic)
        status = post_unit_command(
            regular_user_token, compiled_unit, BackendTopicCommand.UPDATE
        )
        assert status == 204
        received = probe.receive(timeout=5)

    assert received is not None
    assert received.topic == topic.topic
    assert received.qos == MqttQos.AT_LEAST_ONCE.value
    assert json.loads(received.payload) == {
        "COMMAND": BackendTopicCommand.UPDATE.value,
        "PU_COMMIT_VERSION": compiled_repo.tagged_commit,
        "COMPILED_FIRMWARE_LINK": compiled_repo.asset_url,
    }
