import logging
import uuid as uuid_pkg

import httpx

from app import runtime, settings
from app.dto.enum import BackendTopicCommand
from app.schemas.pydantic.shared import MqttRead
from app.schemas.pydantic.unit import UnitMqttTokenAuth, UnitUpdate
from app.schemas.pydantic.unit_io import UnitIOSetState
from app.schemas.pydantic.unit_manifest_validation import (
    UnitManifestValidationErrorRead,
)


def _headers(token: str) -> dict[str, str]:
    return {"accept": "application/json", "x-auth-token": token}


def api_request(
    method: str, path: str, token: str | None = None, **kwargs
) -> httpx.Response:
    return httpx.request(
        method,
        f"{settings.pu_link_prefix_and_v1}{path}",
        headers=_headers(token) if token is not None else None,
        timeout=runtime.get().http_timeout(),
        **kwargs,
    )


def _status(response: httpx.Response) -> int:
    if response.status_code >= 400:
        logging.error(
            f"{response.request.method} {response.request.url}"
            f" -> {response.status_code}: {response.text}"
        )

    return response.status_code


def patch_unit_commit(token: str, unit, target_version: str) -> int:
    url = f"{settings.pu_link_prefix_and_v1}/units/{unit.uuid}"
    response = httpx.patch(
        url=url,
        json=UnitUpdate(
            is_auto_update_unit=False, target_commit=target_version
        ).dict(),
        headers=_headers(token),
        timeout=runtime.get().http_timeout(),
    )
    return _status(response)


def post_bulk_update_units(token: str) -> int:
    url = f"{settings.pu_link_prefix_and_v1}/units/bulk_update"
    response = httpx.post(
        url=url, headers=_headers(token), timeout=runtime.get().http_timeout()
    )
    return _status(response)


def get_unit_logs_file(token: str, unit) -> httpx.Response:
    url = f"{settings.pu_link_prefix_and_v1}/units/logs/{unit.uuid}"
    return httpx.get(
        url=url, headers=_headers(token), timeout=runtime.get().http_timeout()
    )


def post_unit_command(token: str, unit, command: BackendTopicCommand) -> int:
    url = (
        f"{settings.pu_link_prefix_and_v1}/units/"
        f"send_command_to_input_base_topic/{unit.uuid}?command={command.value}"
    )
    response = httpx.post(
        url=url, headers=_headers(token), timeout=runtime.get().http_timeout()
    )
    return _status(response)


def post_schema_update(token: str, unit_uuid: uuid_pkg.UUID) -> int:
    url = (
        f"{settings.pu_link_prefix_and_v1}/units/"
        f"send_command_to_input_base_topic/{unit_uuid}?command=SchemaUpdate"
    )
    response = httpx.post(
        url=url, headers=_headers(token), timeout=runtime.get().http_timeout()
    )
    return _status(response)


def patch_input_state(
    token: str, unit_io_uuid: uuid_pkg.UUID, state: str
) -> int:
    url = f"{settings.pu_link_prefix_and_v1}/unit_ios/set_state_input/{unit_io_uuid}"
    response = httpx.patch(
        url=url,
        json=UnitIOSetState(state=state).dict(),
        headers=_headers(token),
        timeout=runtime.get().http_timeout(),
    )
    return _status(response)


def patch_update_local_repository(token: str, repo_uuid: uuid_pkg.UUID) -> int:
    url = (
        f"{settings.pu_link_prefix_and_v1}"
        f"/repo/update_local_repository/{repo_uuid}"
    )
    response = httpx.patch(
        url=url,
        headers=_headers(token),
        timeout=runtime.get().http_timeout(),
    )
    return _status(response)


def validate_manifest(
    token: str, name: str, content: bytes
) -> list[UnitManifestValidationErrorRead]:
    response = api_request(
        "POST",
        "/unit_manifests/validate",
        token,
        files={"file": (name, content)},
    )
    response.raise_for_status()
    return [
        UnitManifestValidationErrorRead.model_validate(error)
        for error in response.json()
    ]


def generate_manifest_readme(
    token: str | None, content: bytes
) -> httpx.Response:
    return api_request(
        "POST",
        "/unit_manifests/readme",
        token,
        files={"file": ("unit.yml", content)},
    )


def get_unit_env_file(token: str, unit_uuid: uuid_pkg.UUID) -> httpx.Response:
    response = api_request("GET", f"/units/env/{unit_uuid}", token)
    response.raise_for_status()
    return response


def preview_input_state(
    token: str, unit_io_uuid: uuid_pkg.UUID, state: str
) -> int:
    response = api_request(
        "POST",
        f"/unit_ios/check_input_state/{unit_io_uuid}",
        token,
        json=UnitIOSetState(state=state).model_dump(),
    )
    return _status(response)


def mqtt_authorization(
    client: httpx.Client, token: str, action: str, topic: str
) -> str:
    response = client.post(
        f"{settings.pu_link_prefix_and_v1}/units/auth",
        params={"action": action},
        json=UnitMqttTokenAuth(token=token, topic=topic).model_dump(),
    )
    response.raise_for_status()
    return MqttRead.model_validate_json(response.content).result
