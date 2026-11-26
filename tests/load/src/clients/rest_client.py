import asyncio
import base64
import hashlib
import logging
import time
import uuid
from pathlib import Path

import httpx
import yaml
from cryptography.hazmat.primitives.asymmetric.mldsa import MLDSA87PrivateKey

from app.dto.enum import (
    GitPlatform,
    ProcessingPolicyType,
    RepoStatus,
    VisibilityLevel,
)
from tests.load.src.dto.config import LoadTestConfig

HTTP_TIMEOUT = 15.0
SETUP_TIMEOUT = 180.0
PIPE_FILES = {
    ProcessingPolicyType.AGGREGATION: "data_pipe_aggregation.yaml",
    ProcessingPolicyType.LAST_VALUE: "data_pipe_last_value.yaml",
    ProcessingPolicyType.N_RECORDS: "data_pipe_n_records.yaml",
    ProcessingPolicyType.TIME_WINDOW: "data_pipe_time_window.yaml",
}
PIPE_DIRECTORY = Path(__file__).resolve().parents[3] / "data/yaml/load"


class RestClient:
    def __init__(self, config: LoadTestConfig):
        self.config = config
        self.client = httpx.AsyncClient(
            base_url=f"{config.api_url}/", timeout=HTTP_TIMEOUT
        )
        self.unit_ids: list[str] = []

    async def authenticate(self) -> None:
        private_key = MLDSA87PrivateKey.generate()
        raw_public_key = private_key.public_key().public_bytes_raw()
        public_key = base64.b64encode(raw_public_key).decode("ascii")
        await self.request("POST", "users", json={"public_key": public_key})

        raw = await self.request(
            "POST",
            "users/auth/challenge",
            json={
                "hash_public_key": hashlib.sha256(raw_public_key).hexdigest()
            },
        )
        challenge = raw.json()["challenge"]
        signature = base64.b64encode(
            private_key.sign(challenge.encode("utf-8"))
        ).decode("ascii")
        response = await self.request(
            "POST",
            "users/auth",
            json={"challenge": challenge, "signature": signature},
        )
        self.client.headers["x-auth-token"] = response.json()["token"]

    async def request(
        self, method: str, path: str, **kwargs
    ) -> httpx.Response:
        response = await self.client.request(method, path, **kwargs)
        if response.is_error:
            msg = f"{method} {path}: {response.status_code}: {response.text}"
            raise RuntimeError(msg)
        return response

    async def get_repo(self) -> dict:
        response = await self.request(
            "GET", "repo", params={"search_string": self.config.repo_url}
        )
        found = next(
            (
                repo
                for repo in response.json()["repos"]
                if repo["repository_url"] == self.config.repo_url
            ),
            None,
        )
        previous_sync = None
        if found is None:
            response = await self.request(
                "POST",
                "repo",
                json={
                    "platform": GitPlatform.GITLAB,
                    "repository_url": self.config.repo_url,
                    "is_public_repository": True,
                },
            )
            found = response.json()
        elif found["sync_status"] == RepoStatus.ERROR or (
            found["sync_status"] == RepoStatus.UPDATED
            and not found["branches"]
        ):
            previous_sync = found["sync_last_datetime"]
            await self.request(
                "PATCH", f"repo/update_local_repository/{found['uuid']}"
            )

        deadline = time.monotonic() + SETUP_TIMEOUT
        while True:
            response = await self.request("GET", f"repo/{found['uuid']}")
            repo = response.json()
            if repo["sync_status"] == RepoStatus.UPDATED and repo["branches"]:
                return repo
            if (
                repo["sync_status"] == RepoStatus.ERROR
                and repo["sync_last_datetime"] != previous_sync
            ):
                msg = f"Load unit repository sync failed: {repo['sync_error']}"
                raise RuntimeError(msg)
            if time.monotonic() >= deadline:
                msg = f"Load unit repository did not sync: {self.config.repo_url}"
                raise TimeoutError(msg)
            await asyncio.sleep(1)

    def pipe_config(self, policy: ProcessingPolicyType) -> bytes:
        path = PIPE_DIRECTORY / PIPE_FILES[policy]
        data = yaml.safe_load(path.read_bytes())
        filters = data["filters"]
        if filters["type_input_value"] != self.config.value_type:
            filters.pop("type_value_filtering", None)
            filters.pop("filtering_values", None)
            filters.pop("type_value_threshold", None)
            filters.pop("threshold_min", None)
            filters.pop("threshold_max", None)
            data.pop("transformations", None)
        filters["type_input_value"] = self.config.value_type.value
        filters["max_size"] = max(
            filters["max_size"], self.config.message_size
        )
        return yaml.safe_dump(data).encode()

    async def create_units(self, repo: dict) -> tuple[list[dict], dict]:
        run_id = uuid.uuid4().hex[:8]
        config = self.pipe_config(self.config.policy_type)
        semaphore = asyncio.Semaphore(10)
        first = await self.create_unit(
            repo, f"load_{run_id}_0", config, semaphore
        )
        results = await asyncio.gather(
            *(
                self.create_unit(
                    repo, f"load_{run_id}_{index}", config, semaphore
                )
                for index in range(1, self.config.unit_count)
            ),
            return_exceptions=True,
        )
        units = [first]
        for result in results:
            if isinstance(result, BaseException):
                raise result
            units.append(result)
        probe = await self.create_unit(
            repo,
            f"load_{run_id}_probe",
            self.pipe_config(ProcessingPolicyType.LAST_VALUE),
            semaphore,
        )
        return units, probe

    async def create_unit(
        self,
        repo: dict,
        name: str,
        config: bytes,
        semaphore: asyncio.Semaphore,
    ) -> dict:
        async with semaphore:
            response = await self.request(
                "POST",
                "units",
                json={
                    "repo_uuid": repo["uuid"],
                    "visibility_level": VisibilityLevel.PRIVATE,
                    "name": name,
                    "target_branch": repo["branches"][0],
                    "is_auto_update_unit": True,
                    "is_only_tag_update": False,
                },
            )
            unit_uuid = response.json()["uuid"]
            self.unit_ids.append(unit_uuid)
            await self.request(
                "PATCH",
                f"units/env/{unit_uuid}",
                json={"env_json_string": "{}"},
            )
            env = (await self.request("GET", f"units/env/{unit_uuid}")).json()
            output = (
                await self.request("GET", f"units/io_topics/{unit_uuid}")
            ).text
            topics = yaml.safe_load(output)["output"]
            if self.config.output_name not in topics:
                msg = f"{self.config.repo_url} has no {self.config.output_name!r} output"
                raise RuntimeError(msg)
            topic = topics[self.config.output_name]
            io_uuid = topic["topic"].split("/", 1)[1]
            await self.request(
                "PATCH",
                f"unit_ios/{io_uuid}",
                json={"is_data_pipe_active": True},
            )
            await self.request(
                "POST",
                "unit_ios/set_data_pipe_config",
                params={"uuid": io_uuid},
                files={"data": ("load.yaml", config, "application/yaml")},
            )
            return {
                "uuid": unit_uuid,
                "env": env,
                "output": topic,
                "output_uuid": io_uuid,
            }

    async def output_state(self, unit: dict) -> str | None:
        response = await self.request("GET", f"unit_ios/{unit['output_uuid']}")
        return response.json()["state"]

    async def has_processed(self, unit: dict) -> bool:
        response = await self.request(
            "GET", f"unit_ios/{unit['output_uuid']}/resources"
        )
        return any(
            window["requests_per_second"] > 0 for window in response.json()
        )

    async def check_processing(self, unit: dict) -> None:
        deadline = time.monotonic() + 60
        while not await self.has_processed(unit):
            if time.monotonic() >= deadline:
                msg = f"Message Processor did not record messages from {unit['output']['topic']}"
                raise TimeoutError(msg)
            await asyncio.sleep(1)

    async def close(self) -> None:
        try:
            for unit_uuid in self.unit_ids:
                try:
                    await self.request("DELETE", f"units/{unit_uuid}")
                except RuntimeError, httpx.HTTPError:
                    logging.exception(
                        "Could not remove load unit %s", unit_uuid
                    )
        finally:
            await self.client.aclose()
