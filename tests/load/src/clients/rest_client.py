import asyncio
import base64
import hashlib
import logging

import httpx
from cryptography.hazmat.primitives.asymmetric.mldsa import (
    MLDSA87PrivateKey,
)

from app import runtime
from app.dto.enum import GitPlatform, ProcessingPolicyType
from tests.load.src.dto.config import LoadTestConfig
from tests.load.src.loaders.mqtt import OUTPUT_TOPIC, MQTTLoadTest


class RestClient:
    def __init__(self, config: LoadTestConfig):
        self.config = config
        self.headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
        }

        private_key = MLDSA87PrivateKey.generate()
        raw_public_key = private_key.public_key().public_bytes_raw()
        public_key = base64.b64encode(raw_public_key).decode("ascii")
        hash_public_key = hashlib.sha256(raw_public_key).hexdigest()

        create_user_link = f"{self.config.url}/pepeunit/api/v1/users"
        response = httpx.post(
            create_user_link,
            json={"public_key": public_key},
            headers=self.headers,
            timeout=runtime.get().http_timeout(),
        )

        response.raise_for_status()
        self.user_uuid = response.json()["uuid"]

        challenge_link = (
            f"{self.config.url}/pepeunit/api/v1/users/auth/challenge"
        )
        response = httpx.post(
            challenge_link,
            json={"hash_public_key": hash_public_key},
            headers=self.headers,
            timeout=runtime.get().http_timeout(),
        )
        response.raise_for_status()
        challenge = response.json()["challenge"]
        signature = base64.b64encode(
            private_key.sign(challenge.encode("utf-8"))
        ).decode("ascii")

        auth_link = f"{self.config.url}/pepeunit/api/v1/users/auth"
        response = httpx.post(
            auth_link,
            json={"challenge": challenge, "signature": signature},
            headers=self.headers,
            timeout=runtime.get().http_timeout(),
        )
        response.raise_for_status()
        self.token = response.json()["token"]
        self.headers["x-auth-token"] = self.token

    def get_repo(self):
        target_repo_link = "https://git.pepemoss.com/pepe/pepeunit/units/universal_load_unit.git"

        repo_link = f"{self.config.url}/pepeunit/api/v1/repo?search_string={target_repo_link}"
        target_repo = httpx.get(
            repo_link,
            headers=self.headers,
            timeout=runtime.get().http_timeout(),
        )

        target_repo.raise_for_status()
        target_repo = target_repo.json()

        if target_repo["count"] == 0:
            repo_create_link = f"{self.config.url}/pepeunit/api/v1/repo"

            repo = {
                "platform": GitPlatform.GITLAB,
                "repository_url": target_repo_link,
                "is_public_repository": True,
            }

            response = httpx.post(
                repo_create_link,
                json=repo,
                headers=self.headers,
                timeout=runtime.get().http_timeout(),
            )
            response.raise_for_status()
            target_repo = response.json()
        else:
            target_repo = target_repo["repos"][0]

        return target_repo

    async def del_units(self):
        logging.warning("Run del old Units")
        units = self.get_units()

        if units:
            async with httpx.AsyncClient(
                timeout=runtime.get().http_timeout()
            ) as client:
                unit_uuids = [unit["uuid"] for unit in units]
                await self.run_tasks_with_semaphore(
                    client, unit_uuids, self.delete_unit
                )
                logging.warning(f"Deleted {len(unit_uuids)} Units")

    async def create_units(self, target_repo: dict):
        logging.warning("Run create new Units")

        unit_create_link = f"{self.config.url}/pepeunit/api/v1/units"
        responses = []

        async with httpx.AsyncClient(
            timeout=runtime.get().http_timeout()
        ) as client:
            units = [
                {
                    "repo_uuid": target_repo["uuid"],
                    "visibility_level": "Public",
                    "name": f"test_{i}_{self.config.test_hash}",
                    "target_branch": target_repo["branches"][0],
                    "is_auto_update_unit": True,
                    "is_only_tag_update": False,
                    "is_compilable_unit": False,
                }
                for i in range(self.config.unit_count)
            ]

            await self.run_tasks_with_semaphore(
                client, units, self.post_request, unit_create_link, responses
            )

        logging.warning(f"Created {len(responses)} Units")

        return responses

    def get_units(self):
        logging.warning("Fetch all Units")
        get_units = (
            f"{self.config.url}/pepeunit/api/v1/units"
            f"?is_include_output_unit_ios=true&search_string={self.config.test_hash}"
            f"&creator_uuid={self.user_uuid}"
        )

        limit = runtime.get().pu_instance_max_pagination_size

        units = []
        while True:
            response = httpx.get(
                f"{get_units}&offset={len(units)}&limit={limit}",
                headers=self.headers,
                timeout=runtime.get().http_timeout(),
            )
            response.raise_for_status()
            page = response.json()
            units.extend(page["units"])

            if len(units) >= page["count"] or not page["units"]:
                return units

    async def create_units_env(self, target_units: list[dict]):
        logging.warning("Run create env Units")

        async with httpx.AsyncClient(
            timeout=runtime.get().http_timeout()
        ) as client:
            await self.run_tasks_with_semaphore(
                client, target_units, self.patch_unit_env
            )

        logging.warning(f"Created {len(target_units)} env Units")

    async def set_data_pipe(self, target_units: list[dict]):
        logging.warning("Run set data pipe")

        target_unit_ios = [
            MQTTLoadTest.get_dict_by_topic(unit, OUTPUT_TOPIC)
            for unit in target_units
        ]

        async with httpx.AsyncClient(
            timeout=runtime.get().http_timeout()
        ) as client:
            await self.run_tasks_with_semaphore(
                client, target_unit_ios, self.patch_unit_data_pipe
            )

        logging.warning(f"Set {len(target_unit_ios)} data pipe")

    async def get_units_env(self, target_units: list[dict]):
        logging.warning("Fetch env Units")

        async with httpx.AsyncClient(
            timeout=runtime.get().http_timeout()
        ) as client:
            updated_units = await self.run_tasks_with_semaphore(
                client, target_units, self.get_unit_env
            )

        logging.warning(f"Fetched env for {len(target_units)} Units")
        return updated_units

    async def generation_units(self, target_repo: dict):
        await self.del_units()
        await self.create_units(target_repo)
        created_units = self.get_units()
        if len(created_units) != self.config.unit_count:
            raise RuntimeError(
                f"Expected {self.config.unit_count} load units, got {len(created_units)}"
            )
        for unit in created_units:
            MQTTLoadTest.get_dict_by_topic(unit, OUTPUT_TOPIC)
        await self.create_units_env(created_units)
        await self.set_data_pipe(created_units)
        return await self.get_units_env(created_units)

    async def run_tasks_with_semaphore(self, client, items, func, *args):
        semaphore = asyncio.Semaphore(10)
        tasks = [
            self.run_with_semaphore(semaphore, func, client, item, *args)
            for item in items
        ]
        return await asyncio.gather(*tasks)

    async def run_with_semaphore(self, semaphore, func, client, item, *args):
        async with semaphore:
            return await func(client, item, *args)

    async def delete_unit(self, client, unit_id):
        delete_url = f"{self.config.url}/pepeunit/api/v1/units/{unit_id}"
        response = await client.delete(delete_url, headers=self.headers)
        response.raise_for_status()

    async def post_request(self, client, unit, url, responses):
        response = await client.post(url, json=unit, headers=self.headers)
        response.raise_for_status()
        responses.append(response.json())

    async def patch_unit_env(self, client, unit):
        unit_env_link = (
            f"{self.config.url}/pepeunit/api/v1/units/env/{unit['uuid']}"
        )
        payload = {"env_json_string": '{"PU_MQTT_PING_INTERVAL": 30}'}
        response = await client.patch(
            unit_env_link, json=payload, headers=self.headers
        )
        response.raise_for_status()
        return response

    async def patch_unit_data_pipe(self, client, unit_io):
        unit_ios_update_link = (
            f"{self.config.url}/pepeunit/api/v1/unit_ios/{unit_io['uuid']}"
        )
        payload = {"is_data_pipe_active": True}

        response = await client.patch(
            unit_ios_update_link, json=payload, headers=self.headers
        )
        response.raise_for_status()

        set_data_pipe_config = f"{self.config.url}/pepeunit/api/v1/unit_ios/set_data_pipe_config?uuid={unit_io['uuid']}&is_bot_auth=false"

        data_pipe_policy = {
            ProcessingPolicyType.AGGREGATION.value: "tests/data/yaml/load/data_pipe_aggregation.yaml",
            ProcessingPolicyType.LAST_VALUE.value: "tests/data/yaml/load/data_pipe_last_value.yaml",
            ProcessingPolicyType.N_RECORDS.value: "tests/data/yaml/load/data_pipe_n_records.yaml",
            ProcessingPolicyType.TIME_WINDOW.value: "tests/data/yaml/load/data_pipe_time_window.yaml",
        }

        file_path = data_pipe_policy[self.config.policy_type]
        with open(file_path, "rb") as f:
            files = {"data": (file_path.split("/")[-1], f, "application/yaml")}
            header = {"x-auth-token": self.token}
            response = await client.post(
                set_data_pipe_config, files=files, headers=header
            )
            response.raise_for_status()
            return response

    async def get_unit_env(self, client, unit: dict):
        unit_env_link = (
            f"{self.config.url}/pepeunit/api/v1/units/env/{unit['uuid']}"
        )
        response = await client.get(unit_env_link, headers=self.headers)
        response.raise_for_status()
        unit["env"] = response.json()
        MQTTLoadTest.validate_unit(unit)
        return unit
