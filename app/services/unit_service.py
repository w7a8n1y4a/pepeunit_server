import base64
import builtins
import copy
import datetime
import itertools
import json
import logging
import os
import shutil
import uuid as uuid_pkg
import zlib

from fastapi import Depends
from pydantic import JsonValue, ValidationError
from sqlalchemy.exc import IntegrityError

from app import runtime, settings
from app.configs.error_codes import ErrorCode
from app.configs.errors import (
    GitRepoError,
    MqttError,
    NoAccessError,
    UnitError,
    UnitIOError,
)
from app.configs.redis import notify_message_processor
from app.domain.operation_task_model import OperationTask
from app.domain.repo_model import Repo
from app.domain.unit_io_model import UnitIO
from app.domain.unit_model import Unit
from app.dto.agent.abc import AgentBackend, AgentUnit
from app.dto.clickhouse.log import UnitLog
from app.dto.enum import (
    AgentType,
    BackendTopicCommand,
    DestinationTopicType,
    FieldKey,
    GlobalPrefixTopic,
    JsonPayload,
    LogLevel,
    MqttQos,
    NotFoundEntity,
    OperationTaskType,
    OrderByDate,
    OwnershipType,
    PermissionEntities,
    ReservedEnvVariableName,
    StaticRepoFileName,
    UnitIOTypeEnum,
    UserRole,
    UuidEntity,
)
from app.repositories.git_repo_repository import GitRepoRepository
from app.repositories.icon_repository import IconRepository
from app.repositories.repo_repository import (
    RepoRepository,
)
from app.repositories.unit_io_repository import UnitIORepository
from app.repositories.unit_log_repository import UnitLogRepository
from app.repositories.unit_repository import UnitRepository
from app.schemas.mqtt.utils import get_topic_split
from app.schemas.pydantic.operation_task import OperationTaskCreate
from app.schemas.pydantic.shared import UnitIORead
from app.schemas.pydantic.unit import (
    SelectedVersionRead,
    UnitCreate,
    UnitEnvSizeRead,
    UnitFilter,
    UnitLogFilter,
    UnitRead,
    UnitUpdate,
    UnitUpdatePreviewRead,
)
from app.schemas.pydantic.unit_io import (
    UnitIOFilter,
    UnitIOTopicRead,
    UnitIOTopicsRead,
)
from app.schemas.pydantic.unit_manifest import (
    FieldDeclarations,
    ManifestDictionary,
    UnitDefinition,
)
from app.services.access_service import AccessService
from app.services.background import BackgroundService
from app.services.icon_image import IconImage
from app.services.operation_task_service import OperationTaskService
from app.services.permission_service import PermissionService
from app.services.unit_env_serializer import UnitEnvSerializer
from app.services.unit_env_size import UnitEnvSize
from app.services.unit_io_service import UnitIOService
from app.services.unit_manifest_diff import UnitManifestDiff
from app.services.unit_manifest_service import UnitManifestService
from app.services.utils import get_topic_name
from app.services.validators import (
    is_valid_json,
    is_valid_object,
    is_valid_uuid,
)
from app.utils.utils import aes_gcm_decode, aes_gcm_encode


class UnitService:
    def __init__(
        self,
        unit_repository: UnitRepository = Depends(),
        icon_repository: IconRepository = Depends(),
        repo_repository: RepoRepository = Depends(),
        unit_io_repository: UnitIORepository = Depends(),
        unit_log_repository: UnitLogRepository = Depends(),
        access_service: AccessService = Depends(),
        permission_service: PermissionService = Depends(),
        operation_task_service: OperationTaskService = Depends(),
        unit_io_service: UnitIOService = Depends(),
        unit_manifest_service: UnitManifestService = Depends(),
    ) -> None:
        self.unit_repository = unit_repository
        self.icon_repository = icon_repository
        self.repo_repository = repo_repository
        self.git_repo_repository = GitRepoRepository()
        self.unit_io_repository = unit_io_repository
        self.unit_log_repository = unit_log_repository
        self.access_service = access_service
        self.permission_service = permission_service
        self.operation_task_service = operation_task_service
        self.unit_io_service = unit_io_service
        self.unit_manifest_service = unit_manifest_service

    def create(self, data: UnitCreate) -> Unit:
        self.access_service.authorization.check_access([AgentType.USER])
        self.unit_repository.is_valid_name(data.name)

        repo = self.repo_repository.get(Repo(uuid=data.repo_uuid))
        is_valid_object(repo, NotFoundEntity.REPO)
        self.access_service.authorization.check_repo_access(repo)

        unit = Unit(
            creator_uuid=self.access_service.current_agent.uuid,
            **data.model_dump(exclude={"is_compilable_unit"}),
        )
        _, _, manifest, _ = self.select_unit_manifest(repo, unit)

        try:
            unit.create_datetime = datetime.datetime.now(datetime.UTC)
            unit.last_update_datetime = unit.create_datetime
            unit.icon_uuid = self.icon_repository.stage(
                IconImage.generate(str(unit.uuid))
            ).uuid
            self.unit_repository.stage(unit)
            self.permission_service.stage_new_unit_permissions(unit)
            result = self.unit_io_service.synchronize(unit, manifest)
            snapshot = copy.deepcopy(result)
            self.unit_repository.db.commit()
        except Exception as error:
            self.unit_repository.db.rollback()
            if isinstance(error, IntegrityError):
                self.unit_repository.raise_name_conflict(error)
            raise
        notify_message_processor()
        return snapshot

    def select_unit_manifest(
        self, repo: Repo, unit: Unit
    ) -> tuple[str, str | None, UnitDefinition, ManifestDictionary]:
        unit.is_compilable_unit = False
        self.is_valid_update_rule(unit)
        selected_commit, selected_tag = (
            self.git_repo_repository.get_selected_commit_and_tag(repo, unit)
        )
        manifest, dictionary = self.unit_manifest_service.read_from_commit(
            repo, selected_commit
        )
        if unit.is_auto_update_unit:
            unit.target_commit = None
        unit.is_compilable_unit = manifest.is_compilable
        self.is_valid_update_rule(unit)
        self.git_repo_repository.require_compiled_tag(
            unit, selected_commit, selected_tag
        )
        self.git_repo_repository.is_valid_firmware_platform(
            repo,
            unit,
            unit.target_firmware_platform,
            (selected_commit, selected_tag),
        )
        return selected_commit, selected_tag, manifest, dictionary

    def get(self, uuid: uuid_pkg.UUID) -> Unit:
        self.access_service.authorization.check_access(
            [AgentType.BOT, AgentType.USER, AgentType.UNIT]
        )
        unit = self.unit_repository.get(Unit(uuid=uuid))
        is_valid_object(unit, NotFoundEntity.UNIT)
        self.access_service.authorization.check_visibility(unit)
        return unit

    def update(self, uuid: uuid_pkg.UUID, data: UnitUpdate) -> Unit:
        self.access_service.authorization.check_access([AgentType.USER])
        try:
            unit = self.unit_repository.get(Unit(uuid=uuid), for_update=True)
            is_valid_object(unit, NotFoundEntity.UNIT)
            self.access_service.authorization.check_ownership(
                unit, [OwnershipType.CREATOR]
            )
            self.apply_unit_update_fields(unit, data)
            self.unit_repository.is_valid_name(unit.name)
            return self.apply_version_update(unit, suppress_mqtt_error=True)
        except Exception as error:
            self.unit_repository.db.rollback()
            if isinstance(error, IntegrityError):
                self.unit_repository.raise_name_conflict(error)
            raise

    @staticmethod
    def apply_unit_update_fields(unit: Unit, data: UnitUpdate) -> None:
        if unit.is_auto_update_unit and data.is_auto_update_unit is False:
            unit.target_commit = None
        unit.sqlmodel_update(
            data.model_dump(exclude_none=True, exclude={"is_compilable_unit"})
        )

    def check_manual_update(
        self, uuid: uuid_pkg.UUID, data: UnitUpdate
    ) -> UnitUpdatePreviewRead:
        self.access_service.authorization.check_access([AgentType.USER])
        unit = self.unit_repository.get(Unit(uuid=uuid))
        is_valid_object(unit, NotFoundEntity.UNIT)
        self.access_service.authorization.check_ownership(
            unit, [OwnershipType.CREATOR]
        )

        candidate = Unit(**unit.model_dump())
        self.apply_unit_update_fields(candidate, data)
        if candidate.is_auto_update_unit:
            msg = "Update preview is available for manual updates only"
            raise UnitError(
                msg, code=ErrorCode.UNIT_UPDATE_PREVIEW_MANUAL_ONLY
            )

        repo = self.repo_repository.get(Repo(uuid=unit.repo_uuid))
        is_valid_object(repo, NotFoundEntity.REPO)
        selected_commit, _ = (
            self.git_repo_repository.get_selected_commit_and_tag(repo, unit)
        )
        target_commit, _, after, after_dictionary = self.select_unit_manifest(
            repo, candidate
        )
        warnings = []
        if selected_commit != target_commit:
            before, before_dictionary = (
                self.unit_manifest_service.read_from_commit(
                    repo, selected_commit
                )
            )
            warnings = list(
                UnitManifestDiff.compare(
                    before,
                    after,
                    self.env_fields(before, before_dictionary),
                    self.env_fields(after, after_dictionary),
                )
            )
        return UnitUpdatePreviewRead(
            selected_commit=selected_commit,
            target_commit=target_commit,
            warnings=warnings,
        )

    def apply_version_update(
        self, unit: Unit, *, suppress_mqtt_error: bool = False
    ) -> Unit:
        repo = self.repo_repository.get(Repo(uuid=unit.repo_uuid))
        is_valid_object(repo, NotFoundEntity.REPO)
        selected_commit, selected_tag, manifest, dictionary = (
            self.select_unit_manifest(repo, unit)
        )
        if unit.cipher_env_dict:
            env = self.generate_unified_env(
                unit, manifest, dictionary, selected_commit
            )
            self.save_validated_env(unit, manifest, dictionary, env)

        uuid = unit.uuid
        result = self.unit_io_service.synchronize(unit, manifest)
        self.unit_repository.db.commit()
        notify_message_processor()
        try:
            self.unit_io_service.command_to_input_base_topic(
                uuid=uuid,
                command=BackendTopicCommand.UPDATE,
                is_auto_update=True,
                selected_commit_and_tag=(selected_commit, selected_tag),
                manifest=manifest,
            )
        except MqttError:
            if not suppress_mqtt_error:
                raise
        return result

    def get_env(self, uuid: uuid_pkg.UUID) -> dict[str, JsonValue]:
        unit, selected_commit, manifest, dictionary = (
            self.read_owned_unit_manifest(uuid)
        )
        env = self.generate_unified_env(
            unit, manifest, dictionary, selected_commit
        )
        if self.access_service.current_agent.type == AgentType.UNIT:
            self.validate_env(manifest, dictionary, env)
        return env

    def get_env_max_size(self, uuid: uuid_pkg.UUID) -> UnitEnvSizeRead:
        unit, selected_commit, manifest, dictionary = (
            self.read_owned_unit_manifest(uuid)
        )
        fields = self.env_fields(manifest, dictionary)
        runtime_values = self.runtime_env(unit.uuid, selected_commit)
        fixed = {
            name: runtime_values[name]
            for name in fields
            if name in runtime_values
        }
        self.validate_env_values(manifest, fields, fixed)

        max_chars = len(UnitEnvSerializer.serialize(fixed))
        editable = {
            name: field for name, field in fields.items() if name not in fixed
        }
        for name, size in UnitEnvSize.calculate(editable).items():
            if size is None:
                if editable[name][FieldKey.OPTIONAL]:
                    continue
                msg = f"Required environment variable env.{name} has no valid value"
                raise GitRepoError(
                    msg, code=ErrorCode.GIT_REPO_UNIT_MANIFEST_INVALID
                )
            max_chars += len(UnitEnvSerializer.format_line(name, "")) + size
        return UnitEnvSizeRead(max_chars=max_chars)

    def read_owned_unit_manifest(
        self, uuid: uuid_pkg.UUID
    ) -> tuple[Unit, str, UnitDefinition, ManifestDictionary]:
        self.access_service.authorization.check_access(
            [AgentType.USER, AgentType.UNIT]
        )

        unit = self.unit_repository.get(Unit(uuid=uuid))
        is_valid_object(unit, NotFoundEntity.UNIT)

        self.access_service.authorization.check_ownership(
            unit, [OwnershipType.CREATOR, OwnershipType.UNIT]
        )

        repo = self.repo_repository.get(Repo(uuid=unit.repo_uuid))
        is_valid_object(repo, NotFoundEntity.REPO)

        selected_commit, _ = (
            self.git_repo_repository.get_selected_commit_and_tag(repo, unit)
        )

        manifest, dictionary = self.unit_manifest_service.read_from_commit(
            repo, selected_commit
        )
        return unit, selected_commit, manifest, dictionary

    def set_env(self, uuid: uuid_pkg.UUID, env_json_str: str) -> None:
        self.access_service.authorization.check_access([AgentType.USER])

        unit = self.unit_repository.get(Unit(uuid=uuid), for_update=True)
        is_valid_object(unit, NotFoundEntity.UNIT)

        self.access_service.authorization.check_ownership(
            unit, [OwnershipType.CREATOR]
        )

        repo = self.repo_repository.get(Repo(uuid=unit.repo_uuid))
        is_valid_object(repo, NotFoundEntity.REPO)

        selected_commit, _ = (
            self.git_repo_repository.get_selected_commit_and_tag(repo, unit)
        )

        manifest, dictionary = self.unit_manifest_service.read_from_commit(
            repo, selected_commit
        )
        submitted = self.require_env_object(
            is_valid_json(env_json_str, JsonPayload.ENV)
        )
        env = self.generate_unified_env(
            unit, manifest, dictionary, selected_commit, submitted
        )
        self.save_validated_env(unit, manifest, dictionary, env)
        unit.last_update_datetime = datetime.datetime.now(datetime.UTC)
        self.unit_repository.update(unit.uuid, unit)

    def reset_env(self, uuid: uuid_pkg.UUID) -> None:
        self.access_service.authorization.check_access([AgentType.USER])

        unit = self.unit_repository.get(Unit(uuid=uuid), for_update=True)
        is_valid_object(unit, NotFoundEntity.UNIT)

        self.access_service.authorization.check_ownership(
            unit, [OwnershipType.CREATOR]
        )

        unit.cipher_env_dict = None
        unit.last_update_datetime = datetime.datetime.now(datetime.UTC)
        self.unit_repository.update(unit.uuid, unit)

    def get_selected_version(self, uuid: uuid_pkg.UUID) -> SelectedVersionRead:
        self.access_service.authorization.check_access(
            [AgentType.USER, AgentType.UNIT]
        )
        unit = self.unit_repository.get(Unit(uuid=uuid))
        is_valid_object(unit, NotFoundEntity.UNIT)

        self.access_service.authorization.check_ownership(
            unit, [OwnershipType.CREATOR, OwnershipType.UNIT]
        )

        repo = self.repo_repository.get(Repo(uuid=unit.repo_uuid))
        is_valid_object(repo, NotFoundEntity.REPO)

        selected_commit, selected_tag = (
            self.git_repo_repository.get_selected_commit_and_tag(repo, unit)
        )
        return SelectedVersionRead(commit=selected_commit, tag=selected_tag)

    def get_io_topics(self, uuid: uuid_pkg.UUID) -> UnitIOTopicsRead:
        unit, selected_commit, manifest, _ = self.read_owned_unit_manifest(
            uuid
        )
        io_uuids = self.unit_io_repository.get_topic_uuids(unit.uuid)
        topics = UnitIOTopicsRead()
        for direction, declarations, entries in (
            (UnitIOTypeEnum.INPUT, manifest.input, topics.input),
            (UnitIOTypeEnum.OUTPUT, manifest.output, topics.output),
        ):
            for name, field in declarations.items():
                io_uuid = io_uuids.get((direction, name))
                if io_uuid is None:
                    msg = f"UnitIO {direction.value} {name!r} is missing for commit {selected_commit}"
                    raise UnitIOError(msg, code=ErrorCode.UNIT_IO_NOT_FOUND)
                entries[name] = UnitIOTopicRead(
                    topic=f"{settings.pu_domain}/{io_uuid}",
                    qos=MqttQos(field[FieldKey.QOS]),
                )
        return topics

    def get_current_schema(self, uuid: uuid_pkg.UUID) -> dict:
        unit, selected_commit, manifest, dictionary = (
            self.read_owned_unit_manifest(uuid)
        )
        return self.generate_current_schema(
            unit, selected_commit, manifest, dictionary
        )

    def get_unit_firmware(self, uuid: uuid_pkg.UUID) -> str:
        self.access_service.authorization.check_access(
            [AgentType.USER, AgentType.UNIT]
        )

        unit = self.unit_repository.get(Unit(uuid=uuid))

        self.access_service.authorization.check_ownership(
            unit, [OwnershipType.CREATOR, OwnershipType.UNIT]
        )

        repo = self.repo_repository.get(Repo(uuid=unit.repo_uuid))
        is_valid_object(repo, NotFoundEntity.REPO)

        selected_commit, _ = (
            self.git_repo_repository.get_selected_commit_and_tag(repo, unit)
        )

        manifest, dictionary = self.unit_manifest_service.read_from_commit(
            repo, selected_commit
        )
        env_dict = self.generate_unified_env(
            unit, manifest, dictionary, selected_commit
        )
        self.validate_env(manifest, dictionary, env_dict)

        new_schema_dict = self.generate_current_schema(
            unit, selected_commit, manifest, dictionary
        )

        gen_uuid = uuid_pkg.uuid4()

        if unit.is_compilable_unit:
            tmp_git_repo_path = self.git_repo_repository.get_tmp_path(gen_uuid)
            os.mkdir(tmp_git_repo_path)
        else:
            tmp_git_repo_path = self.git_repo_repository.generate_tmp_git_repo(
                repo, selected_commit, gen_uuid
            )

        with open(
            f"{tmp_git_repo_path}/{StaticRepoFileName.ENV.value}",
            "w",
            encoding="utf-8",
            newline="\n",
        ) as f:
            f.write(UnitEnvSerializer.serialize(env_dict))

        with open(
            f"{tmp_git_repo_path}/{StaticRepoFileName.SCHEMA.value}", "w"
        ) as f:
            f.write(json.dumps(new_schema_dict, indent=4))

        return tmp_git_repo_path

    def get_unit_firmware_zip(self, uuid: uuid_pkg.UUID) -> str:
        firmware_path = self.get_unit_firmware(uuid)
        firmware_zip_path = f"tmp/{uuid}"

        shutil.make_archive(firmware_zip_path, "zip", firmware_path)
        shutil.rmtree(firmware_path, ignore_errors=True)

        return f"{firmware_zip_path}.zip"

    def get_unit_firmware_tar(self, uuid: uuid_pkg.UUID) -> str:
        firmware_path = self.get_unit_firmware(uuid)
        firmware_tar_path = f"tmp/{uuid}"

        shutil.make_archive(firmware_tar_path, "tar", firmware_path)
        shutil.rmtree(firmware_path, ignore_errors=True)

        return f"{firmware_tar_path}.tar"

    def get_unit_firmware_tgz(
        self, uuid: uuid_pkg.UUID, wbits: int = 15, level: int = 9
    ) -> str:
        self.is_valid_wbits(wbits)
        self.is_valid_level(level)

        firmware_path = self.get_unit_firmware(uuid)
        firmware_tar_path = f"tmp/{uuid}"

        shutil.make_archive(firmware_tar_path, "tar", firmware_path)
        shutil.rmtree(firmware_path, ignore_errors=True)

        with open(firmware_tar_path + ".tar", "rb") as tar_file:
            producer = zlib.compressobj(wbits=wbits, level=level)

            tar_data = producer.compress(tar_file.read()) + producer.flush()

            with open(f"{firmware_tar_path}.tgz", "wb") as tgz:
                tgz.write(tar_data)

        os.remove(firmware_tar_path + ".tar")

        return f"{firmware_tar_path}.tgz"

    def set_state_storage(self, uuid: uuid_pkg.UUID, state: str) -> None:
        self.access_service.authorization.check_access(
            [AgentType.USER, AgentType.UNIT]
        )
        unit = self.unit_repository.get(Unit(uuid=uuid))

        is_valid_object(unit, NotFoundEntity.UNIT)

        self.access_service.authorization.check_ownership(
            unit, [OwnershipType.CREATOR, OwnershipType.UNIT]
        )

        unit.cipher_state_storage = (
            aes_gcm_encode(state) if state != "" else None
        )
        unit.last_update_datetime = datetime.datetime.now(datetime.UTC)
        self.unit_repository.update(unit.uuid, unit)

    def get_state_storage(self, uuid: uuid_pkg.UUID) -> str:
        self.access_service.authorization.check_access(
            [AgentType.USER, AgentType.UNIT]
        )
        unit = self.unit_repository.get(Unit(uuid=uuid))

        is_valid_object(unit, NotFoundEntity.UNIT)

        self.access_service.authorization.check_ownership(
            unit, [OwnershipType.CREATOR, OwnershipType.UNIT]
        )

        return (
            aes_gcm_decode(unit.cipher_state_storage)
            if unit.cipher_state_storage
            else ""
        )

    def get_mqtt_auth(self, topic: str) -> None:
        self.access_service.authorization.check_access(
            [AgentType.BACKEND, AgentType.UNIT]
        )

        if isinstance(
            self.access_service.current_agent, AgentUnit | AgentBackend
        ):
            struct_topic = get_topic_split(topic)

            len_struct = len(struct_topic)
            if len_struct == 5:
                if isinstance(self.access_service.current_agent, AgentBackend):
                    return

                backend_domain, destination, unit_uuid, topic_name, *_ = (
                    struct_topic
                )
                unit_uuid = is_valid_uuid(unit_uuid, UuidEntity.UNIT)

                if destination in [
                    DestinationTopicType.INPUT_BASE_TOPIC,
                    DestinationTopicType.OUTPUT_BASE_TOPIC,
                ]:
                    if self.access_service.current_agent.uuid != unit_uuid:
                        msg = "Available only for a docked Unit"
                        raise NoAccessError(
                            msg, code=ErrorCode.ACCESS_DOCKED_UNIT_ONLY
                        )
                else:
                    msg = f"Topic destination {destination} is invalid, available {list(DestinationTopicType)}"
                    raise NoAccessError(
                        msg, code=ErrorCode.ACCESS_TOPIC_DESTINATION_INVALID
                    )

            elif len_struct in [2, 3]:
                backend_domain, unit_io_uuid, *_ = struct_topic

                if (
                    unit_io_uuid == "+"
                    and self.access_service.current_agent.type
                    == AgentType.BACKEND
                ):
                    return

                unit_io_uuid = is_valid_uuid(unit_io_uuid, UuidEntity.UNIT_IO)

                unit_io = self.unit_io_repository.get(
                    UnitIO(uuid=unit_io_uuid)
                )
                is_valid_object(unit_io, NotFoundEntity.UNIT_IO)

                self.access_service.authorization.check_visibility(unit_io)
            else:
                msg = f"Topic struct is invalid, len {len_struct}, available - [2, 3]"
                raise MqttError(msg, code=ErrorCode.MQTT_TOPIC_STRUCT)
        else:
            msg = "Only for Unit available topic communication"
            raise MqttError(msg, code=ErrorCode.MQTT_UNIT_TOPIC_ONLY)

    def delete(self, uuid: uuid_pkg.UUID) -> None:
        self.access_service.authorization.check_access([AgentType.USER])

        unit = self.unit_repository.get(Unit(uuid=uuid))
        is_valid_object(unit, NotFoundEntity.UNIT)

        self.access_service.authorization.check_ownership(
            unit, [OwnershipType.CREATOR]
        )

        count, unit_ios = self.unit_io_repository.list(
            UnitIOFilter.unlimited(unit_uuid=unit.uuid)
        )

        # orm feature =_=
        unit_deep = copy.deepcopy(unit)
        unit_ios_deep = copy.deepcopy(unit_ios)

        self.unit_repository.delete(unit)
        notify_message_processor()

        # clickhouse data clear
        self.unit_io_service.data_pipe_repository.bulk_delete(
            [unit_io.uuid for unit_io in unit_ios_deep]
        )
        self.unit_log_repository.delete(unit_deep.uuid)

    def list(
        self,
        filters: UnitFilter,
        is_include_output_unit_ios: bool = False,
    ) -> tuple[int, list[tuple[Unit, list[dict]]]]:
        self.access_service.authorization.check_access(
            [AgentType.BOT, AgentType.USER, AgentType.UNIT]
        )
        restriction = self.access_service.authorization.access_restriction(
            resource_type=PermissionEntities.UNIT
        )

        filters.visibility_level = (
            self.access_service.authorization.get_available_visibility_levels(
                filters.visibility_level, restriction
            )
        )
        return self.unit_repository.list(
            filters,
            restriction=restriction,
            is_include_output_unit_ios=is_include_output_unit_ios,
        )

    def log_list(
        self, filters: UnitLogFilter
    ) -> tuple[int, builtins.list[UnitLog]]:
        self.access_service.authorization.check_access(
            [AgentType.USER, AgentType.UNIT]
        )

        unit = self.unit_repository.get(Unit(uuid=filters.uuid))
        is_valid_object(unit, NotFoundEntity.UNIT)

        self.access_service.authorization.check_ownership(
            unit, [OwnershipType.CREATOR, OwnershipType.UNIT]
        )

        return self.unit_log_repository.list(filters)

    def get_unit_logs_file(self, uuid: uuid_pkg.UUID) -> tuple[str, str]:
        self.access_service.authorization.check_access(
            [AgentType.USER, AgentType.UNIT]
        )

        unit = self.unit_repository.get(Unit(uuid=uuid))
        is_valid_object(unit, NotFoundEntity.UNIT)

        self.access_service.authorization.check_ownership(
            unit, [OwnershipType.CREATOR, OwnershipType.UNIT]
        )

        _, logs = self.unit_log_repository.list(
            UnitLogFilter.unlimited(
                uuid=uuid,
                level=[item.value for item in LogLevel],
                order_by_create_date=OrderByDate.asc,
            )
        )

        log_filepath = f"tmp/{uuid}_{uuid_pkg.uuid4()}.log"
        with open(log_filepath, "w", encoding="utf-8") as handle:
            handle.writelines(f"{log.to_log_line()}\n" for log in logs)

        return log_filepath, f"{unit.name}.log"

    def bulk_update_units_firmware(self) -> str:
        _, units = self.unit_repository.list(
            UnitFilter.unlimited(is_auto_update_unit=True)
        )
        unit_uuids = [unit.uuid for unit, _ in units]
        logging.info(f"{len(unit_uuids)} units update launched")

        success = 0
        error = 0
        for uuid in unit_uuids:
            try:
                unit = self.unit_repository.get(
                    Unit(uuid=uuid), for_update=True
                )
                if unit is None or not unit.is_auto_update_unit:
                    self.unit_repository.db.rollback()
                    continue
                self.apply_version_update(unit)
                success += 1
            except Exception as exc:
                self.unit_repository.db.rollback()
                logging.warning(f"Failed update unit {uuid} {exc}")
                error += 1

        return f"Updated {success}, failed {error}"

    def schedule_bulk_update_units_firmware(self) -> OperationTask:
        self.access_service.authorization.check_access(
            [AgentType.USER],
            [UserRole.ADMIN],
        )
        task = self.operation_task_service.create(
            OperationTaskCreate(
                task_type=OperationTaskType.UPDATE_ALL_UNITS_FIRMWARE,
            )
        )

        def operation(_db):
            with BackgroundService() as services:
                return services.get_unit_service().bulk_update_units_firmware()

        self.operation_task_service.schedule(task, operation)
        return task

    def generate_current_schema(
        self,
        unit: Unit,
        selected_commit: str,
        manifest: UnitDefinition,
        dictionary: ManifestDictionary,
    ) -> dict:
        addresses = {}
        for (
            io_uuid,
            name,
            direction,
            links,
        ) in self.unit_io_repository.get_nodes_with_links(unit.uuid):
            addresses[direction, name] = [
                get_topic_name(io_uuid),
                *(get_topic_name(link_uuid) for link_uuid, _ in (links or ())),
            ]

        schema = {destination: {} for destination in DestinationTopicType}
        for (
            direction,
            declarations,
            reserved,
            base_destination,
            custom_destination,
        ) in (
            (
                UnitIOTypeEnum.INPUT,
                manifest.input,
                dictionary.input,
                DestinationTopicType.INPUT_BASE_TOPIC,
                DestinationTopicType.INPUT_TOPIC,
            ),
            (
                UnitIOTypeEnum.OUTPUT,
                manifest.output,
                dictionary.output,
                DestinationTopicType.OUTPUT_BASE_TOPIC,
                DestinationTopicType.OUTPUT_TOPIC,
            ),
        ):
            for name in declarations:
                if name in reserved:
                    topic = name + GlobalPrefixTopic.BACKEND_SUB_PREFIX.value
                    schema[base_destination][topic] = [
                        f"{settings.pu_domain}/{base_destination.value}/{unit.uuid}/{topic}"
                    ]
                    continue
                topics = addresses.get((direction, name))
                if topics is None:
                    msg = f"UnitIO {direction.value} {name!r} is missing for commit {selected_commit}"
                    raise UnitIOError(msg, code=ErrorCode.UNIT_IO_NOT_FOUND)
                schema[custom_destination][name] = topics
        return schema

    def generate_token(self, uuid: uuid_pkg.UUID) -> str:
        unit = self.unit_repository.get(Unit(uuid=uuid))
        is_valid_object(unit, NotFoundEntity.UNIT)

        return AgentUnit(**unit.dict()).generate_agent_token()

    def runtime_env(
        self, uuid: uuid_pkg.UUID, selected_commit: str
    ) -> dict[str, JsonValue]:
        instance = runtime.get()
        return {
            ReservedEnvVariableName.PU_DOMAIN: settings.pu_domain,
            ReservedEnvVariableName.PU_HTTP_TYPE: settings.pu_http_type,
            ReservedEnvVariableName.PU_APP_PREFIX: settings.pu_app_prefix,
            ReservedEnvVariableName.PU_API_ACTUAL_PREFIX: settings.pu_api_v1_prefix,
            ReservedEnvVariableName.PU_MQTT_HOST: settings.pu_mqtt_host,
            ReservedEnvVariableName.PU_MQTT_PORT: settings.pu_mqtt_port,
            ReservedEnvVariableName.PU_AUTH_TOKEN: self.generate_token(uuid),
            ReservedEnvVariableName.PU_MQTT_KEEPALIVE: instance.pu_mqtt_keepalive,
            ReservedEnvVariableName.PU_STATE_SEND_INTERVAL: instance.pu_instance_unit_state_send_interval,
            ReservedEnvVariableName.PU_COMMIT_VERSION: selected_commit,
        }

    def generate_unified_env(
        self,
        unit: Unit,
        manifest: UnitDefinition,
        dictionary: ManifestDictionary,
        selected_commit: str,
        user_env_dict: dict[str, JsonValue] | None = None,
    ) -> dict[str, JsonValue]:
        fields = manifest.env | dictionary.env
        defaults = {
            name: field[FieldKey.DEFAULT]
            for name, field in fields.items()
            if FieldKey.DEFAULT in field
        }
        saved = {}
        if unit.cipher_env_dict:
            saved = self.require_env_object(
                is_valid_json(
                    aes_gcm_decode(unit.cipher_env_dict),
                    JsonPayload.CIPHER_ENV,
                )
            )
        values = defaults | saved | (user_env_dict or {})
        for name in (
            ReservedEnvVariableName.PU_SECRET_KEY,
            ReservedEnvVariableName.PU_ENCRYPT_KEY,
        ):
            if name not in values:
                values[name] = base64.b64encode(os.urandom(32)).decode("ascii")
        values.update(self.runtime_env(unit.uuid, selected_commit))
        return {name: values[name] for name in fields if name in values}

    @staticmethod
    def require_env_object(env: JsonValue) -> dict[str, JsonValue]:
        if not isinstance(env, dict):
            msg = "Unit settings must be a JSON object"
            raise UnitError(msg, code=ErrorCode.GIT_REPO_ENV_JSON_INVALID)
        return env

    @staticmethod
    def env_fields(
        manifest: UnitDefinition, dictionary: ManifestDictionary
    ) -> FieldDeclarations:
        standard_fields = copy.deepcopy(dictionary.env)
        for field in standard_fields.values():
            manifest.apply_field_defaults(field, dictionary)
        return manifest.env | standard_fields

    @classmethod
    def validate_env(
        cls,
        manifest: UnitDefinition,
        dictionary: ManifestDictionary,
        env: dict[str, JsonValue],
    ) -> None:
        fields = cls.env_fields(manifest, dictionary)
        missing = [
            name
            for name, field in fields.items()
            if name not in env
            and not field.get(
                FieldKey.OPTIONAL, dictionary.defaults[FieldKey.OPTIONAL]
            )
        ]
        if missing:
            msg = f"Required Unit settings are missing: {', '.join(missing)}"
            raise UnitError(msg, code=ErrorCode.GIT_REPO_ENV_UNRESOLVED)
        cls.validate_env_values(manifest, fields, env)

    @staticmethod
    def validate_env_values(
        manifest: UnitDefinition,
        fields: FieldDeclarations,
        env: dict[str, JsonValue],
    ) -> None:
        try:
            for name, field in fields.items():
                if name in env:
                    manifest.validate_literal(
                        field, env[name], (FieldKey.ENV, name)
                    )
            json.dumps(env, allow_nan=False, ensure_ascii=False).encode(
                "utf-8"
            )
        except ValidationError as error:
            raise UnitError(
                str(error), code=ErrorCode.GIT_REPO_ENV_JSON_INVALID
            ) from error
        except ValueError as error:
            msg = "Unit settings must contain finite UTF-8 JSON values"
            raise UnitError(
                msg, code=ErrorCode.GIT_REPO_ENV_JSON_INVALID
            ) from error

    def save_validated_env(
        self,
        unit: Unit,
        manifest: UnitDefinition,
        dictionary: ManifestDictionary,
        env: dict[str, JsonValue],
    ) -> None:
        self.validate_env(manifest, dictionary, env)
        stored = dict(env)
        stored.pop(ReservedEnvVariableName.PU_COMMIT_VERSION.value, None)
        unit.cipher_env_dict = aes_gcm_encode(json.dumps(stored))

    @staticmethod
    def is_valid_update_rule(data: Unit | UnitCreate) -> None:
        if (
            data.is_compilable_unit
            and data.is_auto_update_unit
            and not data.is_only_tag_update
        ):
            msg = "Compiled units use only tags when updating automatically"
            raise UnitError(msg, code=ErrorCode.UNIT_AUTOUPDATE_TAGS_ONLY)

        if not data.is_auto_update_unit and (
            not data.target_branch or not data.target_commit
        ):
            msg = "Unit updated manually requires branch and commit to be filled out"
            raise UnitError(
                msg, code=ErrorCode.UNIT_MANUAL_UPDATE_BRANCH_COMMIT
            )

    @staticmethod
    def mapper_unit_to_unit_read(
        unit: tuple[Unit, builtins.list[dict]],
    ) -> UnitRead:
        return UnitRead(
            **unit[0].to_dict(),
            unit_ios=[UnitIORead(**item) for item in unit[1]],
        )

    @staticmethod
    def is_valid_wbits(wbits: int):
        available_values_list = list(
            itertools.chain(range(-15, -8), range(9, 16), range(25, 32))
        )
        if wbits not in available_values_list:
            msg = f"Wbits {wbits} is not valid, available {available_values_list}"
            raise UnitError(msg, code=ErrorCode.UNIT_WBITS_INVALID)

    @staticmethod
    def is_valid_level(level: int):
        available_values_list = list(range(-1, 10))
        if level not in available_values_list:
            msg = f"Level {level} is not valid, available {available_values_list}"
            raise UnitError(msg, code=ErrorCode.UNIT_LEVEL_INVALID)
