import base64
import contextlib
import datetime
import json
import logging
import uuid as uuid_pkg

from fastapi import Depends, UploadFile
from pydantic import ValidationError

from app import runtime, settings
from app.configs.error_codes import ErrorCode
from app.configs.errors import (
    CustomException,
    CustomPermissionError,
    DataPipeError,
    FeatureFlagError,
    MqttError,
    UnitIOError,
)
from app.configs.redis import DataPipeConfigAction, send_to_data_pipe_stream
from app.domain.link_model import Link
from app.domain.repo_model import Repo
from app.domain.unit_io_model import UnitIO
from app.domain.unit_model import Unit
from app.dto.clickhouse.aggregation import Aggregation
from app.dto.clickhouse.last_value import LastValue
from app.dto.clickhouse.n_records import NRecords
from app.dto.clickhouse.time_window import TimeWindow
from app.dto.enum import (
    AgentType,
    BackendTopicCommand,
    DestinationTopicType,
    FieldKey,
    GlobalPrefixTopic,
    JsonPayload,
    NotFoundEntity,
    OrderByDate,
    OwnershipType,
    PayloadCodec,
    PermissionEntities,
    ProcessingPolicyType,
    ReservedEnvVariableName,
    ReservedInputBaseTopic,
    ReservedOutputBaseTopic,
    UnitFirmwareUpdateStatus,
    UnitIOTypeEnum,
    UuidEntity,
)
from app.repositories.data_pipe_repository import DataPipeRepository
from app.repositories.git_repo_repository import GitRepoRepository
from app.repositories.link_repository import LinkRepository
from app.repositories.repo_repository import (
    RepoRepository,
)
from app.repositories.unit_io_repository import UnitIORepository
from app.repositories.unit_log_repository import UnitLogRepository
from app.repositories.unit_repository import UnitRepository
from app.schemas.mqtt.utils import publish_to_topic
from app.schemas.pydantic.unit_io import (
    DataPipeConfigOptionsRead,
    DataPipeFilter,
    DataPipeValidationErrorRead,
    LinkCompatibilityRead,
    LinkCreate,
    UnitIOFilter,
    UnitIOSetState,
    UnitIOUpdate,
)
from app.schemas.pydantic.unit_manifest import (
    FieldDeclaration,
    UnitDefinition,
)
from app.services.access_service import AccessService
from app.services.permission_service import PermissionService
from app.services.unit_io_compatibility import (
    UnitIOCompatibility,
    UnitIOCompatibilityResult,
)
from app.services.unit_manifest_service import UnitManifestService
from app.services.utils import (
    dict_to_yml_file,
    get_topic_name,
    merge_two_dict_first_priority,
    remove_none_value_dict,
    yml_file_to_dict,
)
from app.services.validators import (
    is_valid_json,
    is_valid_object,
    is_valid_uuid,
    is_valid_visibility_level,
)
from app.utils.utils import async_chunked, obj_serializer, remove_dict_none
from app.validators.content_size import is_valid_content_size
from app.validators.data_pipe import DataPipeConfig, is_valid_data_pipe_config
from app.validators.data_pipe_user_csv import StreamingCSVValidator


class UnitIOService:
    def __init__(
        self,
        unit_repository: UnitRepository = Depends(),
        repo_repository: RepoRepository = Depends(),
        unit_io_repository: UnitIORepository = Depends(),
        unit_log_repository: UnitLogRepository = Depends(),
        data_pipe_repository: DataPipeRepository = Depends(),
        link_repository: LinkRepository = Depends(),
        permission_service: PermissionService = Depends(),
        access_service: AccessService = Depends(),
        unit_manifest_service: UnitManifestService = Depends(),
    ) -> None:
        self.unit_repository = unit_repository
        self.repo_repository = repo_repository
        self.git_repo_repository = GitRepoRepository()
        self.unit_io_repository = unit_io_repository
        self.unit_log_repository = unit_log_repository
        self.data_pipe_repository = data_pipe_repository
        self.link_repository = link_repository
        self.permission_service = permission_service
        self.access_service = access_service
        self.unit_manifest_service = unit_manifest_service

    def get(self, uuid: uuid_pkg.UUID) -> UnitIO:
        self.access_service.authorization.check_access(
            [AgentType.BOT, AgentType.USER, AgentType.UNIT]
        )
        unit_io = self.unit_io_repository.get(UnitIO(uuid=uuid))
        self.access_service.authorization.check_visibility(unit_io)

        is_valid_object(unit_io, NotFoundEntity.UNIT_IO)
        return unit_io

    @staticmethod
    def is_standard_topic(name: str) -> bool:
        return (
            name in ReservedInputBaseTopic or name in ReservedOutputBaseTopic
        )

    @classmethod
    def new_unit_io(
        cls, unit: Unit, direction: UnitIOTypeEnum, name: str
    ) -> UnitIO:
        now = datetime.datetime.now(datetime.UTC)
        unit_io = UnitIO(
            type=direction,
            visibility_level=unit.visibility_level,
            topic_name=name,
            creator_uuid=unit.creator_uuid,
            unit_uuid=unit.uuid,
            create_datetime=now,
            last_update_datetime=now,
        )

        if (
            runtime.get().pu_datapipe_default_last_value_enable
            and not cls.is_standard_topic(name)
        ):
            unit_io.is_data_pipe_active = True
            unit_io.data_pipe_yml = json.dumps(
                {
                    "active_period": {
                        "type": "Permanent",
                        "start": None,
                        "end": None,
                    },
                    "filters": {
                        "type_input_value": "Text",
                        "type_value_filtering": None,
                        "filtering_values": None,
                        "type_value_threshold": None,
                        "threshold_min": None,
                        "threshold_max": None,
                        "max_rate": 5,
                        "last_unique_check": False,
                        "max_size": 100,
                    },
                    "transformations": None,
                    "processing_policy": {
                        "policy_type": "LastValue",
                        "n_records_count": None,
                        "time_window_size": None,
                        "aggregation_functions": None,
                    },
                }
            )

        return unit_io

    def synchronize(self, unit: Unit, manifest: UnitDefinition) -> Unit:
        unit_ios = [
            self.new_unit_io(unit, direction, name)
            for direction, declarations in (
                (UnitIOTypeEnum.INPUT, manifest.input),
                (UnitIOTypeEnum.OUTPUT, manifest.output),
            )
            for name in declarations
        ]
        permissions = [
            permission
            for unit_io in unit_ios
            for permission in self.permission_service.default_unit_permissions(
                unit, unit_io
            )
        ]
        return self.unit_io_repository.synchronize(
            unit,
            manifest.is_compilable,
            unit_ios,
            permissions,
        )

    async def update(self, uuid: uuid_pkg.UUID, data: UnitIOUpdate) -> UnitIO:
        self.access_service.authorization.check_access([AgentType.USER])

        unit_io = self.unit_io_repository.get(UnitIO(uuid=uuid))
        is_valid_object(unit_io, NotFoundEntity.UNIT_IO)

        self.access_service.authorization.check_ownership(
            unit_io, [OwnershipType.CREATOR]
        )

        is_standard_topic = self.is_standard_topic(unit_io.topic_name)
        if is_standard_topic and data.is_data_pipe_active:
            msg = "DataPipe cannot be activated for a standard IO"
            raise DataPipeError(msg, code=ErrorCode.DATAPIPE_STANDARD_IO)

        if data.is_rewritable_input is not None:
            self.is_valid_input_unit_io(unit_io)

        if data.max_connections is not None and data.max_connections < 1:
            msg = "max_connections must be at least 1"
            raise UnitIOError(msg, code=ErrorCode.UNIT_IO_MAX_CONNECTIONS)

        update_unit_io = UnitIO(
            **merge_two_dict_first_priority(
                remove_none_value_dict(data.dict()), unit_io.dict()
            )
        )
        is_valid_visibility_level(
            self.unit_repository.get(Unit(uuid=update_unit_io.unit_uuid)),
            [update_unit_io],
        )

        update_unit_io.last_update_datetime = datetime.datetime.now(
            datetime.UTC
        )

        unit_io_updated = self.unit_io_repository.update(uuid, update_unit_io)

        if (
            runtime.get().pu_ff_datapipe_enable
            and data.is_data_pipe_active is not None
            and not is_standard_topic
        ):
            await send_to_data_pipe_stream(
                DataPipeConfigAction.UPDATE
                if data.is_data_pipe_active
                else DataPipeConfigAction.DELETE,
                unit_io_updated.model_dump(),
            )

        return self.unit_io_repository.update(uuid, update_unit_io)

    def check_input_state(
        self,
        uuid: uuid_pkg.UUID,
        data: UnitIOSetState,
    ) -> tuple[UnitIO, bytes]:
        self.access_service.authorization.check_access(
            [AgentType.USER, AgentType.UNIT]
        )

        unit_io = self.unit_io_repository.get(UnitIO(uuid=uuid))
        is_valid_object(unit_io, NotFoundEntity.UNIT_IO)
        self.is_valid_input_unit_io(unit_io)
        self.access_service.authorization.check_ownership(
            unit_io, [OwnershipType.UNIT_TO_INPUT_NODE]
        )
        self.access_service.authorization.check_visibility(unit_io)

        selected_commit, manifest = self.read_selected_manifest(
            unit_io.unit_uuid
        )
        field = self.get_manifest_field(
            unit_io, FieldKey.INPUT, selected_commit, manifest
        )
        codec = field[FieldKey.CODEC]
        state = data.state
        try:
            payload = (
                base64.b64decode(state, validate=True)
                if codec == PayloadCodec.RAW
                else state.encode("utf-8")
            )
        except ValueError as error:
            msg = (
                "Input state must be valid Base64"
                if codec == PayloadCodec.RAW
                else "Input state must be valid UTF-8"
            )
            raise UnitIOError(
                msg, code=ErrorCode.UNIT_IO_STATE_INVALID
            ) from error

        max_payload_bytes = runtime.get().pu_mqtt_max_payload_size * 1024
        is_valid_content_size(
            payload,
            max_payload_bytes,
            code=ErrorCode.MQTT_PAYLOAD_TOO_LARGE,
            message=f"Input payload exceeds {max_payload_bytes} bytes",
            error_type=MqttError,
        )

        value = payload if codec == PayloadCodec.RAW else state
        if codec == PayloadCodec.JSON:
            try:
                value = is_valid_json(state, JsonPayload.UNIT_IO_STATE)
                json.dumps(value, allow_nan=False, ensure_ascii=False).encode(
                    "utf-8"
                )
            except (ValueError, RecursionError) as error:
                msg = "Input state must contain finite UTF-8 JSON values"
                raise UnitIOError(
                    msg, code=ErrorCode.UNIT_IO_STATE_JSON_INVALID
                ) from error
        elif codec == PayloadCodec.UTF8 and "\x00" in state:
            msg = "Input state cannot store a NUL character"
            raise UnitIOError(msg, code=ErrorCode.UNIT_IO_STATE_INVALID)

        try:
            manifest.validate_literal(
                field, value, (FieldKey.INPUT, unit_io.topic_name)
            )
        except ValidationError as error:
            raise UnitIOError(
                str(error), code=ErrorCode.UNIT_IO_STATE_INVALID
            ) from error

        return unit_io, payload

    def set_state_input(
        self,
        uuid: uuid_pkg.UUID,
        data: UnitIOSetState,
    ) -> UnitIO:
        unit_io, payload = self.check_input_state(uuid, data)
        publish_to_topic(get_topic_name(unit_io.uuid), payload)

        unit_io.state = data.state
        unit_io.last_update_datetime = datetime.datetime.now(datetime.UTC)
        return self.unit_io_repository.update(uuid, unit_io)

    def command_to_input_base_topic(
        self,
        uuid: uuid_pkg.UUID,
        command: BackendTopicCommand,
        is_auto_update: bool = False,
        *,
        selected_commit_and_tag: tuple[str, str | None] | None = None,
        manifest: UnitDefinition | None = None,
    ) -> None:
        if not is_auto_update:
            self.access_service.authorization.check_access(
                [AgentType.USER, AgentType.UNIT]
            )

        unit = self.unit_repository.get(Unit(uuid=uuid))
        is_valid_object(unit, NotFoundEntity.UNIT)

        if not is_auto_update:
            self.access_service.authorization.check_ownership(
                unit, [OwnershipType.CREATOR, OwnershipType.UNIT]
            )

        repo = self.repo_repository.get(Repo(uuid=unit.repo_uuid))
        is_valid_object(repo, NotFoundEntity.REPO)

        rechecked_commit_and_tag = (
            self.git_repo_repository.get_selected_commit_and_tag(repo, unit)
        )
        if (
            selected_commit_and_tag is not None
            and selected_commit_and_tag[0] != rechecked_commit_and_tag[0]
        ):
            return
        selected_commit, selected_tag = (
            selected_commit_and_tag or rechecked_commit_and_tag
        )
        self.git_repo_repository.is_valid_firmware_platform(
            repo,
            unit,
            unit.target_firmware_platform,
            (selected_commit, selected_tag),
        )

        command_to_topic_dict = {
            BackendTopicCommand.UPDATE: ReservedInputBaseTopic.UPDATE.value,
            BackendTopicCommand.ENV_UPDATE: ReservedInputBaseTopic.ENV_UPDATE.value,
            BackendTopicCommand.SCHEMA_UPDATE: ReservedInputBaseTopic.SCHEMA_UPDATE.value,
            BackendTopicCommand.LOG_SYNC: ReservedInputBaseTopic.LOG_SYNC.value,
            BackendTopicCommand.RESET: ReservedInputBaseTopic.RESET.value,
        }

        input_name = command_to_topic_dict[command]
        target_topic = input_name + GlobalPrefixTopic.BACKEND_SUB_PREFIX.value
        if manifest is None:
            manifest, _ = self.unit_manifest_service.read_from_commit(
                repo, selected_commit
            )
        if input_name in manifest.input:
            update_message_dict = self._build_message_dict(
                command,
                repo,
                unit,
                selected_commit,
                selected_tag,
            )

            try:
                publish_to_topic(
                    f"{settings.pu_domain}/{DestinationTopicType.INPUT_BASE_TOPIC.value}/{unit.uuid}/{target_topic}",
                    update_message_dict,
                )
                if command == BackendTopicCommand.UPDATE:
                    unit.firmware_update_error = None
                    unit.last_firmware_update_datetime = datetime.datetime.now(
                        datetime.UTC
                    )
                    unit.firmware_update_status = (
                        UnitFirmwareUpdateStatus.REQUEST_SENT
                    )

                    self.unit_repository.update(unit.uuid, unit)

            except MqttError as e:
                if command == BackendTopicCommand.UPDATE:
                    unit.firmware_update_error = e.message
                    unit.last_firmware_update_datetime = None
                    unit.firmware_update_status = (
                        UnitFirmwareUpdateStatus.ERROR
                    )

                    self.unit_repository.update(unit.uuid, unit)

                raise e

    def _build_message_dict(
        self,
        command: BackendTopicCommand,
        repo: Repo,
        unit: Unit,
        selected_commit: str,
        selected_tag: str,
    ) -> dict:
        message_dict = {"COMMAND": command}

        if command == BackendTopicCommand.UPDATE:
            message_dict[ReservedEnvVariableName.PU_COMMIT_VERSION.value] = (
                selected_commit
            )
            if unit.is_compilable_unit:
                links = is_valid_json(
                    repo.releases_data,
                    JsonPayload.RELEASES_FOR_COMPILE_REPO_TITLE,
                )[selected_tag]
                _, link = self.git_repo_repository.find_by_platform(
                    links, unit.target_firmware_platform
                )
                message_dict["COMPILED_FIRMWARE_LINK"] = link

        elif command == BackendTopicCommand.LOG_SYNC:
            self.unit_log_repository.delete(unit.uuid)

        return message_dict

    def get_link_nodes(self, data: LinkCreate) -> tuple[UnitIO, UnitIO]:
        self.access_service.authorization.check_access([AgentType.USER])
        output_uuid = is_valid_uuid(
            data.output_uuid, UuidEntity.UNIT_IO_OUTPUT
        )
        input_uuid = is_valid_uuid(data.input_uuid, UuidEntity.UNIT_IO_INPUT)

        output_node = self.unit_io_repository.get(UnitIO(uuid=output_uuid))
        is_valid_object(output_node, NotFoundEntity.UNIT_IO)
        self.is_valid_output_unit_io(output_node)
        self.access_service.authorization.check_visibility(output_node)

        input_node = self.unit_io_repository.get(UnitIO(uuid=input_uuid))
        is_valid_object(input_node, NotFoundEntity.UNIT_IO)
        self.is_valid_input_unit_io(input_node)
        self.access_service.authorization.check_visibility(input_node)
        return output_node, input_node

    def read_selected_manifest(
        self, unit_uuid: uuid_pkg.UUID
    ) -> tuple[str, UnitDefinition]:
        unit = self.unit_repository.get(Unit(uuid=unit_uuid))
        is_valid_object(unit, NotFoundEntity.UNIT)
        repo = self.repo_repository.get(Repo(uuid=unit.repo_uuid))
        is_valid_object(repo, NotFoundEntity.REPO)
        selected_commit, _ = (
            self.git_repo_repository.get_selected_commit_and_tag(repo, unit)
        )
        manifest, _ = self.unit_manifest_service.read_from_commit(
            repo, selected_commit
        )
        return selected_commit, manifest

    @staticmethod
    def get_manifest_field(
        node: UnitIO,
        section: FieldKey,
        selected_commit: str,
        manifest: UnitDefinition,
    ) -> FieldDeclaration:
        field = getattr(manifest, section).get(node.topic_name)
        if field is None:
            msg = f"UnitIO {node.type} {node.topic_name!r} is missing for commit {selected_commit}"
            raise UnitIOError(msg, code=ErrorCode.UNIT_IO_NOT_FOUND)
        return field

    def compare_link_schemas(
        self, output_node: UnitIO, input_node: UnitIO
    ) -> UnitIOCompatibilityResult:
        manifests = {
            unit_uuid: self.read_selected_manifest(unit_uuid)
            for unit_uuid in dict.fromkeys(
                (output_node.unit_uuid, input_node.unit_uuid)
            )
        }
        output = self.get_manifest_field(
            output_node, FieldKey.OUTPUT, *manifests[output_node.unit_uuid]
        )
        input_field = self.get_manifest_field(
            input_node, FieldKey.INPUT, *manifests[input_node.unit_uuid]
        )
        return UnitIOCompatibility().check_link(output, input_field)

    def check_link_compatibility(
        self, data: LinkCreate
    ) -> LinkCompatibilityRead:
        result = self.compare_link_schemas(*self.get_link_nodes(data))
        return LinkCompatibilityRead(
            compatible=result.compatible,
            depends_on_value=result.depends_on_value,
            issue=result.issue,
            path=result.path,
        )

    def create_link(self, data: LinkCreate) -> Link:
        output_node, input_node = self.get_link_nodes(data)

        # Check connection limit for input node (incoming connections)
        current_input_connections = self.link_repository.count_by_input_node(
            input_node.uuid
        )
        if current_input_connections >= input_node.max_connections:
            msg = f"Maximum number of incoming connections ({input_node.max_connections}) reached for input node {input_node.uuid}"
            raise UnitIOError(
                msg, code=ErrorCode.UNIT_IO_INPUT_CONNECTION_LIMIT
            )

        # Check connection limit for output node (outgoing connections)
        current_output_connections = self.link_repository.count_by_output_node(
            output_node.uuid
        )
        if current_output_connections >= output_node.max_connections:
            msg = f"Maximum number of outgoing connections ({output_node.max_connections}) reached for output node {output_node.uuid}"
            raise UnitIOError(
                msg, code=ErrorCode.UNIT_IO_OUTPUT_CONNECTION_LIMIT
            )

        new_link = Link(
            output_uuid=output_node.uuid,
            input_uuid=input_node.uuid,
            creator_uuid=self.access_service.current_agent.uuid,
        )

        if self.link_repository.check(new_link):
            msg = "Link exist"
            raise UnitIOError(msg, code=ErrorCode.LINK_EXISTS)

        compatibility = self.compare_link_schemas(output_node, input_node)
        if compatibility.issue is not None:
            path = ".".join(compatibility.path) or "$"
            msg = f"Output and Input are incompatible at {path} ({compatibility.issue.value})"
            raise UnitIOError(msg, code=ErrorCode.LINK_INCOMPATIBLE)

        with contextlib.suppress(CustomPermissionError):
            self.permission_service.create_by_domains(
                Unit(uuid=output_node.unit_uuid), input_node
            )

        with contextlib.suppress(CustomPermissionError):
            self.permission_service.create_by_domains(
                Unit(uuid=input_node.unit_uuid), output_node
            )

        with contextlib.suppress(CustomPermissionError):
            self.permission_service.create_by_domains(
                Unit(uuid=output_node.unit_uuid),
                Unit(uuid=input_node.unit_uuid),
            )

        link = self.link_repository.create(new_link)

        with contextlib.suppress(MqttError):
            self.command_to_input_base_topic(
                uuid=input_node.unit_uuid,
                command=BackendTopicCommand.SCHEMA_UPDATE,
                is_auto_update=True,
            )

        return link

    def get_links(self, unit_uuid: uuid_pkg.UUID) -> tuple[int, list[Link]]:
        self.access_service.authorization.check_access(
            [AgentType.BOT, AgentType.USER]
        )

        restriction = self.access_service.authorization.access_restriction(
            resource_type=PermissionEntities.UNIT_IO
        )

        filters = UnitIOFilter.unlimited(unit_uuid=unit_uuid)

        filters.visibility_level = (
            self.access_service.authorization.get_available_visibility_levels(
                filters.visibility_level, restriction
            )
        )

        count, unit_ios = self.unit_io_repository.list(
            filters=filters, restriction=restriction
        )

        return count, self.link_repository.get_by_nodes(unit_ios)

    def check_data_pipe_preview_access(self) -> None:
        if not runtime.get().pu_ff_datapipe_enable:
            msg = "This feature has been disabled by the instance Owner"
            raise FeatureFlagError(msg, code=ErrorCode.FEATURE_DISABLED)

        self.access_service.authorization.check_access(
            [AgentType.BOT, AgentType.USER, AgentType.UNIT, AgentType.BACKEND]
        )

    def read_data_pipe_field(self, uuid: uuid_pkg.UUID) -> FieldDeclaration:
        unit_io = self.unit_io_repository.get(UnitIO(uuid=uuid))
        is_valid_object(unit_io, NotFoundEntity.UNIT_IO)
        self.access_service.authorization.check_visibility(unit_io)
        section = (
            FieldKey.INPUT
            if unit_io.type == UnitIOTypeEnum.INPUT
            else FieldKey.OUTPUT
        )
        return self.get_manifest_field(
            unit_io, section, *self.read_selected_manifest(unit_io.unit_uuid)
        )

    def get_data_pipe_config_options(
        self, uuid: uuid_pkg.UUID
    ) -> DataPipeConfigOptionsRead:
        self.check_data_pipe_preview_access()
        field = self.read_data_pipe_field(uuid)
        return DataPipeConfig.options_for_unit_io(field)

    async def check_data_pipe_config(
        self, data_pipe: UploadFile, *, uuid: uuid_pkg.UUID | None = None
    ) -> list[DataPipeValidationErrorRead]:
        self.check_data_pipe_preview_access()
        field = self.read_data_pipe_field(uuid) if uuid is not None else None
        data_pipe_dict = await yml_file_to_dict(data_pipe)
        return is_valid_data_pipe_config(data_pipe_dict, unit_io_field=field)

    async def set_data_pipe_config(
        self, uuid: uuid_pkg.UUID, data_pipe
    ) -> None:
        if not runtime.get().pu_ff_datapipe_enable:
            msg = "This feature has been disabled by the instance Owner"
            raise FeatureFlagError(msg, code=ErrorCode.FEATURE_DISABLED)

        self.access_service.authorization.check_access([AgentType.USER])

        unit_io = self.unit_io_repository.get(UnitIO(uuid=uuid))
        is_valid_object(unit_io, NotFoundEntity.UNIT_IO)

        self.access_service.authorization.check_ownership(
            unit_io, [OwnershipType.CREATOR]
        )

        self.is_valid_active_data_pipe(unit_io)

        data_pipe_dict = await yml_file_to_dict(data_pipe)
        data_pipe_entity = is_valid_data_pipe_config(
            data_pipe_dict, is_business_validator=True
        )

        unit_io.data_pipe_yml = json.dumps(
            data_pipe_entity.dict(), default=obj_serializer
        )
        unit_io = self.unit_io_repository.update(uuid, unit_io)

        await send_to_data_pipe_stream(
            DataPipeConfigAction.UPDATE, unit_io.model_dump()
        )

    def get_data_pipe_config(self, uuid: uuid_pkg.UUID) -> str:
        if not runtime.get().pu_ff_datapipe_enable:
            msg = "This feature has been disabled by the instance Owner"
            raise FeatureFlagError(msg, code=ErrorCode.FEATURE_DISABLED)

        self.access_service.authorization.check_access([AgentType.USER])

        unit_io = self.unit_io_repository.get(UnitIO(uuid=uuid))
        is_valid_object(unit_io, NotFoundEntity.UNIT_IO)

        self.is_valid_active_data_pipe(unit_io)
        self.is_valid_filled_config(unit_io)

        return dict_to_yml_file(
            remove_dict_none(
                is_valid_json(
                    unit_io.data_pipe_yml, JsonPayload.DATA_PIPE_CONFIG
                )
            )
        )

    def delete_data_pipe_data(self, uuid: uuid_pkg.UUID) -> None:
        if not runtime.get().pu_ff_datapipe_enable:
            msg = "This feature has been disabled by the instance Owner"
            raise FeatureFlagError(msg, code=ErrorCode.FEATURE_DISABLED)

        self.access_service.authorization.check_access([AgentType.USER])

        unit_io = self.unit_io_repository.get(UnitIO(uuid=uuid))
        is_valid_object(unit_io, NotFoundEntity.UNIT_IO)

        self.access_service.authorization.check_ownership(
            unit_io, [OwnershipType.CREATOR]
        )

        self.data_pipe_repository.bulk_delete([unit_io.uuid])

    def get_data_pipe_data(
        self, filters: DataPipeFilter
    ) -> tuple[int, list[NRecords | TimeWindow | Aggregation | LastValue]]:
        if not runtime.get().pu_ff_datapipe_enable:
            msg = "This feature has been disabled by the instance Owner"
            raise FeatureFlagError(msg, code=ErrorCode.FEATURE_DISABLED)

        self.access_service.authorization.check_access(
            [AgentType.USER, AgentType.UNIT]
        )

        unit_io = self.unit_io_repository.get(UnitIO(uuid=filters.uuid))
        is_valid_object(unit_io, NotFoundEntity.UNIT_IO)

        self.access_service.authorization.check_ownership(
            unit_io, [OwnershipType.CREATOR, OwnershipType.UNIT]
        )

        return self.data_pipe_repository.list(filters=filters)

    def get_data_pipe_data_csv(self, uuid: uuid_pkg.UUID) -> str:
        if not runtime.get().pu_ff_datapipe_enable:
            msg = "This feature has been disabled by the instance Owner"
            raise FeatureFlagError(msg, code=ErrorCode.FEATURE_DISABLED)

        self.access_service.authorization.check_access(
            [AgentType.USER, AgentType.UNIT]
        )

        unit_io = self.unit_io_repository.get(UnitIO(uuid=uuid))
        is_valid_object(unit_io, NotFoundEntity.UNIT_IO)

        self.access_service.authorization.check_ownership(
            unit_io, [OwnershipType.CREATOR, OwnershipType.UNIT]
        )

        self.is_valid_active_data_pipe(unit_io)
        self.is_valid_filled_config(unit_io)

        data_pipe_entity = is_valid_data_pipe_config(
            json.loads(unit_io.data_pipe_yml), is_business_validator=True
        )
        self.is_valid_policy(data_pipe_entity)

        count, data = self.data_pipe_repository.list(
            filters=DataPipeFilter.unlimited(
                uuid=uuid,
                type=data_pipe_entity.processing_policy.policy_type,
                order_by_create_date=OrderByDate.asc,
            )
        )

        return self.data_pipe_repository.models_to_csv(data)

    async def set_data_pipe_data_csv(
        self, uuid: uuid_pkg.UUID, data_csv
    ) -> None:
        if not runtime.get().pu_ff_datapipe_enable:
            msg = "This feature has been disabled by the instance Owner"
            raise FeatureFlagError(msg, code=ErrorCode.FEATURE_DISABLED)

        self.access_service.authorization.check_access([AgentType.USER])

        unit_io = self.unit_io_repository.get(UnitIO(uuid=uuid))
        is_valid_object(unit_io, NotFoundEntity.UNIT_IO)

        self.access_service.authorization.check_ownership(
            unit_io, [OwnershipType.CREATOR]
        )

        self.is_valid_active_data_pipe(unit_io)
        self.is_valid_filled_config(unit_io)

        data_pipe_entity = is_valid_data_pipe_config(
            json.loads(unit_io.data_pipe_yml), is_business_validator=True
        )
        self.is_valid_policy(data_pipe_entity)

        self.data_pipe_repository.bulk_delete([unit_io.uuid])

        async for batch in async_chunked(
            StreamingCSVValidator(data_pipe_entity).iter_validated_streaming(
                unit_io.uuid, data_csv
            ),
            5000,
        ):
            self.data_pipe_repository.bulk_create(
                data_pipe_entity.processing_policy.policy_type, batch
            )

    def delete_link(
        self, input_uuid: uuid_pkg.UUID, output_uuid: uuid_pkg.UUID
    ) -> None:
        self.access_service.authorization.check_access([AgentType.USER])

        link = self.link_repository.get_by_two_uuid(input_uuid, output_uuid)
        is_valid_object(link, NotFoundEntity.LINK)

        input_node = self.unit_io_repository.get(UnitIO(uuid=link.input_uuid))
        is_valid_object(input_node, NotFoundEntity.UNIT_IO)

        output_node = self.unit_io_repository.get(
            UnitIO(uuid=link.output_uuid)
        )
        is_valid_object(output_node, NotFoundEntity.UNIT_IO)

        if link.creator_uuid != self.access_service.current_agent.uuid:
            self.access_service.authorization.check_ownership(
                input_node, [OwnershipType.CREATOR]
            )
        else:
            self.access_service.authorization.check_ownership(
                link, [OwnershipType.CREATOR]
            )

        try:
            self.permission_service.delete(
                output_node.unit_uuid, input_uuid, is_api=False
            )
        except CustomException as ex:
            logging.info(ex)
            # At the time of deletion, the user may have already deleted access,
            # so there should be immunity to this error.

        self.link_repository.delete(link)

        with contextlib.suppress(MqttError):
            self.command_to_input_base_topic(
                uuid=input_node.unit_uuid,
                command=BackendTopicCommand.SCHEMA_UPDATE,
                is_auto_update=True,
            )

    def list(self, filters: UnitIOFilter) -> tuple[int, list[UnitIO]]:
        self.access_service.authorization.check_access(
            [AgentType.BOT, AgentType.USER, AgentType.UNIT]
        )
        restriction = self.access_service.authorization.access_restriction(
            resource_type=PermissionEntities.UNIT_IO
        )
        filters.visibility_level = (
            self.access_service.authorization.get_available_visibility_levels(
                filters.visibility_level, restriction
            )
        )

        return self.unit_io_repository.list(filters, restriction=restriction)

    def set_state(self, unit_io_uuid: uuid_pkg.UUID, state: str) -> UnitIO:
        unit_io = self.unit_io_repository.get(UnitIO(uuid=unit_io_uuid))
        is_valid_object(unit_io, NotFoundEntity.UNIT_IO)
        unit_io.state = state
        unit_io.last_update_datetime = datetime.datetime.now(datetime.UTC)

        return self.unit_io_repository.update(unit_io.uuid, unit_io)

    @staticmethod
    def is_valid_input_unit_io(unit_io: UnitIO) -> None:
        if unit_io.type != UnitIOTypeEnum.INPUT:
            msg = f"This Node {unit_io.uuid} is not Input"
            raise UnitIOError(msg, code=ErrorCode.UNIT_IO_NOT_INPUT)

    @staticmethod
    def is_valid_output_unit_io(unit_io: UnitIO) -> None:
        if unit_io.type != UnitIOTypeEnum.OUTPUT:
            msg = f"This Node {unit_io.uuid} is not Output"
            raise UnitIOError(msg, code=ErrorCode.UNIT_IO_NOT_OUTPUT)

    @staticmethod
    def is_valid_active_data_pipe(unit_io: UnitIO) -> None:
        if not unit_io.is_data_pipe_active:
            msg = "Data pipe is not active"
            raise DataPipeError(msg, code=ErrorCode.DATAPIPE_NOT_ACTIVE)

    @staticmethod
    def is_valid_filled_config(unit_io: UnitIO) -> None:
        if not unit_io.data_pipe_yml:
            msg = "Data pipe is not filled"
            raise DataPipeError(msg, code=ErrorCode.DATAPIPE_NOT_FILLED)

    @staticmethod
    def is_valid_policy(data_pipe_entity: DataPipeValidationErrorRead) -> None:
        if (
            data_pipe_entity.processing_policy.policy_type
            == ProcessingPolicyType.LAST_VALUE
        ):
            msg = "LastValue type is not supported on this function"
            raise DataPipeError(
                msg, code=ErrorCode.DATAPIPE_LAST_VALUE_UNSUPPORTED
            )
