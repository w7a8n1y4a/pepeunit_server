import json
import uuid
import uuid as uuid_pkg
from typing import Annotated

import toml
import yaml
from fastapi import Depends
from fastapi.security import APIKeyHeader
from starlette.datastructures import UploadFile as StarletteUploadFile

from app import settings
from app.configs.error_codes import ErrorCode
from app.configs.errors import ReadmeGenerationError
from app.dto.enum import GlobalPrefixTopic, VisibilityLevel
from app.services.readme_markdown import ReadmeMarkdown
from app.validators.content_size import is_valid_content_size


def token_depends(
    jwt_token: Annotated[
        str | None,
        Depends(APIKeyHeader(name="x-auth-token", auto_error=False)),
    ] = None,
):
    return jwt_token


def merge_two_dict_first_priority(first: dict, two: dict) -> dict:
    return {**two, **first}


def remove_none_value_dict(data: dict) -> dict:
    return {k: v for k, v in data.items() if v is not None}


def get_topic_name(io_uuid: uuid_pkg.UUID, topic_name: str):
    main_topic = f"{settings.pu_domain}/{io_uuid}"
    main_topic += (
        GlobalPrefixTopic.BACKEND_SUB_PREFIX
        if topic_name[-len(GlobalPrefixTopic.BACKEND_SUB_PREFIX) :]
        == GlobalPrefixTopic.BACKEND_SUB_PREFIX
        else ""
    )

    return main_topic


def get_visibility_level_priority(visibility_level: VisibilityLevel) -> int:
    priority_dict = {
        VisibilityLevel.PUBLIC: 0,
        VisibilityLevel.INTERNAL: 1,
        VisibilityLevel.PRIVATE: 2,
    }

    return priority_dict[visibility_level]


async def yml_file_to_dict(yml_file) -> dict:
    content = ""
    if isinstance(yml_file, StarletteUploadFile):
        content = await yml_file.read()

    if isinstance(content, bytes):
        content = content.decode("utf-8")

    return yaml.safe_load(content)


def dict_to_yml_file(yml_dict: dict) -> str:
    yaml_content = yaml.safe_dump(
        yml_dict, allow_unicode=True, default_flow_style=False, sort_keys=False
    )

    filename = f"tmp/data_pipe_yml_{uuid.uuid4()}"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(yaml_content)

    return filename


class TomlToMdConverter(ReadmeMarkdown):
    MAX_SOURCE_CHARACTERS = 10000

    def __init__(self, data: dict) -> None:
        super().__init__()
        self.general = data.get("general") or {}
        self.video = data.get("video") or {}
        self.images = data.get("images") or {}
        self.models = data.get("models") or {}
        self.files = data.get("files") or {}
        self.physical_io = data.get("physical_io") or {}
        self.env_description = data.get("env_description") or {}
        self.topic_assignment = data.get("topic_assignment") or {}
        self.work_algorithm = data.get("work_algorithm") or {}
        self.installation = data.get("installation")

    def generate(self) -> str:
        self._validate_general()
        self._append_general_section()
        self._append_video_section()
        self._append_images_section()
        self._append_models_section()
        self._append_files_section()
        self._append_physical_io_section()
        self._append_env_description_section()
        self._append_topic_assignment_section()
        self._append_work_algorithm_section()
        self._append_installation_section()

        return self.render()

    def _validate_general(self) -> None:
        # String length limits
        str_limits = {
            "name": 50,
            "description": 400,
            "language": 50,
            "version": 30,
            "license": 50,
        }
        for key, max_len in str_limits.items():
            raw_val = self.general.get(key)
            if isinstance(raw_val, str):
                val = raw_val.strip()
                if len(val) > max_len:
                    msg = f'"general.{key}" is too long ({len(val)}) – max {max_len} characters'
                    raise ReadmeGenerationError(
                        msg, code=ErrorCode.README_FIELD_TOO_LONG
                    )

        # List size limits
        list_limits = {
            "hardware": 30,
            "firmware": 30,
            "stack": 30,
            "authors": 30,
        }
        for key, max_items in list_limits.items():
            raw_val = self.general.get(key)
            if isinstance(raw_val, list) and len(raw_val) > max_items:
                msg = f'"general.{key}" has too many items ({len(raw_val)}) – max {max_items}'
                raise ReadmeGenerationError(
                    msg, code=ErrorCode.README_FIELD_TOO_MANY_ITEMS
                )

    def _fmt_mixed_list(self, items):
        formatted = []
        for item in items:
            if isinstance(item, dict):
                name = (item.get("name") or "").strip()
                link = (item.get("link") or "").strip()
                if name and link:
                    formatted.append(f"[{name}]({link})")
                elif link:
                    formatted.append(f"[{link}]({link})")
                elif name:
                    formatted.append(name)
                else:
                    formatted.append(json.dumps(item, ensure_ascii=False))
            else:
                formatted.append(f"`{item}`")
        return ", ".join(formatted)

    def _label_for_general_key(self, key: str) -> str:
        mapping = {
            "description": "Description",
            "language": "Lang",
            "hardware": "Hardware",
            "firmware": "Firmware",
            "stack": "Stack",
            "version": "Version",
            "license": "License",
            "authors": "Authors",
        }
        return mapping.get(key, key)

    def _format_general_value(self, key: str, value):
        if value is None:
            return None

        if key in ("hardware", "firmware", "stack"):
            return self._format_hw_fw_stack_value(value)

        if key == "language":
            return self._format_language_value(value)

        if key == "authors":
            return self._format_authors_value(value)

        return self._format_default_general_value(value)

    def _format_hw_fw_stack_value(self, value):
        if not isinstance(value, list) or not value:
            return None
        return self._fmt_mixed_list(value)

    def _format_language_value(self, value):
        if not isinstance(value, str):
            return None
        v = value.strip()
        return f"`{v}`" if v else None

    def _format_authors_value(self, value):
        if not isinstance(value, list) or not value:
            return None
        authors = []
        for author in value:
            if not isinstance(author, dict):
                continue
            name = (author.get("name") or "").strip()
            email = (author.get("email") or "").strip()
            if name and email:
                authors.append(f"{name} <{email}>")
            elif name:
                authors.append(name)
            elif email:
                authors.append(f"<{email}>")
        if not authors:
            return None
        return ", ".join(authors)

    def _format_default_general_value(self, value):
        if isinstance(value, str):
            v = value.strip()
            return v or None
        return str(value)

    def _append_general_section(self) -> None:
        raw_name = self.general.get("name")
        if isinstance(raw_name, str):
            name = raw_name.strip()
            if name:
                self.append_heading(name, level=1)

        table_rows: list[tuple[str, str]] = []
        for key, value in self.general.items():
            formatted_value = self._format_general_value(key, value)
            if formatted_value is None or key == "name":
                continue
            label = self._label_for_general_key(key)
            table_rows.append((label, formatted_value))

        self.append_table(("Parameter", "Implementation"), table_rows)

    def _append_video_section(self) -> None:
        video = self.video.get("video")
        if not video:
            return

        if isinstance(video, list):
            video = video[0] if video else None
        if not isinstance(video, dict):
            return

        preview = (video.get("preview") or "").strip()
        link = (video.get("link") or "").strip()
        if not preview or not link:
            return

        self.lines.append("## Video")
        self.lines.append("")
        self.lines.append(f"[![video]({preview})]({link})")
        self.lines.append("")

    def _append_images_section(self) -> None:
        for title, urls in self.images.items():
            heading = (str(title) or "").strip()
            if not heading:
                continue
            if not heading.startswith("#"):
                heading = "## " + heading.capitalize()
            self.append_images(heading, urls)

    def _append_models_section(self) -> None:
        models = self.models.get("models")
        if not isinstance(models, list) or not models:
            return

        rows: list[tuple[str, str, str]] = []
        for item in models:
            if not isinstance(item, dict):
                continue
            version = (item.get("version") or "").strip()
            description = (item.get("description") or "").strip()
            link = (item.get("link") or "").strip()
            if not link:
                continue
            rows.append((version, description, f"[link]({link})"))

        self.append_table(
            ("Version", "Description", "Link"), rows, title="Models"
        )

    def _append_files_section(self) -> None:
        if not self.files:
            return

        items = []
        for name, url in self.files.items():
            name_str = (str(name) or "").strip()
            url_str = (str(url) or "").strip()
            if not url_str:
                continue
            if not name_str:
                name_str = url_str
            items.append((name_str, url_str))

        self.append_links("Files", items)

    def _append_physical_io_section(self) -> None:
        rows = [
            (f"`{key}`", str(desc)) for key, desc in self.physical_io.items()
        ]
        self.append_table(("Key", "Description"), rows, title="Physical IO")

    def _append_env_description_section(self) -> None:
        rows = [
            (f"`{key}`", str(desc))
            for key, desc in self.env_description.items()
        ]
        self.append_table(
            ("Variable", "Description"), rows, title="Env variable assignment"
        )

    def _append_topic_assignment_section(self) -> None:
        rows = [
            (f"`{topic}`", str(desc))
            for topic, desc in self.topic_assignment.items()
        ]
        self.append_table(
            ("Topic", "Description"), rows, title="Assignment of Device Topics"
        )

    def _append_work_algorithm_section(self) -> None:
        self.append_steps(
            "Work algorithm", self.work_algorithm.get("steps") or []
        )

    def _append_installation_section(self) -> None:
        steps = (
            self.installation.get("steps") or []
            if isinstance(self.installation, dict)
            else []
        )
        self.append_steps("Installation", steps)


def toml_to_md(data: dict) -> str:
    converter = TomlToMdConverter(data)
    return converter.generate()


async def toml_file_to_md(toml_file) -> str:
    content: str | bytes = ""
    if isinstance(toml_file, StarletteUploadFile):
        content = await toml_file.read()

    if isinstance(content, bytes):
        content = content.decode("utf-8")

    is_valid_content_size(
        content,
        TomlToMdConverter.MAX_SOURCE_CHARACTERS,
        code=ErrorCode.README_TOML_TOO_LARGE,
        message=f"toml is too large ({len(content)}) limit {TomlToMdConverter.MAX_SOURCE_CHARACTERS} characters",
        error_type=ReadmeGenerationError,
    )

    try:
        data = toml.loads(content)
    except Exception as e:
        msg = f"Error toml deserialization: {e}"
        raise ReadmeGenerationError(
            msg, code=ErrorCode.README_TOML_INVALID
        ) from None

    return toml_to_md(data)
