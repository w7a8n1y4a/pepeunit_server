import json

from app.dto.enum import FieldKey, ManifestDocKey
from app.schemas.pydantic.unit_manifest import UnitDefinition
from app.services.readme_markdown import ReadmeMarkdown


class UnitManifestReadme(ReadmeMarkdown):
    def __init__(self, unit: UnitDefinition) -> None:
        super().__init__()
        self.unit = unit

    def generate(self) -> str:
        search = self.unit.search
        self.append_heading(search.name, level=1)
        self.lines.extend((search.description.rstrip(), ""))
        self.append_table(
            ("Parameter", "Implementation"),
            [
                ("Type", search.type),
                ("Execution type", search.execution_type),
                ("Platforms", ", ".join(search.platforms)),
                ("Contract version", self.unit.contract_version),
            ],
        )
        docs = self.unit.docs
        if video := docs.get(ManifestDocKey.VIDEO):
            self.append_heading("Video")
            link = video[ManifestDocKey.LINK]
            preview = video.get(ManifestDocKey.PREVIEW)
            self.lines.extend(
                (f"[![Video]({preview})]({link})" if preview else link, "")
            )
        if schema := docs.get(ManifestDocKey.SCHEMA):
            self.append_images("## Schema", schema[ManifestDocKey.LINK])
        for title, url in docs.get(ManifestDocKey.IMAGES, {}).items():
            self.append_images(f"## {title}", url)
        self.append_links(
            "Models", list(docs.get(ManifestDocKey.MODELS, {}).items())
        )
        for key, title in (
            (ManifestDocKey.ALGORITHM, "Work algorithm"),
            (ManifestDocKey.INSTALLATION, "Installation"),
        ):
            if text := docs.get(key):
                self.append_heading(title)
                self.lines.extend((text.rstrip(), ""))
        for section, title in (
            (FieldKey.ENV, "Environment"),
            (FieldKey.INPUT, "Input"),
            (FieldKey.OUTPUT, "Output"),
        ):
            declarations = getattr(self.unit, section)
            if not declarations:
                continue
            self.append_heading(title)
            self.lines.extend(
                (
                    "```json",
                    json.dumps(declarations, ensure_ascii=False, indent=2),
                    "```",
                    "",
                )
            )
        return self.render()
