from collections.abc import Sequence


class ReadmeMarkdown:
    def __init__(self) -> None:
        self.lines: list[str] = []

    def append_heading(self, title: str, level: int = 2) -> None:
        self.lines.extend((f"{'#' * level} {title}", ""))

    def append_table(
        self,
        headers: Sequence[str],
        rows: Sequence[Sequence[str]],
        title: str | None = None,
    ) -> None:
        if not rows:
            return
        if title:
            self.append_heading(title)
        self.lines.append(" | ".join(headers))
        self.lines.append(" | ".join("--" for _ in headers))
        self.lines.extend(" | ".join(row) for row in rows)
        self.lines.append("")

    def append_images(self, heading: str, urls: str | list[str]) -> None:
        self.lines.extend((heading, ""))
        images = urls if isinstance(urls, list) else [urls]
        self.lines.extend(
            f'<div align="center"><img align="center" src="{url}"></div>'
            for url in images
        )
        self.lines.append("")

    def append_links(
        self, title: str, items: Sequence[tuple[str, str]]
    ) -> None:
        if not items:
            return
        self.append_heading(title)
        self.lines.extend(
            f"{index}. [{name}]({url})"
            for index, (name, url) in enumerate(items, start=1)
        )
        self.lines.append("")

    def render(self) -> str:
        return "\n".join(self.lines).rstrip() + "\n"
