from typing import Literal

import yaml
from pydantic import BaseModel, ValidationError
from pydantic_core import InitErrorDetails, PydanticCustomError
from yaml.nodes import MappingNode, Node


class YamlMappingError(yaml.MarkedYAMLError):
    def __init__(self, message: str, node: Node) -> None:
        super().__init__(problem=message, problem_mark=node.start_mark)


class YamlDocumentLoader(yaml.SafeLoader):
    @classmethod
    def read_document(cls, content: str | bytes) -> object:
        loader = cls(content)
        try:
            return loader.get_single_data()
        finally:
            loader.dispose()

    def construct_mapping(
        self, node: Node, deep: bool = False
    ) -> dict[str, object]:
        if not isinstance(node, MappingNode):
            msg = "Expected a YAML mapping"
            raise YamlMappingError(msg, node)
        self.flatten_mapping(node)
        mapping = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            if not isinstance(key, str):
                msg = "Expected a string key"
                raise YamlMappingError(msg, key_node)
            if key in mapping:
                msg = "Duplicate YAML key"
                raise YamlMappingError(msg, key_node)
            mapping[key] = self.construct_object(value_node, deep=deep)
        return mapping


class BaseYamlRepository:
    @staticmethod
    def validate_model[T: BaseModel](
        model: type[T],
        document: object,
        *,
        extra: Literal["forbid", "ignore"] = "forbid",
    ) -> T:
        try:
            return model.model_validate(document, strict=True, extra=extra)
        except ValidationError as err:
            issues = []
            for item in err.errors(include_input=False):
                error = PydanticCustomError(
                    item["type"], item["msg"], item.get("ctx")
                )
                issues.append(
                    InitErrorDetails(type=error, loc=item["loc"], input=None)
                )
            raise ValidationError.from_exception_data(
                err.title, issues, hide_input=True
            ) from None

    @staticmethod
    def load_yaml(content: str | bytes, model: type[BaseModel]) -> object:
        try:
            return YamlDocumentLoader.read_document(content)
        except (
            yaml.YAMLError,
            AttributeError,
            LookupError,
            TypeError,
            ValueError,
            OverflowError,
            RecursionError,
        ) as err:
            message = "Invalid YAML document"
            if isinstance(err, yaml.MarkedYAMLError) and err.problem_mark:
                mark = err.problem_mark
                position = f"line {mark.line + 1}, column {mark.column + 1}"
                reason = (
                    err.problem
                    if isinstance(err, YamlMappingError)
                    else "Invalid YAML"
                )
                message = f"{reason} at {position}"
            issue = InitErrorDetails(
                type="value_error", input=None, ctx={"error": message}
            )
            raise ValidationError.from_exception_data(
                model.__name__, [issue], hide_input=True
            ) from None
