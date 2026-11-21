from app.configs.error_codes import ErrorCode
from app.configs.errors import CustomException, ValidationError


def is_valid_content_size(
    content: str | bytes,
    max_size: int,
    *,
    code: ErrorCode,
    message: str,
    error_type: type[CustomException] = ValidationError,
) -> None:
    if len(content) <= max_size:
        return
    raise error_type(message, code=code)
