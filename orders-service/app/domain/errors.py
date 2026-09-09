from http import HTTPStatus
from typing import ClassVar


class DomainError(Exception):
    code: ClassVar[str] = "domain_error"
    status: ClassVar[HTTPStatus] = HTTPStatus.BAD_REQUEST

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class ValidationError(DomainError):
    code = "validation_error"
    status = HTTPStatus.UNPROCESSABLE_ENTITY


class NotFoundError(DomainError):
    code = "not_found"
    status = HTTPStatus.NOT_FOUND


class ConflictError(DomainError):
    code = "conflict"
    status = HTTPStatus.CONFLICT
