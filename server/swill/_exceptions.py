import typing as t
from collections import defaultdict


class SwillException(Exception):
    pass


class SwillSerializationError(SwillException):
    pass


class SwillDeserializationError(SwillException):
    pass


class ValidationExceptionItem(t.NamedTuple):
    field: str
    index: t.Optional[t.Union[int, str]]
    exception: Exception


class ComplexValidationError(SwillException):
    def __init__(self, exceptions: t.List[Exception], data: t.Mapping[str, t.Any] = None):
        super().__init__(", ".join(str(exception) for exception in exceptions))
        self.data = dict(data or {})
        self.data["errors"] = []
        self.exceptions = exceptions
        for exception in exceptions:
            if isinstance(exception, (SwillValidationError, ComplexValidationError)):
                self.data["errors"].append(exception.data)
            else:
                self.data["errors"].append({"description": str(exception)})


class SwillValidationError(SwillException):
    def __init__(self, exceptions: t.List[ValidationExceptionItem]):
        super().__init__("Validation Error")
        self.code = 422
        self.data = {}
        for field, index, exception in exceptions:
            if not field:
                field = "*"

            if field not in self.data:
                self.data[field] = []

            if isinstance(exception, SwillValidationError):
                exc_data = dict(exception.data)
            elif isinstance(exception, ComplexValidationError):
                exc_data = dict(exception.data)
            else:
                exc_data = {"description": str(exception)}

            if index is not None:
                exc_data = {"index": index, "errors": [exc_data]}

            self.data[field].append(exc_data)

    def __str__(self):
        err = "\n".join([f"\t{item}: {self.data[item]}" for item in self.data])
        return f"{len(self.data)} validation errors:\n{err}"

    def __repr__(self):
        return f"SwillValidationError: {self.data}"


class SwillRequestError(SwillException):
    pass


class SwillRequestCancelled(SwillRequestError):
    pass


class SwillResponseError(SwillException):
    pass


class HandlerNotFound(SwillException):
    pass


class CloseConnection(SwillException):
    """CloseConnection will end the currently open Web-Socket connection if raised. Depending on when in the life
    cycle the exception is raised it will either return an http response (before the websocket is accepted) or
    close the websocket with a status code (after the websocket is accepted)

    The code parameter should be a valid WebSocket Status Code (RFC6455) OR a valid HTTP Response code.

    IF the connection is closed before the WebSocket handshake is complete then the http response code will be used.
    If the code is outside the range 0-999 it will return 403 instead.

    IF the connection is closed after the handshake then the WebSocket status code will be used.
    If the code is below 1000 then it will return the standard WebSocket status code 1000 instead.
    """

    def __init__(self, *, code: int = 1000, reason: str = ""):
        super().__init__()
        self.code = code
        self.reason = reason

    def __str__(self):
        return self.reason


class Error(SwillException):
    """A generic error that can be raised in your func"""

    def __init__(self, *, code: int = 400, message: str = ""):
        super().__init__(message)
        self.code = code
