import typing as t
from collections import defaultdict


class SwillException(Exception):
    pass


class SwillSerializationError(SwillException):
    pass


class SwillDeserializationError(SwillException):
    pass


class SwillValidationError(SwillException):
    def __init__(
        self, exceptions: t.List[t.Tuple[t.List[str], t.Union[ValueError, TypeError]]]
    ):
        super().__init__("Validation error")
        self.code = 422
        self.data = {}
        for fields, exception in exceptions:
            if not fields:
                fields = ["*"]
            for field in fields:
                if field not in self.data:
                    self.data[field] = []
                if isinstance(exception, SwillValidationError):
                    self.data[field].append(exception.data)
                else:
                    self.data[field].append({"description": str(exception)})

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
