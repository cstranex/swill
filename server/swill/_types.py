import enum
from typing import Awaitable, Callable, AsyncIterator, Dict, Any, Optional, Union
from msgspec import Struct

Handler = Callable[[Any], Union[AsyncIterator[Any], Awaitable[Any]]]
StreamingResponse = AsyncIterator
Metadata = Dict[str, Any]


class ErrorMessage(Struct):
    code: int
    message: str
    data: Optional[Any] = None


class MethodType(enum.IntEnum):
    SINGLE = 0
    STREAM = 1


class ErrorCode(enum.IntEnum):
    """Pre-defined error codes that your application may make use of. Similar to
    HTTP Error Codes"""

    BAD_REQUEST = 400
    UNAUTHORIZED = 401
    NOT_FOUND = 404
    FORBIDDEN = 403

    INVALID_RPC = 501
    INTERNAL_ERROR = 500



