import contextvars
import enum
import typing as t
from msgspec import Struct

Handler = t.Callable[[t.Any], t.Union[t.AsyncIterator[t.Any], t.Awaitable[t.Any]]]
StreamingResponse = t.AsyncIterator
Metadata = t.Dict[str, t.Any]


class ErrorMessage(Struct, omit_defaults=True):
    code: int
    message: str
    data: t.Optional[t.Any] = None


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


T = t.TypeVar("T")


class ContextVarType(t.Generic[T]):
    def set(self, value: T) -> contextvars.Token[T]:
        ...

    def get(self) -> T:
        ...

    def reset(self, token: contextvars.Token[T]):
        ...
