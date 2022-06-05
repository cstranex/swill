import typing as t
import contextvars

from ._connection import current_connection
from ._types import Metadata, ContextVarType
from ._protocol import ResponseType
from ._serialize import serialize_response
from ._exceptions import SwillResponseError
from ._request import BaseRequest


class Response:
    """The response object allows setting leading and trailing metadata for a request"""

    _leading_metadata = None
    _leading_metadata_sent = False
    _trailing_metadata = None

    def __init__(self, swill: "Swill", request: BaseRequest):
        self._swill = swill
        self._request = request

    async def send_leading_metadata(self):
        """Send leading metadata. Leading metadata can only be sent once and must be sent with
        the first response"""

        if self._leading_metadata_sent:
            raise SwillResponseError(
                "Leading metadata has already been sent for this request"
            )

        await current_connection.get().send(
            serialize_response(
                seq=self._request.seq,
                type=ResponseType.METADATA,
                leading_metadata=await self.consume_leading_metadata(),
            )
        )

    async def set_leading_metadata(self, metadata: Metadata, send_immediately=True):
        """Set leading metadata for this request. This can only be set once"""

        if self.leading_metadata_sent:
            raise SwillResponseError("Metadata has already been sent for this request")

        self._leading_metadata = metadata
        if send_immediately:
            await self.send_leading_metadata()

    def set_trailing_metadata(self, metadata: Metadata):
        """Set the trailing metadata"""
        self._trailing_metadata = metadata

    async def consume_leading_metadata(self):
        """Read the leading metadata. If it has been sent then return None"""

        if self._leading_metadata_sent:
            return

        await self._swill._call_lifecycle_handlers(
            "before_leading_metadata", self._request, self._leading_metadata
        )

        self._leading_metadata_sent = True
        return self._leading_metadata

    @property
    def leading_metadata_sent(self):
        """Return if the leading metadata has already been sent"""
        return self._leading_metadata_sent

    @property
    def leading_metadata(self):
        """Return the leading metadata if there is any or None"""
        return self._leading_metadata

    @property
    def trailing_metadata(self):
        """Return trailing metadata if there is any or None"""
        return self._trailing_metadata

    def __repr__(self):
        return f"<Response {self._request.reference}>"


current_response = t.cast(
    ContextVarType[Response], contextvars.ContextVar("current_response")
)
