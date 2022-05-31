"""Swill in-built Handlers"""

import traceback
from ._connection import current_connection
from ._types import ErrorCode
from ._protocol import EncapsulatedRequest
from ._exceptions import Error
from ._serialize import serialize_error_response


async def handle_not_found(exception: BaseException, message: EncapsulatedRequest):
    traceback.print_exception(
        exception.__class__,
        exception,
        exception.__traceback__
    )

    await current_connection.get().send(
        serialize_error_response(
            code=ErrorCode.NOT_FOUND,
            seq=message.seq,
            message=str(exception)
        )
    )


async def handle_catch_all(exception: BaseException, message: EncapsulatedRequest):
    traceback.print_exception(
        exception.__class__,
        exception,
        exception.__traceback__
    )

    await current_connection.get().send(
        serialize_error_response(
            code=ErrorCode.INTERNAL_ERROR,
            seq=message.seq,
            message='An internal server error occurred'
        )
    )


async def handle_error(exception: Error, message: EncapsulatedRequest):
    traceback.print_exception(
        exception.__class__,
        exception,
        exception.__traceback__
    )

    await current_connection.get().send(
        serialize_error_response(
            code=exception.code,
            seq=message.seq,
            message=str(exception)
        )
    )
