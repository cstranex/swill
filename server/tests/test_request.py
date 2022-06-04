"""Test BaseRequest and StreamingRequest Objects"""
import pytest

from swill import StreamingRequest
from swill._exceptions import SwillRequestError
from swill._protocol import EncapsulatedRequest, RequestType
from swill._request import Request, RequestReference


def create_single_request(mocker):

    swill = mocker.MagicMock()
    swill._call_lifecycle_handlers = mocker.AsyncMock()

    request = Request(
        swill,
        RequestReference(seq=1, rpc='test'),
        metadata={},
        message_type=int
    )

    return request, swill


def create_streaming_request(mocker, mock_queue=True):

    swill = mocker.MagicMock()
    swill._call_lifecycle_handlers = mocker.AsyncMock()

    request = StreamingRequest(
        swill,
        RequestReference(seq=1, rpc='test'),
        metadata={},
        message_type=int
    )
    if mock_queue:
        request._queue = mocker.MagicMock()

    return request, swill


@pytest.mark.asyncio
async def test_process_message_single_request(mocker):
    """Test processing a single message"""
    request, swill = create_single_request(mocker)

    await request.process_message(EncapsulatedRequest(
        seq=1,
        rpc='test',
        data=b'\x01',
    ))
    assert request.data == 1
    swill._call_lifecycle_handlers.assert_called_with(
        'before_request_message',
        request,
        1,
    )


@pytest.mark.asyncio
async def test_process_message_single_cancel(mocker):
    request, swill = create_single_request(mocker)
    await request.process_message(EncapsulatedRequest(
        seq=1,
        rpc='test',
        type=RequestType.CANCEL,
        data=None
    ))
    assert request.cancelled is True
    swill._call_lifecycle_handlers.assert_not_called()


@pytest.mark.asyncio
async def test_process_message_single_unknown(mocker):
    request, swill = create_single_request(mocker)
    swill._call_lifecycle_handlers = mocker.AsyncMock()
    request.cancel = mocker.MagicMock()

    with pytest.raises(SwillRequestError):
        await request.process_message(EncapsulatedRequest(
            seq=1,
            rpc='test',
            type=RequestType.END_OF_STREAM,
            data=None
        ))


@pytest.mark.asyncio
async def test_request_metadata(mocker):
    swill = mocker.MagicMock()
    request = Request(
        swill,
        RequestReference(seq=1, rpc='test'),
        metadata={'key': 'value'},
        message_type=int
    )
    assert request.metadata == {'key': 'value'}


@pytest.mark.asyncio
async def test_process_message_streaming(mocker):
    request, swill = create_streaming_request(mocker)

    await request.process_message(EncapsulatedRequest(
        seq=1,
        rpc='test',
        type=RequestType.MESSAGE,
        data=b'\x01'
    ))
    request._queue.add.assert_called_with(1)
    swill._call_lifecycle_handlers.assert_called_with(
        'before_request_message',
        request,
        1,
    )


@pytest.mark.asyncio
async def test_process_message_metadata(mocker):
    request, swill = create_streaming_request(mocker)

    await request.process_message(EncapsulatedRequest(
        seq=1,
        rpc='test',
        type=RequestType.METADATA,
        metadata={'key': 'value'},
        data=None,
    ))
    assert request.metadata == {'key': 'value'}


@pytest.mark.asyncio
async def test_process_message_duplicate_metadata(mocker):
    request, swill = create_streaming_request(mocker)

    await request.process_message(EncapsulatedRequest(
        seq=1,
        rpc='test',
        type=RequestType.METADATA,
        metadata={'key': 'value'},
        data=None,
    ))
    assert request.metadata == {'key': 'value'}

    with pytest.raises(SwillRequestError):
        await request.process_message(EncapsulatedRequest(
            seq=1,
            rpc='test',
            type=RequestType.METADATA,
            metadata={'key2': 'value2'},
            data=None,
        ))
