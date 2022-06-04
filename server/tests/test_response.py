import pytest

from swill._protocol import ResponseType
from swill._response import Response
from swill import current_connection, SwillResponseError


def create_test_response(mocker):
    mocked_swill = mocker.MagicMock()
    mocked_swill._call_lifecycle_handlers = mocker.AsyncMock()

    mocked_request = mocker.MagicMock()
    mocked_request.seq = 1

    response = Response(mocked_swill, mocked_request)
    return response, mocked_swill, mocked_request


@pytest.mark.asyncio
async def test_set_leading_metadata(mocker):
    """Test setting leading metadata"""

    connection = mocker.MagicMock()
    current_connection.set(connection)
    response, swill, request = create_test_response(mocker)
    response.send_leading_metadata = mocker.AsyncMock(spec=response.send_leading_metadata)
    await response.set_leading_metadata({'key': 'value'}, send_immediately=False)
    response.send_leading_metadata.assert_not_called()
    assert response.leading_metadata == {'key': 'value'}


@pytest.mark.asyncio
async def test_set_leading_metadata_immediately(mocker):
    """Test setting send immediately """

    connection = mocker.MagicMock()
    current_connection.set(connection)
    response, swill, request = create_test_response(mocker)
    response.send_leading_metadata = mocker.AsyncMock(spec=response.send_leading_metadata)
    await response.set_leading_metadata({'key': 'value'})
    response.send_leading_metadata.assert_called()



@pytest.mark.asyncio
async def test_consume_metadata(mocker):
    """Test consuming leading metadata"""
    connection = mocker.MagicMock()
    current_connection.set(connection)
    response, swill, request = create_test_response(mocker)
    await response.set_leading_metadata({'key': 'value'}, send_immediately=False)
    assert await response.consume_leading_metadata() == {'key': 'value'}

    swill._call_lifecycle_handlers.assert_called_with(
        'before_leading_metadata',
        request,
        {'key': 'value'}
    )


@pytest.mark.asyncio
async def test_duplicate_consume(mocker):
    connection = mocker.MagicMock()
    current_connection.set(connection)
    response, swill, request = create_test_response(mocker)
    await response.set_leading_metadata({'key': 'value'}, send_immediately=False)
    assert await response.consume_leading_metadata() == {'key': 'value'}
    assert await response.consume_leading_metadata() == None


@pytest.mark.asyncio
async def test_send_leading_metadata(mocker):
    serializer = mocker.patch('swill._response.serialize_response')
    connection = mocker.MagicMock()
    connection.send = mocker.AsyncMock()

    current_connection.set(connection)
    response, swill, request = create_test_response(mocker)
    response.consume_leading_metadata = mocker.AsyncMock(return_value='XXX')
    await response.set_leading_metadata({'key': 'value'}, send_immediately=False)
    await response.send_leading_metadata()
    connection.send.assert_called()
    serializer.assert_called_with(
        seq=request.seq,
        type=ResponseType.METADATA,
        leading_metadata='XXX'
    )


@pytest.mark.asyncio
async def test_duplicate_send(mocker):
    connection = mocker.MagicMock()
    connection.send = mocker.AsyncMock()

    current_connection.set(connection)
    response, swill, request = create_test_response(mocker)
    await response.set_leading_metadata({'key': 'value'}, send_immediately=False)
    await response.send_leading_metadata()
    with pytest.raises(SwillResponseError):
        await response.send_leading_metadata()


@pytest.mark.asyncio
async def test_set_after_send(mocker):
    """Test setting metadata after it has been sent already"""

    connection = mocker.MagicMock()
    current_connection.set(connection)
    response, swill, request = create_test_response(mocker)
    response.send_leading_metadata = mocker.AsyncMock(spec=response.send_leading_metadata)
    await response.set_leading_metadata({'key': 'value'})
    response._leading_metadata_sent = True
    with pytest.raises(SwillResponseError):
        await response.set_leading_metadata({'key2': 'value2'})
