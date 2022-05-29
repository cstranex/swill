# Swill - Asynchronous WebSocket + MsgPack RPC Framework for Python

Swill is an ASGI framework for creating RPC backends that use Websockets for communication
and Msgpack for serialization.

**This project is still in alpha phase. The underlying protocol and implementations may still change**

## Quick Example

```python
# Server
import swill.app
from typing import Tuple
import swill

app = swill.app.Swill(__name__)


@app.handle()
def add(request: swill.Request[Tuple[int, int]]) -> int:
   return request.data[0] + request.data[1]
```


```typescript
// Generic Client
import {Client} from '@swill/swill';

const client = new Client();
await client.connect('localhost');
const result = await client.call('add', [1, 2]);
console.log(result); // 3
```

## Lifecycle
There are two different lifecycles in Swill - The Connection lifecycle and the Request lifecycle.

### Connection Lifecycle
Connections are created when a WebSocket is accepted and contain some details about the initial
HTTP request from the client (eg: Cookies, headers, etc.). A connection has a unique id and
persists for the life of the WebSocket.

### Request Lifecycle
A request lifecycle starts when the client sends a WebSocket message to the server and ends
when the server finalises the request by sending either a response (for single requests), an
`END_OF_STREAM` message for streaming requests or an `ERROR` message. Requests are always
attached to one connection.


## Features

### Message Validation
[msgspec](https://jcristharif.com/msgspec/) is used to perform serialization and validation
on messages being sent and received. 

### Typing Annotations
Typing annotations are used in conjunction with `msgspec` to determine how handlers process
messages. Handlers can handle single or streaming requests and responses. 

### Streaming Requests
Handlers can accept streaming requests where the client sends multiple messages for
processing. Streaming requests provide an async iterator to handle the messages. When a
client is finished streaming it sends a special `END_OF_STREAM` message.

### Streaming Responses
Handlers can stream responses by yielding multiple messages. When a handler returns it
sends the client a special `END_OF_STREAM` message and (optionally) trailing metadata.
Clients can also cancel streaming responses by sending a special `CLOSE` message.

### Errors
Errors are their own message type that contain some pre-defined error codes while also
allowing arbitrary data.

### Metadata
Both clients and server can send metadata for a specific request. Metadata are arbitrary
key/value pairs that can be used to send data alongside a message.

#### Client Metadata
Client metadata is sent with the initial request. For streaming requests the metadata
can be sent on its own too.

#### Leading Metadata
The server can send leading metadata with the initial response or immediately before
sending any data regardless if it is a streaming or single handler. It cannot send
it more than once and cannot send metadata with any subsequent messages.

#### Trailing Metadata
Trailing metadata can only be sent with the `END_OF_MESSAGE` message during streaming
responses or with the response of a single response.

### Lifecycle Handlers
At each stage of the connection and request lifecycles, handlers can be invoked which
receive data such as connection information or request information. These can be used
to modify the connection or request as needed. Currently supported are (in order):
`before_connection`, `before_accept`, `before_request`, `before_request_metadata`,
`before_request_data`, `before_request_message`, `before_leading_metadata`,
`before_response_message`, `before_trailing_metadata`, `after_request`, `after_connection`

## TODO
 - [ ] Typescript Client
 - [ ] Auto-Generated Client Libraries for Typescript
 - [ ] WebSocket Implementation
   - [ ] Ping/Pong 
 - [X] Streaming requests
 - [X] Streaming responses
 - [X] Leading and trailing metadata
 - [X] Exception Handling
   - [X] Tracebacks
 - [ ] Connection Management
 - [ ] Session Management
   - [ ] Redis Backend
 - [ ] PubSub Plugin
 - [ ] Logging Plugin
 - [ ] Modules (ie: Flask Blueprints)
 - [ ] Static File Serving
