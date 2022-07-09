# Swill Client Library for Typescript

Swill is a Python-based RPC web framework that uses WebSockets and MsgPack for communication.


After making a request we are left with a RpcCall object

```typescript

// Single Requests
let request = await client.rpc('add', [1, 2]);
let message = await request.receive();
/*
 {
    type: ResponseType.MESSAGE,
    data: 3
 }
*/
request.close();


// Streaming Requests and Responses
request = await client.rpc('multiply', [2]);
request.hasData(); // Returns false
request.send(2);
// ...
request.hasData(); // Returns true
message = await request.receive();
/*
 {
    type: ResponseType.MESSAGE,
    data: 4
 }
*/
request.send(4);
message = await request.receive();
/*
 {
    type: ResponseType.MESSAGE,
    data: 16
 }
*/
request.endStream(); // Notify the server that we're done




```

