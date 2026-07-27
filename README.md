# bare-rpc-python

Pure-Python port of the [bare-rpc](https://github.com/holepunchto/bare-rpc) message/frame wire codec and async RPC runtime, wire-compatible with the JavaScript reference. Built on [compact-encoding-python](https://github.com/holepunchto/compact-encoding-python).

Typically you don't call this directly - [hrpc-python](https://github.com/holepunchto/hrpc-python) generates a typed client/server on top of it. Use it directly when you want the raw transport.

## Install

```sh
pip install git+https://github.com/holepunchto/bare-rpc-python
```

## Wire codec

Encode and decode frames without the runtime:

```python
import bare_rpc as rpc

frame = rpc.encode_request(1, 42, data=b"hi")  # -> bytes
msg = rpc.decode_frame(frame)  # -> RequestMessage(id=1, command=42, ...)
```

Encoders: `encode_request`, `encode_response`, `encode_error_response`, `encode_event`, `encode_stream`. `decode_frame` returns a `RequestMessage`, `ResponseMessage`, or `StreamMessage`. `Type` and `StreamFlag` are the wire enums.

## RPC runtime

`RPC` is transport-agnostic: give it a `send` callback for outgoing frames and feed it inbound bytes with `receive`. Wire those to an asyncio stream, a websocket, or an in-memory pipe.

```python
import bare_rpc as rpc


async def on_request(req):
    await req.reply(b"pong")


server = rpc.RPC(send=my_send, on_request=on_request)
client = rpc.RPC(send=my_other_send)

await server.receive(incoming_bytes)  # feed inbound frames in

result = await client.request(command=5, data=b"ping")  # -> b"pong"
await client.event(command=3, data=b"note")  # fire-and-forget, no response
```

Constructor: `RPC(send, *, on_request=None, on_event=None, on_error=None, max_frame_size=...)`. `on_request` receives an `IncomingRequest` (`.command`, `.data`, `await .reply(data)`, `await .reject(error)`); `on_event` receives an `IncomingEvent`. `close()` rejects all in-flight requests.

## Streams and backpressure

All three streaming shapes are implemented, with cork/uncork backpressure. An `OutgoingStream` has `await write(data)`, `await end()`, and `await destroy(error=None)`; an `IncomingStream` is async-iterable.

Response stream (server streams back, client reads):

```python
async def on_request(req):
    out = await req.create_response_stream()
    await out.write(b"chunk-1")
    await out.write(b"chunk-2")
    await out.end()


incoming = await client.request_with_response_stream(command=5, data=b"go")
async for chunk in incoming:
    ...
```

Request stream (client streams up, server reads, then replies):

```python
async def on_request(req):
    async for chunk in req.request_stream:
        ...
    await req.reply(b"done")


out, reply = await client.stream_request(command=6)
await out.write(b"chunk-1")
await out.end()
result = await reply  # -> b"done"
```

Duplex (both directions):

```python
out, incoming = await client.create_bidirectional_stream(command=7)
await out.write(b"ping")
await out.end()
async for chunk in incoming:
    ...
```

## Errors

A rejected request raises `RPCRemoteError` (`.message`, `.code`, `.errno`) on the caller. Pass one to `await req.reject(...)` to carry a specific code across the wire; any other exception is rejected with its string message.

## License

Apache-2.0
