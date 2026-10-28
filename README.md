# bare-rpc-python

RPC over any byte transport for Python. The Python counterpart of [bare-rpc](https://github.com/holepunchto/bare-rpc), and wire compatible with it. Built on [compact-encoding-python](https://github.com/holepunchto/compact-encoding-python).

```
pip install bare-rpc
```

[hrpc-python](https://github.com/holepunchto/hrpc-python) generates typed clients and servers on top of it. Use it directly for the raw transport.

## Wire codec

Encode and decode frames without the runtime:

```python
import bare_rpc as rpc

frame = rpc.encode_request(1, 42, data=b"hi")  # -> bytes
msg = rpc.decode_frame(frame)  # -> RequestMessage(id=1, command=42, ...)
```

## RPC runtime

`RPC` runs a session over a `Transport`: Any object with an async `read()`, returning `b""` once it has ended, an async `write(data)`, and `close()`. A `Sidecar` from [bare-sidecar-python](https://github.com/holepunchto/bare-sidecar-python) is one:

```python
import asyncio

from bare_rpc import RPC
from bare_sidecar import spawn


async def main():
    sidecar = await spawn("app.js")

    async with RPC(sidecar) as rpc:
        print(await rpc.request(1, b"ping"))

        await rpc.event(2, b"note")


asyncio.run(main())
```

Where `app.js` serves the requests with [bare-rpc](https://github.com/holepunchto/bare-rpc):

```js
const RPC = require("bare-rpc");

new RPC(Bare.IPC, (req) => req.reply("pong"));
```

For a socket or any other asyncio stream, wrap its reader and writer in a `StreamTransport`:

```python
import asyncio

from bare_rpc import RPC, StreamTransport


async def main():
    reader, writer = await asyncio.open_connection("localhost", 8080)

    async with RPC(StreamTransport(reader, writer)) as rpc:
        print(await rpc.request(1, b"ping"))
```

The session closes once the transport ends, and closing it closes the transport. Pass `on_request` and `on_event` to answer requests and receive events from the peer:

```python
async def on_request(req):
    await req.reply(b"pong")


rpc = RPC(transport, on_request=on_request)
```

## Command routing

`CommandRouter` sends each request and event to the handler registered for its command. Pass its `dispatch_request` and `dispatch_event` to `RPC`:

```python
router = rpc.CommandRouter()


async def ping(req):
    return b"pong"


router.on_request(5, ping)
router.on_event(3, lambda event: print(event.data))

server = rpc.RPC(
    transport,
    on_request=router.dispatch_request,
    on_event=router.dispatch_event,
)
```

## Streams and backpressure

All three streaming shapes are implemented, with backpressure.

Incoming streams are read with `async for`, or a chunk at a time with `await stream.read()`, which returns `None` once the peer has ended the stream.

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

## License

Apache-2.0
