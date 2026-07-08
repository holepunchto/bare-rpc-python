# bare-rpc-python

Pure-Python port of the [bare-rpc](https://github.com/holepunchto/bare-rpc) message/frame wire codec and async RPC runtime, wire-compatible with the JavaScript reference. Built on [compact-encoding-python](https://github.com/holepunchto/compact-encoding-python).

## Usage

```python
import bare_rpc as rpc

frame = rpc.encode_request(1, 42, data=b"hi")   # -> bytes
msg = rpc.decode_frame(frame)                    # -> RequestMessage(...)
```

## RPC transport

```python
import bare_rpc as rpc

async def on_request(req):
    await req.reply(b"pong")

# `send` delivers outgoing frames to your transport; feed incoming bytes to `receive`.
server = rpc.RPC(send=my_send, on_request=on_request)
await server.receive(incoming_bytes)

client = rpc.RPC(send=my_send)
result = await client.request(command=5, data=b"ping")   # -> b"pong"
await client.event(command=3, data=b"note")              # fire-and-forget
```

The RPC is transport-agnostic: wire `send`/`receive` to an asyncio stream, a websocket, or an in-memory pipe. Streams and backpressure are not yet implemented.
