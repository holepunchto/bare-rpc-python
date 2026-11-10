# bare-rpc-python

RPC over any byte transport for Python. The Python counterpart of [bare-rpc](https://github.com/holepunchto/bare-rpc), and wire compatible with it. Built on [compact-encoding-python](https://github.com/holepunchto/compact-encoding-python).

```
pip install bare-rpc
```

[hrpc-python](https://github.com/holepunchto/hrpc-python) generates typed clients and servers on top of it. Use it directly for the raw transport.

## Usage

This example runs two peers over a TCP connection and makes one request:

```python
import asyncio

from bare_rpc import RPC, StreamTransport


async def pong(req):
    await req.reply(b"pong")


async def serve(reader, writer):
    await RPC(StreamTransport(reader, writer), on_request=pong).wait()


async def main():
    server = await asyncio.start_server(serve, "localhost", 8080)

    reader, writer = await asyncio.open_connection("localhost", 8080)

    async with RPC(StreamTransport(reader, writer)) as rpc:
        print(await rpc.request(1, b"ping"))  # b'pong'

    server.close()


asyncio.run(main())
```

`RPC` runs a session over a `Transport`. A `Transport` is any object with an async `read()`, which returns `b""` once it has ended, an async `write(data)`, and `close()`. A `Sidecar` from [bare-sidecar-python](https://github.com/holepunchto/bare-sidecar-python) is a transport. `StreamTransport` builds one from the reader and writer of a socket or any other asyncio stream. The session must be created while an event loop is running.

## Command routing

A `CommandRouter` sends each request and event to the handler registered for its command. Pass it to `RPC` as `handler`:

```python
from bare_rpc import RPC, CommandRouter

router = CommandRouter()

router.on_request(1, lambda req: b"pong")
router.on_event(2, lambda event: print(event.command))

rpc = RPC(transport, handler=router)
```

A request handler returns the payload to reply with, unless it has answered the request itself. Either kind of handler may be async. A request for a command with no handler is rejected with the code `UNKNOWN_COMMAND`. An event for a command with no handler is dropped.

`CommandRouter` is a `Handler`. You can write your own `Handler` instead, with `handle_request(req)` and `handle_event(event)`. A subclass of `Handler` only needs to define the methods it uses. By default, requests are rejected with the code `UNKNOWN_COMMAND` and events are dropped. Passing `handler` together with `on_request` or `on_event` raises `ValueError`.

## Requests and responses

`await rpc.request(command, data)` sends a request and returns its reply. `await rpc.event(command, data)` sends an event, which gets no reply. Each request and event of the peer is handled in its own task, so a slow handler does not hold back the others.

A request is answered once, with `reply()`, `reject()` or `create_response_stream()`. Answering again raises an `RPCError` with the code `ALREADY_SENT`, and `req.sent` tells whether the request has been answered.

If a request handler raises before it answers, the request is rejected with the exception. An `RPCRemoteError` is sent as is. Any other exception is sent with `str(error)` as the message, and its `code` and `errno` attributes if it has them, or an empty code and errno 0. If the handler had opened a response stream, the stream is destroyed with the exception instead.

When the peer rejects a request, `request()` raises an `RPCRemoteError` with the `message`, `code` and `errno` of the peer. It is a subclass of `RPCError`, so `except RPCError` catches every error of the session.

Without `on_request`, every request is rejected with the code `UNKNOWN_COMMAND`. Without `on_event`, events are dropped.

## Streaming requests and responses

A request can stream its body, its reply, or both:

- `stream_request(command)` returns an `OutgoingStream` for the body and a future of the reply.
- `request_with_response_stream(command, data)` sends a plain request and returns an `IncomingStream` for the reply.
- `create_bidirectional_stream(command)` returns an `OutgoingStream` for the body and an `IncomingStream` for the reply.

On the answering side, `req.request_stream` is the `IncomingStream` of the body, or `None` if the request has none, and `create_response_stream()` returns an `OutgoingStream` for the reply. This handler echoes the body back:

```python
async def echo(req):
    out = await req.create_response_stream()

    async for chunk in req.request_stream:
        await out.write(chunk)

    await out.end()


out, incoming = await client.create_bidirectional_stream(1)
await out.write(b"ping")
await out.end()

async for chunk in incoming:
    print(chunk)  # b'ping'
```

A stream only carries data once both ends have opened it, so `write()` and `end()` wait until the peer has opened its end. The handler opens the body when it first reads `req.request_stream`. If the peer answers before it opens the body, the body is not sent, and later writes and `end()` do nothing. A write after `end()` is dropped, and a write after `destroy()` raises `RuntimeError`.

An `IncomingStream` asks the peer to pause once it buffers 16 KiB, and to resume once less than that is buffered. While the peer has paused an `OutgoingStream`, `write()` waits. If the peer keeps sending anyway, an incoming stream holds at most `max_buffered_bytes`, 16 MiB by default, before the session fails with an `RPCError` with the code `STREAM_OVERFLOW`.

Read an `IncomingStream` with `async for`, or one chunk at a time with `await stream.read()`, which returns `None` once the peer has ended the stream. Reading `req.request_stream` again returns the same stream, and calling `read()` while another `read()` is waiting raises `RuntimeError`.

## Cancellation

Cancelling the future that a call waits for cancels the request. Cancelling the task that awaits `request()`, `request_with_response_stream()` or `create_bidirectional_stream()` does that, and so does cancelling the `reply` future of `stream_request()`. The peer is told to stop handling the request, and the streams of the request fail with an `RPCError` with the code `CANCELLED`. Once the answer has arrived, cancelling does nothing. A request stream of `stream_request()` that is still open then stays open, so end or destroy it to stop it.

The same goes once `request_with_response_stream()` or `create_bidirectional_stream()` has returned its stream. Cancelling the task that reads the stream then only interrupts the read, as with any asyncio stream. To stop a streamed reply, destroy the stream.

`request()` also takes a `timeout` in seconds. If no reply arrives in time, the request is cancelled with the code `TIMEOUT`, and the call raises an `RPCError` with that code:

```python
reply = await rpc.request(1, b"ping", timeout=5)
```

A timeout from `asyncio.timeout()` or `asyncio.wait_for()` cancels the task instead, so the peer gets the code `CANCELLED`.

On the answering side, a cancel of the peer cancels the task of the handler, so it sees `asyncio.CancelledError`. `request.cancellation_error` holds the `RPCRemoteError` that the peer cancelled with. The streams of the request end as if the peer had destroyed them with that error: reading the request stream raises it, and writing to the response stream raises `RuntimeError`. Answering the request sends nothing:

```python
async def on_request(request):
    try:
        await request.reply(await search(request.data))
    except asyncio.CancelledError:
        if request.cancellation_error is not None:
            print("The peer cancelled with", request.cancellation_error.code)
        raise
```

When the session ends, the tasks of the request and event handlers that are still running are cancelled too.

Cancelling a request needs a peer that supports it. A peer without support fails the session with the code `UNKNOWN_MESSAGE` when it receives a cancel.

## Termination

The session ends with an `RPCError` with the code `CHANNEL_CLOSED` when you call `close()`, leave `async with`, or the transport ends. It fails with an error instead when:

- The transport ends partway through a frame, with the code `INVALID_MESSAGE`.
- Reading from or writing to the transport raises, with that error.
- An event handler raises, with that error.
- The peer sends a bad frame, as described under [Untrusted peers](#untrusted-peers).

Pending requests and streams then fail with the error, and so do later requests and writes. The tasks of running request and event handlers are cancelled, and a handler that calls `close()` itself is cancelled once `close()` returns. Events sent after the end are dropped. If you passed `on_error`, it is called with the error that failed the session.

The session then closes the transport. If it ended with `CHANNEL_CLOSED`, it first writes the frames already queued. Otherwise it drops them.

`close()` waits for the transport to close. `wait()` waits for the session to end. It returns if the session ended with `CHANNEL_CLOSED`, and raises the error otherwise:

```python
try:
    await rpc.wait()
except Exception as error:
    print(f"The session failed: {error}")
```

## Untrusted peers

`RPC` does not trust the peer to send valid frames. If a frame is larger than `max_frame_size`, cannot be decoded, or breaks the protocol, the session fails with an `RPCError`. The code tells you why:

- `FRAME_TOO_LARGE`: The frame, with its 4-byte length prefix, is larger than `max_frame_size`. This is checked as soon as the length prefix arrives, before the rest of the frame is read.
- `UNKNOWN_MESSAGE`: The frame has a message type that `RPC` does not know.
- `INVALID_MESSAGE`: The frame cannot be decoded, the transport ends partway through a frame, or the frame is one that a valid peer never sends. Examples are a response, stream or cancel message with an id of 0, a stream message with an invalid set of flags, and a new request that reuses the id of a request that is still in flight.
- `STREAM_OVERFLOW`: An incoming stream holds more than `max_buffered_bytes` of data that has not been read. This only happens when the peer keeps sending after it was asked to pause.

A frame that refers to a request or stream that has already finished, or that was never known, is ignored. The same goes for frames that arrive for a stream that has already ended or been destroyed.

If you read the reply as a stream but the peer replies without one, the reply arrives on the stream as a single chunk, unless it is empty, and the stream then ends. An error reply makes the call raise the error instead of returning a stream.

If you expect a plain reply but the peer replies with a stream, the call raises an `RPCError` with the code `UNEXPECTED_STREAM`. The session destroys the stream toward the peer and stays open.

## License

Apache-2.0
