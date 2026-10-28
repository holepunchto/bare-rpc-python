import asyncio
import inspect
from collections import deque

from .constants import StreamFlag
from .incoming import IncomingEvent, IncomingRequest
from .incoming_stream import IncomingStream
from .messages import (
    RequestMessage,
    ResponseMessage,
    StreamMessage,
    decode_frame,
    encode_event,
    encode_request,
    encode_stream,
)
from .outgoing_stream import OutgoingStream
from .transport import Transport

DEFAULT_MAX_FRAME_SIZE = 16 * 1024 * 1024


async def _maybe_await(result):
    if inspect.isawaitable(result):
        return await result
    return result


class RPC:
    """One end of a bare-rpc session over ``transport``.

    ``transport`` is a ``Transport``. The session starts reading from it at
    once, so it must be created while an event loop is running. It closes once
    the transport ends, and leaving ``async with`` closes it.

    ``on_request`` is called with an
    ``IncomingRequest`` for each request, which is ignored if it is ``None``.
    Raising before answering rejects the request with the exception, unless the
    request opened a request stream. ``on_event`` is called with an
    ``IncomingEvent`` for each event, which is dropped if it is ``None``.
    ``on_error`` is called with the error once the session fails. Each of them
    may be a coroutine function. ``max_frame_size`` is the size of the largest
    inbound frame accepted, including its length prefix.

    A failure is terminal: the transport or ``on_event`` raising, or a
    malformed or oversized inbound frame. Pending requests and streams fail
    with the error, later requests raise it, and the transport is closed.
    """

    def __init__(
        self,
        transport: Transport,
        *,
        on_request=None,
        on_event=None,
        on_error=None,
        max_frame_size=DEFAULT_MAX_FRAME_SIZE,
    ):
        if max_frame_size <= 0:
            raise ValueError("max_frame_size must be positive")
        loop = asyncio.get_running_loop()
        self._transport = transport
        self._on_request = on_request
        self._on_event = on_event
        self._on_error = on_error
        self._max_frame_size = max_frame_size
        self._id = 0
        self._pending = {}
        self._buffer = bytearray()
        self._tasks = set()
        self._failed = None
        self._outbound = deque()
        self._outbound_ready = asyncio.Event()
        self._closed = False
        self._closing = None
        self._outgoing_streams = {}
        self._incoming_streams = {}
        self._pending_response_streams = {}
        self._writer = loop.create_task(self._write_frames())
        self._reader = loop.create_task(self._read_frames())

    def _send(self, frame):
        if self._failed is not None or self._closed:
            return
        self._outbound.append(frame)
        self._outbound_ready.set()

    async def _write_frames(self):
        while True:
            while self._outbound:
                try:
                    await self._transport.write(self._outbound.popleft())
                except Exception as exc:
                    self._fail(exc)
                    return
            if self._failed is not None or self._closed:
                return
            self._outbound_ready.clear()
            await self._outbound_ready.wait()

    async def _read_frames(self):
        try:
            while data := await self._transport.read():
                self._receive(data)
        except Exception as exc:
            self._fail(exc)
        self._spawn(self.close())

    async def close(self):
        """Close the session and the transport once the frames already sent
        have been written.

        Pending requests and streams fail with ``RuntimeError``, and later
        events and inbound frames are dropped.
        """
        if self._closing is None:
            self._closing = asyncio.ensure_future(self._close())
        await asyncio.shield(self._closing)

    async def _close(self):
        self._closed = True
        if self._failed is None:
            self._reject_all(RuntimeError("RPC is closed"))
        self._outbound_ready.set()
        await self._writer
        await _maybe_await(self._transport.close())
        self._reader.cancel()
        await asyncio.wait([self._reader])

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.close()

    async def request(self, command, data=None):
        """Send a request for ``command`` with ``data``, or an empty payload if
        ``None``, and return the payload of the reply.

        Raises ``RPCRemoteError`` if the peer rejects the request,
        ``RuntimeError`` if the session is closed, and the error of the session
        if it has failed.
        """
        if self._failed is not None:
            raise self._failed
        if self._closed:
            raise RuntimeError("RPC is closed")
        self._id += 1
        request_id = self._id
        future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        self._send(encode_request(request_id, command, data=data))
        return await future

    async def request_with_response_stream(self, command, data=None):
        """Send a request for ``command`` with ``data``, or an empty payload if
        ``None``, and return the ``IncomingStream`` of the response stream the
        peer opens.

        Raises ``RPCRemoteError`` if the peer rejects the request,
        ``RuntimeError`` if the session is closed or the peer replies without
        opening a stream, and the error of the session if it has failed.
        """
        if self._failed is not None:
            raise self._failed
        if self._closed:
            raise RuntimeError("RPC is closed")
        self._id += 1
        request_id = self._id
        future = asyncio.get_running_loop().create_future()
        self._pending_response_streams[request_id] = future
        self._send(encode_request(request_id, command, data=data))
        return await future

    async def stream_request(self, command):
        """Open a request stream for ``command`` and return an
        ``(OutgoingStream, reply)`` pair.

        ``reply`` is a future of the payload of the reply, which raises
        ``RPCRemoteError`` if the peer rejects the request. Raises
        ``RuntimeError`` if the session is closed, and the error of the session
        if it has failed.
        """
        if self._failed is not None:
            raise self._failed
        if self._closed:
            raise RuntimeError("RPC is closed")
        self._id += 1
        request_id = self._id
        future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        outgoing = OutgoingStream(self, request_id, StreamFlag.REQUEST)
        self._outgoing_streams[request_id] = outgoing
        self._send(encode_request(request_id, command, stream=StreamFlag.OPEN))
        return outgoing, future

    async def create_bidirectional_stream(self, command):
        """Open a request stream for ``command`` and return an
        ``(OutgoingStream, IncomingStream)`` pair once the peer opens its
        response stream.

        Raises ``RPCRemoteError`` if the peer rejects the request,
        ``RuntimeError`` if the session is closed or the peer replies without
        opening a stream, and the error of the session if it has failed.
        """
        if self._failed is not None:
            raise self._failed
        if self._closed:
            raise RuntimeError("RPC is closed")
        self._id += 1
        request_id = self._id
        future = asyncio.get_running_loop().create_future()
        self._pending_response_streams[request_id] = future
        outgoing = OutgoingStream(self, request_id, StreamFlag.REQUEST)
        self._outgoing_streams[request_id] = outgoing
        self._send(encode_request(request_id, command, stream=StreamFlag.OPEN))
        incoming = await future
        return outgoing, incoming

    async def event(self, command, data=None):
        """Send an event for ``command`` with ``data``, or an empty payload if
        ``None``, which gets no response.

        The event is dropped if the session is closed or has failed.
        """
        if self._failed is not None or self._closed:
            return
        self._send(encode_event(command, data=data))

    def _receive(self, data):
        if self._failed is not None or self._closed:
            return
        self._buffer += data
        while True:
            if len(self._buffer) < 4:
                return
            length = int.from_bytes(self._buffer[0:4], "little")
            if 4 + length > self._max_frame_size:
                self._fail(ValueError("Frame exceeds max_frame_size"))
                return
            if len(self._buffer) < 4 + length:
                return
            frame = bytes(self._buffer[: 4 + length])
            del self._buffer[: 4 + length]
            try:
                message = decode_frame(frame)
            except Exception as exc:
                self._fail(exc)
                return
            self._dispatch(message)

    def _dispatch(self, message):
        if isinstance(message, RequestMessage):
            if message.stream & StreamFlag.OPEN:
                self._on_request_stream_open(message)
            elif message.id == 0:
                if self._on_event is not None:
                    self._spawn(
                        self._run_event(IncomingEvent(message.command, message.data))
                    )
            elif self._on_request is not None:
                req = IncomingRequest(self, message.id, message.command, message.data)
                self._spawn(self._run_request(req))
        elif isinstance(message, ResponseMessage):
            if message.stream & StreamFlag.OPEN:
                self._on_response_stream_open(message)
                return
            if message.id == 0:
                return
            future = self._pending.pop(message.id, None)
            if future is not None:
                if not future.done():
                    if message.error is not None:
                        future.set_exception(message.error)
                    else:
                        future.set_result(message.data)
                return
            pending_stream = self._pending_response_streams.pop(message.id, None)
            if pending_stream is not None and not pending_stream.done():
                if message.error is not None:
                    pending_stream.set_exception(message.error)
                else:
                    pending_stream.set_exception(
                        RuntimeError("Expected a response stream")
                    )
        elif isinstance(message, StreamMessage):
            self._on_stream(message)

    def _on_response_stream_open(self, message):
        pending = self._pending_response_streams.pop(message.id, None)
        if pending is None or pending.done():
            return
        incoming = IncomingStream(self, message.id, StreamFlag.RESPONSE)
        self._incoming_streams[message.id] = incoming
        self._send(encode_stream(message.id, StreamFlag.RESPONSE | StreamFlag.OPEN))
        pending.set_result(incoming)

    def _on_request_stream_open(self, message):
        if self._on_request is None:
            return
        incoming = IncomingStream(self, message.id, StreamFlag.REQUEST)
        self._incoming_streams[message.id] = incoming
        self._send(encode_stream(message.id, StreamFlag.REQUEST | StreamFlag.OPEN))
        req = IncomingRequest(self, message.id, message.command, message.data)
        req.request_stream = incoming
        self._spawn(self._run_stream_request(req))

    async def _run_stream_request(self, req):
        # The stream is already open, so the handler answers or destroys it
        # rather than the request being rejected.
        try:
            await _maybe_await(self._on_request(req))
        except Exception:
            pass

    def _on_stream(self, message):
        flags = message.flags
        if flags & StreamFlag.OPEN:
            # Acknowledges a stream this side opened, which needs no handling.
            return
        if flags & StreamFlag.CLOSE:
            stream = self._incoming_streams.get(message.id)
            if stream is None:
                return
            if flags & StreamFlag.ERROR:
                stream.destroy(message.error)
            else:
                stream.end()
            return
        if flags & StreamFlag.PAUSE:
            stream = self._outgoing_streams.get(message.id)
            if stream is not None:
                stream.cork()
            return
        if flags & StreamFlag.RESUME:
            stream = self._outgoing_streams.get(message.id)
            if stream is not None:
                stream.uncork()
            return
        if flags & StreamFlag.DATA:
            stream = self._incoming_streams.get(message.id)
            if stream is not None:
                stream.push(message.data)
            return
        if flags & StreamFlag.END:
            stream = self._incoming_streams.get(message.id)
            if stream is not None:
                stream.end()
            return
        if flags & StreamFlag.DESTROY:
            stream = self._outgoing_streams.get(message.id)
            if stream is not None:
                stream.destroy(message.error)

    def _spawn(self, coro):
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _run_request(self, req):
        try:
            await _maybe_await(self._on_request(req))
        except Exception as exc:
            if not req._replied:
                try:
                    await req.reject(exc)
                except Exception as send_exc:
                    self._fail(send_exc)

    async def _run_event(self, event):
        try:
            await _maybe_await(self._on_event(event))
        except Exception as exc:
            self._fail(exc)

    def _fail(self, error):
        if self._failed is not None or self._closed:
            return
        self._failed = error
        self._buffer.clear()
        self._outbound.clear()
        self._outbound_ready.set()
        self._reject_all(error)
        if self._on_error is not None:
            result = self._on_error(error)
            if inspect.isawaitable(result):
                self._spawn(_maybe_await(result))
        self._spawn(self.close())

    def _reject_all(self, error):
        for future in self._pending.values():
            if not future.done():
                future.set_exception(error)
        self._pending.clear()
        for future in self._pending_response_streams.values():
            if not future.done():
                future.set_exception(error)
        self._pending_response_streams.clear()
        for stream in list(self._outgoing_streams.values()):
            stream._abort(error)
        self._outgoing_streams.clear()
        for stream in list(self._incoming_streams.values()):
            stream._abort(error)
        self._incoming_streams.clear()
