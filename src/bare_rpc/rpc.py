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

DEFAULT_MAX_FRAME_SIZE = 16 * 1024 * 1024


async def _maybe_await(result):
    if inspect.isawaitable(result):
        return await result
    return result


class RPC:
    """One end of a bare-rpc session, independent of the transport.

    Outgoing frames go to ``send``; inbound bytes come in through ``receive``.

    A failure is terminal: ``send`` raising, a malformed or oversized inbound
    frame, or ``on_event`` raising. Pending requests and streams fail with the
    error, later requests raise it, and ``on_error`` is called with it.

    Args:
        send: Called with each outgoing frame as bytes, in order. May be a
            coroutine function.
        on_request: Called with an ``IncomingRequest`` for each request. May be
            a coroutine function. Raising before answering rejects the request
            with the exception, unless the request opened a request stream.
            Requests are ignored when None.
        on_event: Called with an ``IncomingEvent`` for each event. May be a
            coroutine function. Events are dropped when None.
        on_error: Called with the error when the session fails. May be a
            coroutine function.
        max_frame_size: Largest inbound frame accepted, length prefix included.

    Raises:
        ValueError: ``max_frame_size`` is not positive.
    """

    def __init__(
        self,
        send,
        *,
        on_request=None,
        on_event=None,
        on_error=None,
        max_frame_size=DEFAULT_MAX_FRAME_SIZE,
    ):
        if max_frame_size <= 0:
            raise ValueError("max_frame_size must be positive")
        self._send_cb = send
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
        self._writer_task = None
        self._closed = False
        self._outgoing_streams = {}
        self._incoming_streams = {}
        self._pending_response_streams = {}

    def _send(self, frame):
        if self._failed is not None or self._closed:
            return
        self._outbound.append(frame)
        self._outbound_ready.set()
        self._ensure_writer()

    def _ensure_writer(self):
        if self._writer_task is None:
            self._writer_task = asyncio.create_task(self._writer())

    async def _writer(self):
        while self._failed is None and not self._closed:
            await self._outbound_ready.wait()
            self._outbound_ready.clear()
            while self._outbound:
                frame = self._outbound.popleft()
                try:
                    await _maybe_await(self._send_cb(frame))
                except Exception as exc:
                    self._fail(exc)
                    return

    def close(self):
        """Close the session.

        Pending requests and streams fail with ``RuntimeError``, and later
        events and inbound bytes are dropped.
        """
        if self._failed is not None or self._closed:
            return
        self._closed = True
        self._outbound.clear()
        self._outbound_ready.set()
        self._reject_all(RuntimeError("RPC is closed"))

    async def request(self, command, data=None):
        """Send a request and wait for the reply.

        Args:
            command: Command number.
            data: Payload. None sends an empty payload.

        Returns:
            The reply payload as bytes.

        Raises:
            RPCRemoteError: The peer rejected the request.
            RuntimeError: The session is closed. When the session has failed,
                its error is raised instead.
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
        """Send a request and wait for the peer to open a response stream.

        Args:
            command: Command number.
            data: Payload. None sends an empty payload.

        Returns:
            An ``IncomingStream`` of the peer's chunks.

        Raises:
            RPCRemoteError: The peer rejected the request.
            RuntimeError: The session is closed, or the peer replied without
                opening a stream. When the session has failed, its error is
                raised instead.
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
        """Open a request stream to the peer.

        Args:
            command: Command number.

        Returns:
            An ``(OutgoingStream, reply)`` pair. ``reply`` is a future that
            resolves to the reply payload as bytes, or raises
            ``RPCRemoteError`` if the peer rejects the request.

        Raises:
            RuntimeError: The session is closed. When the session has failed,
                its error is raised instead.
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
        """Open a request stream and wait for the peer's response stream.

        Args:
            command: Command number.

        Returns:
            An ``(OutgoingStream, IncomingStream)`` pair.

        Raises:
            RPCRemoteError: The peer rejected the request.
            RuntimeError: The session is closed, or the peer replied without
                opening a stream. When the session has failed, its error is
                raised instead.
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
        """Send an event, which gets no response.

        Dropped when the session is closed or failed.

        Args:
            command: Command number.
            data: Payload. None sends an empty payload.
        """
        if self._failed is not None or self._closed:
            return
        self._send(encode_event(command, data=data))

    async def receive(self, data):
        """Feed inbound bytes from the transport.

        ``data`` need not align with frames: partial and coalesced frames are
        both handled. A malformed or oversized frame fails the session rather
        than raising here.

        Args:
            data: Bytes read from the transport.
        """
        if self._failed is not None or self._closed:
            return
        self._buffer += data
        while True:
            if len(self._buffer) < 4:
                return
            length = int.from_bytes(self._buffer[0:4], "little")
            if 4 + length > self._max_frame_size:
                self._fail(ValueError("frame exceeds max_frame_size"))
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
                        RuntimeError("expected a response stream")
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
        # Unlike _run_request, a throwing stream-open handler is NOT auto-rejected:
        # by the time we're here the id has already been promoted to a stream (its
        # OPEN ack is already on the wire), so a reject would send a spurious
        # second/duplicate response on a stream id, not a clean error for request
        # id 0. The generated code is responsible for replying / destroying its
        # streams. Matches Swift's try? on handleRequestStreamOpen.
        try:
            await _maybe_await(self._on_request(req))
        except Exception:
            pass

    def _on_stream(self, message):
        flags = message.flags
        if flags & StreamFlag.OPEN:
            # STREAM|OPEN is a protocol ack, not consumed by this runtime (kept
            # for wire parity / future cross-runtime interop); intentionally a
            # no-op on receipt, not dead code.
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
        if self._failed is not None:
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
