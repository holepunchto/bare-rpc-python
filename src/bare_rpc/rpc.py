import asyncio
import inspect
from collections import deque
from collections.abc import Awaitable, Callable
from functools import partial

from .constants import (
    _CLOSE,
    _DATA,
    _DESTROY,
    _END,
    _ERROR,
    _OPEN,
    _PAUSE,
    _REQUEST,
    _RESPONSE,
    _RESUME,
)
from .handler import Handler
from .incoming import IncomingEvent, IncomingRequest, _remote_error, _unknown_command
from .incoming_stream import IncomingStream
from .messages import (
    CancelMessage,
    RequestMessage,
    ResponseMessage,
    RPCError,
    RPCRemoteError,
    StreamMessage,
    UnknownMessageTypeError,
    decode_frame,
    encode_cancel,
    encode_event,
    encode_request,
    encode_stream,
)
from .outgoing_stream import OutgoingStream
from .transport import Transport

DEFAULT_MAX_FRAME_SIZE = 16 * 1024 * 1024
DEFAULT_MAX_BUFFERED_BYTES = 16 * 1024 * 1024

_MAX_EARLY_OPENS = 1024

_DIRECTIONS = _REQUEST | _RESPONSE

_CANCELLED = RPCRemoteError("Request was cancelled", "CANCELLED")
_TIMED_OUT = RPCRemoteError("Request timed out", "TIMEOUT")

# The stream actions that a valid peer sends, from the encoders of the streams.
_STREAM_ACTIONS = frozenset(
    {
        _OPEN,
        _CLOSE,
        _CLOSE | _ERROR,
        _PAUSE,
        _RESUME,
        _DATA,
        _END,
        _DESTROY,
        _DESTROY | _ERROR,
    }
)


_RequestHandler = Callable[[IncomingRequest], Awaitable[None] | None]
_EventHandler = Callable[[IncomingEvent], Awaitable[None] | None]
_ErrorHandler = Callable[[Exception], Awaitable[None] | None]


def _invalid(message):
    return RPCError(message, "INVALID_MESSAGE")


def _check_stream(kind, stream, may_open):
    if stream != 0 and (stream != _OPEN or not may_open):
        raise _invalid(f"Invalid {kind} stream {stream}")


async def _maybe_await(result):
    if inspect.isawaitable(result):
        return await result
    return result


def _reject_unknown_command(request):
    raise _unknown_command(request.command)


class RPC:
    """One end of a bare-rpc session over ``transport``.

    ``transport`` is a ``Transport``. The session starts reading from it at
    once, so it must be created while an event loop is running. It closes once
    the transport ends, and leaving ``async with`` closes it.

    ``on_request`` is called with an ``IncomingRequest`` for each request.
    Raising before answering rejects the request with the exception, and
    raising after opening a response stream destroys that stream with it. If
    ``on_request`` is ``None``, every request is rejected with an
    ``RPCRemoteError`` with code ``UNKNOWN_COMMAND``. ``on_event`` is called
    with an ``IncomingEvent`` for each event, which is dropped if it is
    ``None``. ``handler`` is a ``Handler`` to use instead of ``on_request`` and
    ``on_event``, and passing it with either of them raises ``ValueError``.
    ``on_error`` is called with the error once the session fails. Each of them
    may be a coroutine function.

    ``max_frame_size`` is the size of the largest inbound frame accepted,
    including its length prefix. ``max_buffered_bytes`` is the most data that
    one incoming stream may hold before it is read. A peer that keeps writing
    after it has been paused fails the session once it exceeds that.

    A failure is terminal: the transport or the event handler raising, or an
    inbound frame that fails with ``RPCError``. That is a frame that exceeds
    ``max_frame_size``, has an unknown message type, does not decode, or
    breaks the protocol, such as a request that reuses the id of a live
    request of the peer, or data that overflows ``max_buffered_bytes``.
    Pending requests and streams fail with the error, later requests and
    writes raise it, and the transport is closed. Once the session closes
    without failing, they fail with an ``RPCError`` with code
    ``CHANNEL_CLOSED`` instead. Frames that refer to a request or stream that
    has finished, or that was never known, are ignored.

    Each side of a stream waits for the other to open its end before sending
    on it, and chunks that arrive for a stream before this side has opened it
    are dropped, as in bare-rpc.

    Cancelling the future that a call waits for, as cancelling the task that
    awaits it does, cancels the request: the peer is told to stop handling it,
    and the streams of the request fail with an ``RPCError`` with code
    ``CANCELLED``. When the peer cancels a request, the task of its handler is
    cancelled, and ``IncomingRequest.cancellation_error`` holds the error the
    peer cancelled it with. Once the session ends, the tasks of the handlers
    that are still running are cancelled too.
    """

    def __init__(
        self,
        transport: Transport,
        *,
        handler: Handler | None = None,
        on_request: _RequestHandler | None = None,
        on_event: _EventHandler | None = None,
        on_error: _ErrorHandler | None = None,
        max_frame_size: int = DEFAULT_MAX_FRAME_SIZE,
        max_buffered_bytes: int = DEFAULT_MAX_BUFFERED_BYTES,
    ) -> None:
        if max_frame_size <= 0:
            raise ValueError("max_frame_size must be positive")
        if max_buffered_bytes <= 0:
            raise ValueError("max_buffered_bytes must be positive")
        if handler is not None:
            if on_request is not None or on_event is not None:
                raise ValueError("Pass either handler or on_request and on_event")
            on_request, on_event = handler.handle_request, handler.handle_event
        loop = asyncio.get_running_loop()
        self._transport = transport
        self._on_request = on_request or _reject_unknown_command
        self._on_event = on_event
        self._on_error = on_error
        self._max_frame_size = max_frame_size
        self._max_buffered_bytes = max_buffered_bytes
        self._id = 0
        self._pending = {}
        self._buffer = bytearray()
        self._tasks = set()
        self._failed = None
        self._outbound = deque()
        self._outbound_ready = asyncio.Event()
        self._flushes = set()
        self._ended = asyncio.Event()
        self._closed_error = None
        self._closing = None
        self._outgoing_streams = {}
        self._incoming_streams = {}
        self._pending_response_streams = {}
        self._unanswered = {}
        self._handlers = {}
        self._early_opens = set()
        self._highest_request_id = 0
        self._writer = loop.create_task(self._write_frames())
        self._reader = loop.create_task(self._read_frames())

    @property
    def _end_error(self):
        return self._failed if self._failed is not None else self._closed_error

    def _send(self, frame, flushed=None):
        if self._end_error is not None:
            return
        self._outbound.append((frame, flushed))
        self._outbound_ready.set()

    async def _send_and_wait(self, frame):
        if self._end_error is not None:
            raise self._end_error
        flushed = asyncio.get_running_loop().create_future()
        self._flushes.add(flushed)
        self._send(frame, flushed)
        try:
            await flushed
        finally:
            self._flushes.discard(flushed)

    async def _write_frames(self):
        while True:
            while self._outbound:
                frame, flushed = self._outbound.popleft()
                try:
                    await self._transport.write(frame)
                except Exception as exc:
                    self._fail(exc)
                    return
                if flushed is not None and not flushed.done():
                    flushed.set_result(None)
            if self._end_error is not None:
                return
            self._outbound_ready.clear()
            await self._outbound_ready.wait()

    async def _read_frames(self):
        try:
            while data := await self._transport.read():
                self._receive(data)
            if self._buffer:
                self._fail(_invalid("Truncated frame"))
        except Exception as exc:
            self._fail(exc)
        self._spawn(self.close())

    async def close(self) -> None:
        """Close the session and the transport once the frames already sent
        have been written.

        Pending requests and streams fail with an ``RPCError`` with code
        ``CHANNEL_CLOSED``, and later events and inbound frames are dropped.
        """
        if self._closing is None:
            self._closing = asyncio.create_task(self._close())
        await asyncio.shield(self._closing)

    async def _close(self):
        self._closed_error = RPCError("Channel closed", "CHANNEL_CLOSED")
        if self._failed is None:
            self._reject_all(self._closed_error)
        self._ended.set()
        self._outbound_ready.set()
        await self._writer
        await _maybe_await(self._transport.close())
        self._reader.cancel()
        await asyncio.wait([self._reader])

    async def wait(self) -> None:
        """Wait for the session to end.

        Returns once it closes cleanly, after the transport ends or ``close()``
        is called, and raises the error that failed it otherwise.
        """
        await self._ended.wait()
        if self._failed is not None:
            raise self._failed

    async def __aenter__(self) -> "RPC":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    def _next_request_id(self):
        if self._end_error is not None:
            raise self._end_error
        self._id += 1
        return self._id

    async def request(
        self, command: int, data: bytes | None = None, *, timeout: float | None = None
    ) -> bytes:
        """Send a request for ``command`` with ``data``, or an empty payload if
        ``None``, and return the payload of the reply.

        If no reply arrives within ``timeout`` seconds, the request is
        cancelled and an ``RPCError`` with code ``TIMEOUT`` is raised.

        Raises ``RPCRemoteError`` if the peer rejects the request, an
        ``RPCError`` with code ``UNEXPECTED_STREAM`` if the peer answers with
        a response stream, an ``RPCError`` with code ``CHANNEL_CLOSED`` if the
        session is closed, and the error of the session if it has failed.
        """
        request_id = self._next_request_id()
        future = self._track(request_id)
        self._pending[request_id] = future
        self._send(encode_request(request_id, command, data=data))
        if timeout is None:
            return await future
        timer = asyncio.get_running_loop().call_later(
            timeout,
            self._cancel_request,
            request_id,
            _TIMED_OUT,
            RPCError("Request timed out", "TIMEOUT"),
        )
        try:
            return await future
        finally:
            timer.cancel()

    async def request_with_response_stream(
        self, command: int, data: bytes | None = None
    ) -> IncomingStream:
        """Send a request for ``command`` with ``data``, or an empty payload if
        ``None``, and return the ``IncomingStream`` of the response stream the
        peer opens.

        If the peer replies without opening a stream, the stream yields the
        payload of the reply, unless it is empty, and then ends.

        Raises ``RPCRemoteError`` if the peer rejects the request,
        an ``RPCError`` with code ``CHANNEL_CLOSED`` if the session is closed,
        and the error of the session if it has failed.
        """
        request_id = self._next_request_id()
        future = self._read_response_stream(request_id)
        self._send(encode_request(request_id, command, data=data))
        self._send(encode_stream(request_id, _RESPONSE | _OPEN))
        return await future

    async def stream_request(
        self, command: int
    ) -> tuple[OutgoingStream, asyncio.Future[bytes]]:
        """Open a request stream for ``command`` and return an
        ``(OutgoingStream, reply)`` pair.

        ``reply`` is a future of the payload of the reply, which raises
        ``RPCRemoteError`` if the peer rejects the request, and an
        ``RPCError`` with code ``UNEXPECTED_STREAM`` if the peer answers with
        a response stream. If the peer answers without having opened its end
        of the request stream, the stream is closed. Raises
        an ``RPCError`` with code ``CHANNEL_CLOSED`` if the session is closed,
        and the error of the session if it has failed.
        """
        request_id = self._next_request_id()
        future = self._track(request_id)
        self._pending[request_id] = future
        outgoing = OutgoingStream(self, request_id, _REQUEST)
        self._open_outgoing(outgoing)
        self._send(encode_request(request_id, command, stream=_OPEN))
        return outgoing, future

    async def create_bidirectional_stream(
        self, command: int
    ) -> tuple[OutgoingStream, IncomingStream]:
        """Open a request stream for ``command`` and return an
        ``(OutgoingStream, IncomingStream)`` pair once the peer opens its
        response stream.

        If the peer replies without opening a stream, the stream yields the
        payload of the reply, unless it is empty, and then ends.

        Raises ``RPCRemoteError`` if the peer rejects the request,
        an ``RPCError`` with code ``CHANNEL_CLOSED`` if the session is closed,
        and the error of the session if it has failed.
        """
        request_id = self._next_request_id()
        future = self._read_response_stream(request_id)
        outgoing = OutgoingStream(self, request_id, _REQUEST)
        self._open_outgoing(outgoing)
        self._send(encode_request(request_id, command, stream=_OPEN))
        self._send(encode_stream(request_id, _RESPONSE | _OPEN))
        incoming = await future
        return outgoing, incoming

    # The reader of a response opens its end of the stream as soon as it
    # sends the request, as bare-rpc does, so that the peer can write to it
    # without another round trip.
    def _read_response_stream(self, request_id):
        future = self._track(request_id)
        incoming = IncomingStream(self, request_id, _RESPONSE)
        self._pending_response_streams[request_id] = (future, incoming)
        self._incoming_streams[(request_id, _RESPONSE)] = incoming
        return future

    # A request whose future is cancelled before it is answered is cancelled at
    # the peer too.
    def _track(self, request_id):
        future = asyncio.get_running_loop().create_future()
        future.add_done_callback(partial(self._on_settled, request_id))
        return future

    def _on_settled(self, request_id, future):
        if future.cancelled():
            self._cancel_request(request_id, _CANCELLED)

    def _cancel_request(self, request_id, reason, error=None):
        future = self._pending.pop(request_id, None)
        if future is None:
            pending = self._pending_response_streams.pop(request_id, None)
            if pending is None:
                return
            future, _ = pending
        if error is not None and not future.done():
            future.set_exception(error)
        self._send(encode_cancel(request_id, reason))
        error = error or RPCError("Request was cancelled", "CANCELLED")
        outgoing = self._outgoing_streams.get((request_id, _REQUEST))
        if outgoing is not None:
            outgoing._abort(error)
        incoming = self._incoming_streams.pop((request_id, _RESPONSE), None)
        if incoming is not None:
            incoming._abort(error)

    def _open_outgoing(self, outgoing):
        if self._end_error is not None:
            outgoing._abort(self._end_error)
            return
        self._outgoing_streams[(outgoing._id, outgoing._mask)] = outgoing

    def _open_request_stream(self, request_id, cancellation_error):
        incoming = IncomingStream(self, request_id, _REQUEST)
        error = self._end_error or cancellation_error
        if error is not None:
            incoming._abort(error)
            return incoming
        self._incoming_streams[(request_id, _REQUEST)] = incoming
        self._send(encode_stream(request_id, _REQUEST | _OPEN))
        return incoming

    def _on_answered(self, request_id):
        self._unanswered.pop(request_id, None)
        if request_id in self._early_opens:
            self._early_opens.discard(request_id)
            return True
        return False

    async def event(self, command: int, data: bytes | None = None) -> None:
        """Send an event for ``command`` with ``data``, or an empty payload if
        ``None``, which gets no response.

        The event is dropped if the session is closed or has failed.
        """
        if self._end_error is not None:
            return
        self._send(encode_event(command, data=data))

    def _receive(self, data):
        if self._end_error is not None:
            return
        self._buffer += data
        while True:
            if len(self._buffer) < 4:
                return
            length = int.from_bytes(self._buffer[0:4], "little")
            if 4 + length > self._max_frame_size:
                raise RPCError("Frame exceeds max_frame_size", "FRAME_TOO_LARGE")
            if len(self._buffer) < 4 + length:
                return
            frame = bytes(self._buffer[: 4 + length])
            del self._buffer[: 4 + length]
            try:
                message = decode_frame(frame)
            except UnknownMessageTypeError as exc:
                raise RPCError(str(exc), "UNKNOWN_MESSAGE") from exc
            except Exception as exc:
                raise _invalid(f"Frame does not decode: {exc}") from exc
            self._dispatch(message)

    def _dispatch(self, message):
        if isinstance(message, RequestMessage):
            _check_stream("request", message.stream, message.id != 0)
            if self._is_live_request(message.id):
                raise _invalid(f"Request {message.id} is still live")
            self._highest_request_id = max(self._highest_request_id, message.id)
            if message.id == 0:
                if self._on_event is not None:
                    self._spawn(
                        self._run_event(IncomingEvent(message.command, message.data))
                    )
            else:
                req = IncomingRequest(
                    self,
                    message.id,
                    message.command,
                    message.data,
                    streamed=bool(message.stream & _OPEN),
                )
                self._unanswered[message.id] = req
                task = self._spawn(self._run_request(req))
                self._handlers[message.id] = task
                task.add_done_callback(partial(self._forget_handler, message.id))
        elif isinstance(message, ResponseMessage):
            if message.id == 0:
                raise _invalid("Response with id 0")
            _check_stream("response", message.stream, message.error is None)
            self._on_response(message)
        elif isinstance(message, StreamMessage):
            self._on_stream(message)
        elif isinstance(message, CancelMessage):
            if message.id == 0:
                raise _invalid("Cancel with id 0")
            self._on_cancel(message)

    def _forget_handler(self, request_id, task):
        if self._handlers.get(request_id) is task:
            del self._handlers[request_id]

    # The streams of the request end as a destroy of the peer would end them,
    # but without frames, as the peer has dropped its end too.
    def _on_cancel(self, message):
        req = self._unanswered.pop(message.id, None)
        if req is not None:
            req._cancellation_error = message.error
        self._early_opens.discard(message.id)
        task = self._handlers.pop(message.id, None)
        if task is not None:
            task.cancel()
        incoming = self._incoming_streams.pop((message.id, _REQUEST), None)
        if incoming is not None:
            incoming._abort(message.error)
        outgoing = self._outgoing_streams.get((message.id, _RESPONSE))
        if outgoing is not None:
            outgoing._drop()

    def _on_response(self, message):
        if message.stream & _OPEN:
            pending = self._pending_response_streams.pop(message.id, None)
            if pending is not None:
                future, incoming = pending
                if not future.done():
                    future.set_result(incoming)
            elif message.id in self._pending:
                self._on_unexpected_stream(message.id)
            return
        self._release_request_stream(message.id, message.error)
        future = self._pending.pop(message.id, None)
        if future is not None:
            if not future.done():
                if message.error is not None:
                    future.set_exception(message.error)
                else:
                    future.set_result(message.data)
            return
        pending = self._pending_response_streams.pop(message.id, None)
        incoming = self._incoming_streams.get((message.id, _RESPONSE))
        if pending is not None:
            future, incoming = pending
        if incoming is not None:
            if message.error is not None:
                incoming.destroy(message.error)
            else:
                if message.data:
                    incoming._push(message.data)
                incoming._end()
        if pending is not None and not future.done():
            if message.error is not None:
                future.set_exception(message.error)
            else:
                future.set_result(incoming)

    def _on_unexpected_stream(self, request_id):
        future = self._pending.pop(request_id)
        error = RPCError("Expected a reply without a stream", "UNEXPECTED_STREAM")
        if not future.done():
            future.set_exception(error)
        self._send(encode_stream(request_id, _RESPONSE | _DESTROY))
        self._release_request_stream(request_id, _remote_error(error))

    # Once the peer has answered, a request stream that it never opened would
    # wait for the open forever, so it is closed, as in bare-rpc.
    def _release_request_stream(self, request_id, error):
        outgoing = self._outgoing_streams.get((request_id, _REQUEST))
        if outgoing is not None and not outgoing._opened.is_set():
            outgoing._release(error)

    def _is_live_request(self, id):
        return (
            id in self._unanswered
            or (id, _REQUEST) in self._incoming_streams
            or (id, _RESPONSE) in self._outgoing_streams
        )

    def _on_early_open(self, request_id):
        if request_id in self._early_opens:
            return
        if (
            request_id not in self._unanswered
            and request_id <= self._highest_request_id
        ):
            return
        if len(self._early_opens) >= _MAX_EARLY_OPENS:
            raise _invalid("Too many response streams opened early")
        self._early_opens.add(request_id)

    def _on_stream(self, message):
        flags = message.flags
        if (
            message.id == 0
            or flags & _DIRECTIONS not in (_REQUEST, _RESPONSE)
            or flags & ~_DIRECTIONS not in _STREAM_ACTIONS
        ):
            raise _invalid(f"Invalid stream flags {flags} for id {message.id}")
        key = (message.id, flags & _DIRECTIONS)
        if flags & _OPEN:
            stream = self._outgoing_streams.get(key)
            if stream is not None:
                stream._open()
            elif flags & _RESPONSE:
                self._on_early_open(message.id)
            return
        if flags & _CLOSE:
            stream = self._incoming_streams.get(key)
            if stream is None:
                return
            if flags & _ERROR:
                stream.destroy(message.error)
            else:
                stream._end()
            return
        if flags & _PAUSE:
            stream = self._outgoing_streams.get(key)
            if stream is not None:
                stream._cork()
            return
        if flags & _RESUME:
            stream = self._outgoing_streams.get(key)
            if stream is not None:
                stream._uncork()
            return
        if flags & _DATA:
            stream = self._incoming_streams.get(key)
            if stream is not None:
                stream._push(message.data)
            return
        if flags & _END:
            stream = self._incoming_streams.get(key)
            if stream is not None:
                stream._end()
            return
        if flags & _DESTROY:
            if flags & _RESPONSE:
                self._early_opens.discard(message.id)
            stream = self._outgoing_streams.get(key)
            if stream is not None:
                stream.destroy(message.error)

    def _spawn(self, coro):
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def _run_request(self, req):
        try:
            await _maybe_await(self._on_request(req))
        except Exception as exc:
            try:
                await req._fail(exc)
            except Exception as send_exc:
                self._fail(send_exc)

    async def _run_event(self, event):
        try:
            await _maybe_await(self._on_event(event))
        except Exception as exc:
            self._fail(exc)

    def _fail(self, error):
        if self._end_error is not None:
            return
        self._failed = error
        self._buffer.clear()
        self._outbound.clear()
        self._outbound_ready.set()
        self._reject_all(error)
        self._ended.set()
        if self._on_error is not None:
            result = self._on_error(error)
            if inspect.isawaitable(result):
                self._spawn(_maybe_await(result))
        self._spawn(self.close())

    def _reject_all(self, error):
        for flushed in self._flushes:
            if not flushed.done():
                flushed.set_exception(error)
        for future in self._pending.values():
            if not future.done():
                future.set_exception(error)
        self._pending.clear()
        for future, _ in self._pending_response_streams.values():
            if not future.done():
                future.set_exception(error)
        self._pending_response_streams.clear()
        for stream in list(self._outgoing_streams.values()):
            stream._abort(error)
        self._outgoing_streams.clear()
        for stream in list(self._incoming_streams.values()):
            stream._abort(error)
        self._incoming_streams.clear()
        self._unanswered.clear()
        self._early_opens.clear()
        handlers = list(self._handlers.values())
        self._handlers.clear()
        for task in handlers:
            task.cancel()
