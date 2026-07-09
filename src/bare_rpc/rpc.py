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

DEFAULT_MAX_FRAME_SIZE = 16 * 1024 * 1024


async def _maybe_await(result):
    if inspect.isawaitable(result):
        await result


class RPC:
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
        if self._failed is not None or self._closed:
            return
        self._closed = True
        self._outbound.clear()
        self._outbound_ready.set()
        self._reject_all(RuntimeError("RPC is closed"))

    async def request(self, command, data=None):
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

    async def event(self, command, data=None):
        if self._failed is not None or self._closed:
            return
        self._send(encode_event(command, data=data))

    async def receive(self, data):
        if self._failed is not None:
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
        if message is None:
            return
        if isinstance(message, RequestMessage):
            if message.id == 0:
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

    def _on_stream(self, message):
        flags = message.flags
        if flags & StreamFlag.OPEN:
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
            self._on_error(error)

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
