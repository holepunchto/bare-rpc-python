import asyncio
import inspect
from collections import deque

from .incoming import IncomingEvent, IncomingRequest
from .messages import (
    RequestMessage,
    ResponseMessage,
    StreamMessage,
    decode_frame,
    encode_event,
    encode_request,
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

    async def request(self, command, data=None):
        if self._failed is not None:
            raise self._failed
        self._id += 1
        request_id = self._id
        future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        self._send(encode_request(request_id, command, data=data))
        return await future

    async def event(self, command, data=None):
        if self._failed is not None:
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
                req = IncomingRequest(
                    self._send, message.id, message.command, message.data
                )
                self._spawn(self._run_request(req))
        elif isinstance(message, ResponseMessage):
            if message.id == 0:
                return
            future = self._pending.pop(message.id, None)
            if future is None or future.done():
                return
            if message.error is not None:
                future.set_exception(message.error)
            else:
                future.set_result(message.data)
        elif isinstance(message, StreamMessage):
            return  # streams deferred

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
        for future in self._pending.values():
            if not future.done():
                future.set_exception(error)
        self._pending.clear()
        if self._on_error is not None:
            self._on_error(error)
