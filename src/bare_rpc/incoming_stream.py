import asyncio
from collections import deque

from .constants import StreamFlag
from .messages import encode_stream

HIGH_WATER_MARK = 16
LOW_WATER_MARK = 4


class IncomingStream:
    def __init__(self, rpc, id, mask):
        self._rpc = rpc
        self._id = id
        self._mask = mask
        self._buffer = deque()
        self._paused = False
        self._finished = False
        self._error = None
        self._waiter = None

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._buffer:
            chunk = self._buffer.popleft()
            if self._paused and len(self._buffer) <= LOW_WATER_MARK:
                self._paused = False
                self._rpc._send(encode_stream(self._id, self._mask | StreamFlag.RESUME))
            return chunk
        if self._error is not None:
            raise self._error
        if self._finished:
            raise StopAsyncIteration
        self._waiter = asyncio.get_running_loop().create_future()
        try:
            return await self._waiter
        finally:
            self._waiter = None

    def push(self, data):
        if self._finished:
            return
        if self._waiter is not None and not self._waiter.done():
            waiter = self._waiter
            self._waiter = None
            waiter.set_result(data)
            return
        self._buffer.append(data)
        if not self._paused and len(self._buffer) >= HIGH_WATER_MARK:
            self._paused = True
            self._rpc._send(encode_stream(self._id, self._mask | StreamFlag.PAUSE))

    def end(self):
        if self._finished:
            return
        self._finished = True
        self._wake(StopAsyncIteration())
        self._rpc._incoming_streams.pop(self._id, None)

    def destroy(self, error=None):
        if self._finished:
            return
        self._finished = True
        self._error = error
        if error is not None:
            self._rpc._send(
                encode_stream(
                    self._id,
                    self._mask | StreamFlag.DESTROY | StreamFlag.ERROR,
                    error=error,
                )
            )
        else:
            self._rpc._send(encode_stream(self._id, self._mask | StreamFlag.DESTROY))
        self._wake(error or StopAsyncIteration())
        self._rpc._incoming_streams.pop(self._id, None)

    def _abort(self, error):
        # Local teardown for _fail/close: fail a parked waiter, no wire send,
        # no map mutation (the caller clears the maps).
        self._finished = True
        self._error = error
        self._wake(error)

    def _wake(self, exc):
        if self._waiter is not None and not self._waiter.done():
            waiter = self._waiter
            self._waiter = None
            waiter.set_exception(exc)
