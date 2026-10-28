import asyncio
from collections import deque

from .constants import StreamFlag
from .messages import encode_stream

HIGH_WATER_MARK = 16
LOW_WATER_MARK = 4


class IncomingStream:
    """Chunks from the peer.

    Iterating it with ``async for`` yields the chunks until the peer ends the
    stream. The peer is paused once 16 chunks are buffered and resumed once 4
    remain.
    """

    def __init__(self, rpc, id, mask):
        self._rpc = rpc
        self._id = id
        self._mask = mask
        self._buffer = deque()
        self._paused = False
        self._finished = False
        self._error = None
        self._waiter = None

    async def read(self):
        """Read the next chunk from the peer, or ``None`` once the peer has
        ended the stream.

        Raises ``RPCRemoteError`` if the peer closes the stream with an error,
        the error of the session once it fails, and ``RuntimeError`` once it
        closes.
        """
        if self._buffer:
            chunk = self._buffer.popleft()
            if self._paused and len(self._buffer) <= LOW_WATER_MARK:
                self._paused = False
                self._rpc._send(encode_stream(self._id, self._mask | StreamFlag.RESUME))
            return chunk
        if self._error is not None:
            raise self._error
        if self._finished:
            return None
        self._waiter = asyncio.get_running_loop().create_future()
        try:
            return await self._waiter
        finally:
            self._waiter = None

    def __aiter__(self):
        return self._chunks()

    async def _chunks(self):
        while (chunk := await self.read()) is not None:
            yield chunk

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
        self._wake()
        self._rpc._incoming_streams.pop(self._id, None)

    def destroy(self, error=None):
        """Stop the stream and tell the peer to stop writing.

        ``error`` is an ``RPCRemoteError`` sent to the peer, which iteration
        then raises. Iteration ends instead if it is ``None``.
        """
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
        self._wake(error)
        self._rpc._incoming_streams.pop(self._id, None)

    def _abort(self, error):
        self._finished = True
        self._error = error
        self._wake(error)

    def _wake(self, error=None):
        if self._waiter is not None and not self._waiter.done():
            waiter = self._waiter
            self._waiter = None
            if error is None:
                waiter.set_result(None)
            else:
                waiter.set_exception(error)
