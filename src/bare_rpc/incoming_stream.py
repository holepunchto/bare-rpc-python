import asyncio
from collections import deque

from .constants import _DESTROY, _ERROR, _PAUSE, _RESUME
from .messages import RPCError, RPCRemoteError, encode_stream

HIGH_WATER_MARK = 16384


class IncomingStream:
    """Chunks from the peer.

    Iterating it with ``async for`` yields the chunks until the peer ends the
    stream. The peer is paused once ``HIGH_WATER_MARK`` bytes are buffered and
    resumed once fewer remain. A peer that buffers more than the
    ``max_buffered_bytes`` of the session fails it with an ``RPCError`` with
    code ``STREAM_OVERFLOW``.
    """

    def __init__(self, rpc, id, mask):
        self._rpc = rpc
        self._id = id
        self._mask = mask
        self._buffer = deque()
        self._buffered = 0
        self._paused = False
        self._finished = False
        self._error = None
        self._waiter = None

    async def read(self) -> bytes | None:
        """Read the next chunk from the peer, or ``None`` once the peer has
        ended the stream.

        Raises ``RPCRemoteError`` if the peer closes the stream with an error,
        the error of the session once it fails, and an ``RPCError`` with code
        ``CHANNEL_CLOSED`` once it closes. Raises ``RuntimeError`` if another
        ``read()`` is already waiting for a chunk, which is left waiting.
        """
        if self._buffer:
            chunk = self._buffer.popleft()
            self._buffered -= len(chunk)
            if self._paused and self._buffered < HIGH_WATER_MARK:
                self._paused = False
                self._rpc._send(encode_stream(self._id, self._mask | _RESUME))
            return chunk
        if self._error is not None:
            raise self._error
        if self._finished:
            return None
        if self._waiter is not None:
            raise RuntimeError("read() called while another read() is waiting")
        self._waiter = asyncio.get_running_loop().create_future()
        try:
            return await self._waiter
        finally:
            self._waiter = None

    def __aiter__(self) -> "IncomingStream":
        return self

    async def __anext__(self) -> bytes:
        chunk = await self.read()
        if chunk is None:
            raise StopAsyncIteration
        return chunk

    def _push(self, data):
        if self._finished:
            return
        if self._waiter is not None and not self._waiter.done():
            waiter = self._waiter
            self._waiter = None
            waiter.set_result(data)
            return
        self._buffer.append(data)
        self._buffered += len(data)
        if self._buffered > self._rpc._max_buffered_bytes:
            raise RPCError("Stream exceeds max_buffered_bytes", "STREAM_OVERFLOW")
        if not self._paused and self._buffered >= HIGH_WATER_MARK:
            self._paused = True
            self._rpc._send(encode_stream(self._id, self._mask | _PAUSE))

    def _end(self):
        if self._finished:
            return
        self._finished = True
        self._wake()
        self._rpc._incoming_streams.pop((self._id, self._mask), None)

    def destroy(self, error: RPCRemoteError | None = None) -> None:
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
                    self._mask | _DESTROY | _ERROR,
                    error=error,
                )
            )
        else:
            self._rpc._send(encode_stream(self._id, self._mask | _DESTROY))
        self._wake(error)
        self._rpc._incoming_streams.pop((self._id, self._mask), None)

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
