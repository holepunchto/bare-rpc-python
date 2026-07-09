import asyncio

from .constants import StreamFlag
from .messages import encode_stream


class OutgoingStream:
    def __init__(self, rpc, id, mask):
        self._rpc = rpc
        self._id = id
        self._mask = mask
        self._ended = False
        self._error = None
        self._corked = False
        self._uncork = asyncio.Event()
        self._uncork.set()  # start uncorked

    async def write(self, data):
        if self._ended:
            raise self._error or RuntimeError("stream ended")
        while self._corked and not self._ended:
            await self._uncork.wait()
        if self._ended:
            raise self._error or RuntimeError("stream ended")
        self._rpc._send(
            encode_stream(self._id, self._mask | StreamFlag.DATA, data=data)
        )

    async def end(self):
        if self._ended:
            return
        self._ended = True
        self._rpc._send(encode_stream(self._id, self._mask | StreamFlag.END))
        self._rpc._send(encode_stream(self._id, self._mask | StreamFlag.CLOSE))
        self._uncork.set()
        self._rpc._outgoing_streams.pop(self._id, None)

    async def destroy(self, error=None):
        if self._ended:
            return
        self._ended = True
        # An errorless destroy() sends a bare CLOSE, which the peer reader treats
        # as a clean end (StopAsyncIteration), not an abrupt error-close.
        if error is not None:
            self._rpc._send(
                encode_stream(
                    self._id,
                    self._mask | StreamFlag.CLOSE | StreamFlag.ERROR,
                    error=error,
                )
            )
        else:
            self._rpc._send(encode_stream(self._id, self._mask | StreamFlag.CLOSE))
        self._uncork.set()
        self._rpc._outgoing_streams.pop(self._id, None)

    def cork(self):
        self._corked = True
        self._uncork.clear()

    def uncork(self):
        self._corked = False
        self._uncork.set()

    def _abort(self, error):
        # Local teardown for _fail/close: mark ended + store the error and wake
        # a suspended write so it raises. Sends nothing (the connection is dead)
        # and does not touch the maps (the caller clears them).
        self._ended = True
        self._error = error
        self._uncork.set()
