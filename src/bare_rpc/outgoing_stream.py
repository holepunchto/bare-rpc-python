import asyncio

from .constants import StreamFlag
from .messages import encode_stream


class OutgoingStream:
    """Chunks to the peer."""

    def __init__(self, rpc, id, mask):
        self._rpc = rpc
        self._id = id
        self._mask = mask
        self._ended = False
        self._error = None
        self._corked = False
        self._uncork = asyncio.Event()
        self._uncork.set()

    async def write(self, data):
        """Send ``data`` to the peer, waiting while the peer has paused the
        stream.

        Raises ``RuntimeError`` once the stream has ended or been destroyed, or
        the error of the session once it has failed.
        """
        if self._ended:
            raise self._error or RuntimeError("Stream has ended")
        while self._corked and not self._ended:
            await self._uncork.wait()
        if self._ended:
            raise self._error or RuntimeError("Stream has ended")
        self._rpc._send(
            encode_stream(self._id, self._mask | StreamFlag.DATA, data=data)
        )

    async def end(self):
        """Finish the stream, ending the peer's iteration."""
        if self._ended:
            return
        self._ended = True
        self._rpc._send(encode_stream(self._id, self._mask | StreamFlag.END))
        self._rpc._send(encode_stream(self._id, self._mask | StreamFlag.CLOSE))
        self._uncork.set()
        self._rpc._outgoing_streams.pop(self._id, None)

    async def destroy(self, error=None):
        """Close the stream early.

        ``error`` is an ``RPCRemoteError`` for the peer's iteration to raise.
        The peer's iteration ends instead if it is ``None``.
        """
        if self._ended:
            return
        self._ended = True
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
        self._ended = True
        self._error = error
        self._uncork.set()
