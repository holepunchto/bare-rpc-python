import asyncio

from .constants import _CLOSE, _DATA, _END, _ERROR
from .messages import RPCRemoteError, encode_stream

_ACTIVE = "active"
_ENDING = "ending"
_CLOSED = "closed"


class OutgoingStream:
    """Chunks to the peer.

    Nothing is sent on it until the peer has opened its end of the stream.
    """

    def __init__(self, rpc, id, mask, *, opened=False):
        self._rpc = rpc
        self._id = id
        self._mask = mask
        self._phase = _ACTIVE
        # Once closed, the error that writes raise, or None to drop them.
        self._error = None
        self._opened = asyncio.Event()
        if opened:
            self._opened.set()
        self._uncorked = asyncio.Event()
        self._uncorked.set()

    async def write(self, data: bytes) -> None:
        """Send ``data`` to the peer and wait until it has been written to the
        transport, first waiting until the peer has opened its end of the
        stream and while it has paused the stream.

        ``data`` is dropped once ``end()`` has been called. Raises
        ``RuntimeError`` once the stream has been destroyed, and the error of
        the session once it has failed or closed.
        """
        if self._phase is not _ACTIVE:
            if self._error is not None:
                raise self._error
            return
        await self._opened.wait()
        while not self._uncorked.is_set() and self._phase is not _CLOSED:
            await self._uncorked.wait()
        if self._phase is _CLOSED:
            if self._error is not None:
                raise self._error
            return
        await self._rpc._send_and_wait(
            encode_stream(self._id, self._mask | _DATA, data=data)
        )

    async def end(self) -> None:
        """Finish the stream, ending the peer's iteration, once the peer has
        opened its end of the stream."""
        if self._phase is not _ACTIVE:
            return
        self._phase = _ENDING
        await self._opened.wait()
        if self._phase is not _ENDING:
            return
        self._close(None)
        self._rpc._send(encode_stream(self._id, self._mask | _END))

    def destroy(self, error: RPCRemoteError | None = None) -> None:
        """Close the stream early.

        ``error`` is an ``RPCRemoteError`` for the peer's iteration to raise.
        The peer's iteration ends instead if it is ``None``.
        """
        if self._phase is _CLOSED:
            return
        self._send_close(error, self._ended_error())

    def _release(self, error):
        self._send_close(error, None)

    def _send_close(self, error, write_error):
        self._close(write_error)
        if error is not None:
            self._rpc._send(
                encode_stream(self._id, self._mask | _CLOSE | _ERROR, error=error)
            )
        else:
            self._rpc._send(encode_stream(self._id, self._mask | _CLOSE))

    def _close(self, error):
        self._phase = _CLOSED
        self._error = error
        self._opened.set()
        self._uncorked.set()
        self._rpc._outgoing_streams.pop((self._id, self._mask), None)

    def _cork(self):
        self._uncorked.clear()

    def _uncork(self):
        self._uncorked.set()

    def _open(self):
        self._opened.set()

    # Closes the stream as a destroy of the peer would, but without telling the
    # peer, which has already dropped its end.
    def _drop(self):
        self._close(self._ended_error())

    def _ended_error(self):
        return None if self._phase is _ENDING else RuntimeError("Stream has ended")
