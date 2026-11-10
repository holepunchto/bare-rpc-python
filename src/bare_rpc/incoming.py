from .constants import _OPEN, _RESPONSE
from .incoming_stream import IncomingStream
from .messages import RPCError, RPCRemoteError, encode_error_response, encode_response
from .outgoing_stream import OutgoingStream


def _unknown_command(command):
    return RPCRemoteError(f"Unknown command {command}", "UNKNOWN_COMMAND", 0)


def _remote_error(error):
    if isinstance(error, RPCRemoteError):
        return error
    return RPCRemoteError(
        str(error),
        getattr(error, "code", "") or "",
        getattr(error, "errno", 0) or 0,
    )


class IncomingEvent:
    """An event from the peer, carrying its ``command`` and ``data``."""

    def __init__(self, command: int, data: bytes) -> None:
        self.command = command
        self.data = data


class IncomingRequest:
    """A request from the peer, carrying its ``id``, ``command``, and ``data``.

    ``data`` is ``None`` when the request opened a request stream, in which
    case ``request_stream`` is an ``IncomingStream`` of the peer's chunks.

    Answer it once with ``reply()``, ``reject()``, or
    ``create_response_stream()``. Each of them raises an ``RPCError`` with code
    ``ALREADY_SENT`` once ``sent`` is true.
    """

    def __init__(
        self, rpc, id: int, command: int, data: bytes | None, *, streamed=False
    ) -> None:
        self._rpc = rpc
        self.id = id
        self.command = command
        self.data = data
        self._streamed = streamed
        self._request_stream = None
        self._response_stream = None
        self._sent = False
        self._cancellation_error = None
        self._task = None

    @property
    def request_stream(self) -> IncomingStream | None:
        """The ``IncomingStream`` of the peer's chunks, or ``None`` if the
        request did not open a request stream.

        Reading it for the first time opens the stream toward the peer, and
        chunks that the peer sends before then are dropped. Later reads return
        the same stream.
        """
        if self._streamed and self._request_stream is None:
            self._request_stream = self._rpc._open_request_stream(self)
            self._rpc._forget_request(self)
        return self._request_stream

    @property
    def sent(self) -> bool:
        """Whether the request has been answered."""
        return self._sent

    @property
    def cancellation_error(self) -> RPCRemoteError | None:
        """The ``RPCRemoteError`` that the peer cancelled the request with, or
        ``None`` if it has not.

        Once the peer has cancelled the request, the task of its handler is
        cancelled, and its streams end as if the peer had destroyed them with
        this error. Answering it sends nothing.
        """
        return self._cancellation_error

    async def reply(self, data: bytes | None = None) -> None:
        """Answer the request with ``data``, or an empty payload if ``None``."""
        self._answer()
        if self._cancellation_error is None:
            self._rpc._send(encode_response(self.id, data=data))

    async def reject(self, error: Exception) -> None:
        """Answer the request with ``error``.

        An ``RPCRemoteError`` is sent as is, and any other exception with
        ``str(error)`` as its message.
        """
        self._answer()
        if self._cancellation_error is None:
            error = _remote_error(error)
            self._rpc._send(
                encode_error_response(self.id, error.message, error.code, error.errno)
            )

    def _answer(self):
        if self._sent:
            raise RPCError("Response has already been sent", "ALREADY_SENT")
        self._sent = True
        # The peer may already reuse the id of a cancelled request.
        if self._cancellation_error is not None:
            return False
        return self._rpc._on_answered(self)

    async def create_response_stream(self) -> OutgoingStream:
        """Answer the request by opening a response stream and return its
        ``OutgoingStream``."""
        outgoing = OutgoingStream(self._rpc, self.id, _RESPONSE, opened=self._answer())
        self._response_stream = outgoing
        if self._cancellation_error is not None:
            outgoing._drop()
            return outgoing
        self._rpc._open_outgoing(outgoing)
        self._rpc._send(encode_response(self.id, stream=_OPEN))
        return outgoing

    async def _fail(self, error):
        if self._response_stream is not None:
            self._response_stream.destroy(_remote_error(error))
        elif not self._sent:
            await self.reject(error)
