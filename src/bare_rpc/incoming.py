from .constants import StreamFlag
from .messages import RPCRemoteError, encode_error_response, encode_response
from .outgoing_stream import OutgoingStream


class IncomingEvent:
    """An event from the peer, carrying its ``command`` and ``data``."""

    def __init__(self, command, data):
        self.command = command
        self.data = data


class IncomingRequest:
    """A request from the peer, carrying its ``id``, ``command``, and ``data``.

    ``data`` is ``None`` when the request opened a request stream, in which
    case ``request_stream`` is an ``IncomingStream`` of the peer's chunks.

    Answer it with ``reply()``, ``reject()``, or ``create_response_stream()``.
    Calls to ``reply()`` and ``reject()`` once it is answered are ignored.
    """

    def __init__(self, rpc, id, command, data):
        self._rpc = rpc
        self.id = id
        self.command = command
        self.data = data
        self.request_stream = None
        self._replied = False

    async def reply(self, data=None):
        """Answer the request with ``data``, or an empty payload if ``None``."""
        if self._replied:
            return
        self._replied = True
        self._rpc._send(encode_response(self.id, data=data))

    async def reject(self, error):
        """Answer the request with ``error``.

        An ``RPCRemoteError`` is sent as is, and any other exception with
        ``str(error)`` as its message.
        """
        if self._replied:
            return
        self._replied = True
        if isinstance(error, RPCRemoteError):
            message, code, errno = error.message, error.code, error.errno
        else:
            message = str(error)
            code = getattr(error, "code", "") or ""
            errno = getattr(error, "errno", 0) or 0
        self._rpc._send(encode_error_response(self.id, message, code, errno))

    async def create_response_stream(self):
        """Answer the request by opening a response stream and return its
        ``OutgoingStream``."""
        self._replied = True
        outgoing = OutgoingStream(self._rpc, self.id, StreamFlag.RESPONSE)
        self._rpc._outgoing_streams[self.id] = outgoing
        self._rpc._send(encode_response(self.id, stream=StreamFlag.OPEN))
        return outgoing
