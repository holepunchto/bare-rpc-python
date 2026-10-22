from .constants import StreamFlag
from .messages import RPCRemoteError, encode_error_response, encode_response
from .outgoing_stream import OutgoingStream


class IncomingEvent:
    """An event from the peer.

    Attributes:
        command: Command number.
        data: Payload.
    """

    def __init__(self, command, data):
        self.command = command
        self.data = data


class IncomingRequest:
    """A request from the peer.

    Answer with ``reply``, ``reject`` or ``create_response_stream``. Later
    calls to ``reply`` and ``reject`` are ignored.

    Attributes:
        id: Request id.
        command: Command number.
        data: Payload, or None when the request opened a request stream.
        request_stream: An ``IncomingStream`` of the peer's chunks when the
            request opened a request stream, otherwise None.
    """

    def __init__(self, rpc, id, command, data):
        self._rpc = rpc
        self.id = id
        self.command = command
        self.data = data
        self.request_stream = None
        self._replied = False

    async def reply(self, data=None):
        """Answer the request with a payload.

        Args:
            data: Payload. None sends an empty payload.
        """
        if self._replied:
            return
        self._replied = True
        self._rpc._send(encode_response(self.id, data=data))

    async def reject(self, error):
        """Answer the request with an error.

        Args:
            error: An ``RPCRemoteError`` is sent as is. Any other exception is
                sent with ``str(error)`` as its message.
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
        """Answer the request by opening a response stream.

        Returns:
            An ``OutgoingStream`` to the peer.
        """
        self._replied = True
        outgoing = OutgoingStream(self._rpc, self.id, StreamFlag.RESPONSE)
        self._rpc._outgoing_streams[self.id] = outgoing
        self._rpc._send(encode_response(self.id, stream=StreamFlag.OPEN))
        return outgoing
