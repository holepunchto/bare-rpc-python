from .constants import StreamFlag
from .messages import RPCRemoteError, encode_error_response, encode_response
from .outgoing_stream import OutgoingStream


class IncomingEvent:
    def __init__(self, command, data):
        self.command = command
        self.data = data


class IncomingRequest:
    def __init__(self, rpc, id, command, data):
        self._rpc = rpc
        self.id = id
        self.command = command
        self.data = data
        self.request_stream = None
        self._replied = False

    async def reply(self, data=None):
        if self._replied:
            return
        self._replied = True
        self._rpc._send(encode_response(self.id, data=data))

    async def reject(self, error):
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
        outgoing = OutgoingStream(self._rpc, self.id, StreamFlag.RESPONSE)
        self._rpc._outgoing_streams[self.id] = outgoing
        self._rpc._send(encode_response(self.id, stream=StreamFlag.OPEN))
        return outgoing
