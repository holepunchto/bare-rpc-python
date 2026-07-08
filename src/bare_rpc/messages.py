from dataclasses import dataclass

import compact_encoding as cenc

from .constants import StreamFlag, Type


@dataclass(frozen=True)
class RPCRemoteError(Exception):
    message: str
    code: str
    errno: int


@dataclass(frozen=True)
class RequestMessage:
    id: int
    command: int
    stream: int
    data: bytes | None


@dataclass(frozen=True)
class ResponseMessage:
    id: int
    stream: int
    data: bytes | None
    error: RPCRemoteError | None


@dataclass(frozen=True)
class StreamMessage:
    id: int
    flags: int
    data: bytes | None
    error: RPCRemoteError | None


DecodedMessage = RequestMessage | ResponseMessage | StreamMessage


def _encode_frame(preencode, encode, msg) -> bytes:
    state = cenc.State()
    preencode(state, msg)
    state.allocate()
    encode(state, msg)
    body = bytes(state.buffer)
    return cenc.encode(cenc.uint32, len(body)) + body


def _preencode_request(state, msg):
    cenc.uint.preencode(state, Type.REQUEST)
    cenc.uint.preencode(state, msg.id)
    cenc.uint.preencode(state, msg.command)
    cenc.uint.preencode(state, msg.stream)
    if msg.stream == 0:
        cenc.buffer.preencode(state, msg.data or b"")


def _encode_request(state, msg):
    cenc.uint.encode(state, Type.REQUEST)
    cenc.uint.encode(state, msg.id)
    cenc.uint.encode(state, msg.command)
    cenc.uint.encode(state, msg.stream)
    if msg.stream == 0:
        cenc.buffer.encode(state, msg.data or b"")


def _decode_request(state):
    id = cenc.uint.decode(state)
    command = cenc.uint.decode(state)
    stream = cenc.uint.decode(state)
    data = cenc.buffer.decode(state) if stream == 0 else None
    return RequestMessage(id=id, command=command, stream=stream, data=data)


def encode_request(id, command, *, stream=0, data=None):
    return _encode_frame(
        _preencode_request, _encode_request, RequestMessage(id, command, stream, data)
    )


def encode_event(command, data=None):
    return encode_request(0, command, data=data)


def _preencode_response(state, msg):
    cenc.uint.preencode(state, Type.RESPONSE)
    cenc.uint.preencode(state, msg.id)
    is_error = msg.error is not None
    cenc.bool.preencode(state, is_error)
    cenc.uint.preencode(state, msg.stream)
    if is_error:
        cenc.utf8.preencode(state, msg.error.message)
        cenc.utf8.preencode(state, msg.error.code)
        cenc.int.preencode(state, msg.error.errno)
    elif msg.stream == 0:
        cenc.buffer.preencode(state, msg.data or b"")


def _encode_response(state, msg):
    cenc.uint.encode(state, Type.RESPONSE)
    cenc.uint.encode(state, msg.id)
    is_error = msg.error is not None
    cenc.bool.encode(state, is_error)
    cenc.uint.encode(state, msg.stream)
    if is_error:
        cenc.utf8.encode(state, msg.error.message)
        cenc.utf8.encode(state, msg.error.code)
        cenc.int.encode(state, msg.error.errno)
    elif msg.stream == 0:
        cenc.buffer.encode(state, msg.data or b"")


def _decode_response(state):
    id = cenc.uint.decode(state)
    is_error = cenc.bool.decode(state)
    stream = cenc.uint.decode(state)
    if is_error:
        message = cenc.utf8.decode(state)
        code = cenc.utf8.decode(state)
        errno = cenc.int.decode(state)
        error = RPCRemoteError(message=message, code=code, errno=errno)
        return ResponseMessage(id=id, stream=stream, data=None, error=error)
    data = cenc.buffer.decode(state) if stream == 0 else None
    return ResponseMessage(id=id, stream=stream, data=data, error=None)


def encode_response(id, *, stream=0, data=None):
    return _encode_frame(
        _preencode_response, _encode_response, ResponseMessage(id, stream, data, None)
    )


def encode_error_response(id, message, code, errno=0):
    error = RPCRemoteError(message=message, code=code, errno=errno)
    return _encode_frame(
        _preencode_response, _encode_response, ResponseMessage(id, 0, None, error)
    )


def _preencode_stream(state, msg):
    cenc.uint.preencode(state, Type.STREAM)
    cenc.uint.preencode(state, msg.id)
    cenc.uint.preencode(state, msg.flags)
    if msg.flags & StreamFlag.ERROR:
        cenc.utf8.preencode(state, msg.error.message)
        cenc.utf8.preencode(state, msg.error.code)
        cenc.int.preencode(state, msg.error.errno)
    elif msg.flags & StreamFlag.DATA:
        cenc.buffer.preencode(state, msg.data or b"")


def _encode_stream(state, msg):
    cenc.uint.encode(state, Type.STREAM)
    cenc.uint.encode(state, msg.id)
    cenc.uint.encode(state, msg.flags)
    if msg.flags & StreamFlag.ERROR:
        cenc.utf8.encode(state, msg.error.message)
        cenc.utf8.encode(state, msg.error.code)
        cenc.int.encode(state, msg.error.errno)
    elif msg.flags & StreamFlag.DATA:
        cenc.buffer.encode(state, msg.data or b"")


def _decode_stream(state):
    id = cenc.uint.decode(state)
    flags = cenc.uint.decode(state)
    if flags & StreamFlag.ERROR:
        message = cenc.utf8.decode(state)
        code = cenc.utf8.decode(state)
        errno = cenc.int.decode(state)
        error = RPCRemoteError(message=message, code=code, errno=errno)
        return StreamMessage(id=id, flags=flags, data=None, error=error)
    if flags & StreamFlag.DATA:
        data = cenc.buffer.decode(state)
        return StreamMessage(id=id, flags=flags, data=data, error=None)
    return StreamMessage(id=id, flags=flags, data=None, error=None)


def encode_stream(id, flags, *, data=None, error=None):
    if flags & StreamFlag.ERROR and error is None:
        raise ValueError("stream ERROR flag set but error is None")
    return _encode_frame(
        _preencode_stream, _encode_stream, StreamMessage(id, flags, data, error)
    )


def decode_frame(frame):
    state = cenc.State(frame)
    length = cenc.uint32.decode(state)
    if state.remaining < length:
        raise cenc.OutOfBounds("frame body shorter than declared length")
    type_ = cenc.uint.decode(state)
    if type_ == Type.REQUEST:
        return _decode_request(state)
    if type_ == Type.RESPONSE:
        return _decode_response(state)
    if type_ == Type.STREAM:
        return _decode_stream(state)
    return None
