from dataclasses import dataclass

import compact_encoding as cenc

from .constants import _DATA, _ERROR, Type


class UnknownMessageTypeError(ValueError):
    """Raised by ``decode_frame`` for a type tag that ``Type`` does not
    define."""


class RPCError(RuntimeError):
    """An error of an RPC session, with a human-readable ``message`` and a
    machine-readable ``code``.

    The session raises it itself with these codes. The ``code`` is
    ``CHANNEL_CLOSED`` once the session or its transport has closed,
    ``ALREADY_SENT`` when a request is answered twice, and
    ``UNEXPECTED_STREAM`` when the peer answers a plain request by opening a
    response stream, ``TIMEOUT`` when a request is not answered within its
    timeout, and ``CANCELLED`` for the streams of a request whose call was
    cancelled. A frame from the peer fails the session with
    ``FRAME_TOO_LARGE`` if it exceeds ``max_frame_size``, with
    ``UNKNOWN_MESSAGE`` if its message type is not one that ``Type`` defines,
    with ``STREAM_OVERFLOW`` if it overflows ``max_buffered_bytes`` of an
    incoming stream, and with ``INVALID_MESSAGE`` if it does not decode, if
    the transport ends partway through it, or if a valid peer never sends
    it. The decoding error of an unknown or undecodable frame is chained
    as ``__cause__``.
    """

    def __init__(self, message: str, code: str) -> None:
        super().__init__(message)
        self.message = message
        self.code = code

    def __reduce__(self):
        return type(self), (self.message, self.code)


class RPCRemoteError(RPCError):
    """An error carried in an error response or a stream frame, with a
    human-readable ``message``, a machine-readable ``code``, and a numeric
    ``errno``.

    Two of them are equal if their fields are.
    """

    def __init__(self, message: str, code: str, errno: int = 0) -> None:
        super().__init__(message, code)
        self.errno = errno

    def __reduce__(self):
        return type(self), (self.message, self.code, self.errno)

    def __repr__(self) -> str:
        return (
            f"RPCRemoteError(message={self.message!r}, code={self.code!r}, "
            f"errno={self.errno!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, RPCRemoteError):
            return NotImplemented
        return (self.message, self.code, self.errno) == (
            other.message,
            other.code,
            other.errno,
        )

    def __hash__(self) -> int:
        return hash((self.message, self.code, self.errno))


@dataclass(frozen=True)
class RequestMessage:
    """A decoded request frame, or an event frame if ``id`` is 0.

    ``stream`` holds the ``StreamFlag`` bits, or 0 for a request without a
    stream, and ``data`` is ``None`` if the frame carries no payload.
    """

    id: int
    command: int
    stream: int
    data: bytes | None


@dataclass(frozen=True)
class ResponseMessage:
    """A decoded response frame to the request with ``id``.

    ``stream`` holds the ``StreamFlag`` bits, or 0 for a response without a
    stream, ``data`` is ``None`` if the frame carries no payload, and ``error``
    is the ``RPCRemoteError`` of an error response.
    """

    id: int
    stream: int
    data: bytes | None
    error: RPCRemoteError | None


@dataclass(frozen=True)
class StreamMessage:
    """A decoded stream frame of the request with ``id``.

    ``flags`` holds the ``StreamFlag`` bits, including the direction, ``data``
    is ``None`` if the frame carries no payload, and ``error`` is the
    ``RPCRemoteError`` under ``StreamFlag.ERROR``.
    """

    id: int
    flags: int
    data: bytes | None
    error: RPCRemoteError | None


@dataclass(frozen=True)
class CancelMessage:
    """A decoded cancel frame of the request with ``id``, carrying the
    ``RPCRemoteError`` that the request is cancelled with."""

    id: int
    error: RPCRemoteError


DecodedMessage = RequestMessage | ResponseMessage | StreamMessage | CancelMessage
"""A message returned by ``decode_frame``."""


def _decode_bool(state):
    value = cenc.uint8.decode(state)
    if value > 1:
        raise ValueError(f"Invalid boolean {value}")
    return value == 1


def _decode_error(state):
    message = cenc.utf8.decode(state)
    code = cenc.utf8.decode(state)
    errno = cenc.int.decode(state)
    return RPCRemoteError(message=message, code=code, errno=errno)


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


def encode_request(
    id: int, command: int, *, stream: int = 0, data: bytes | None = None
) -> bytes:
    """Encode a request frame, or an event frame if ``id`` is 0, including its
    length prefix.

    ``stream`` holds the ``StreamFlag`` bits, or 0 for a request without a
    stream. ``data`` is sent only without a stream, as an empty payload if
    ``None``.
    """
    return _encode_frame(
        _preencode_request, _encode_request, RequestMessage(id, command, stream, data)
    )


def encode_event(command: int, data: bytes | None = None) -> bytes:
    """Encode an event frame, a request with id 0 that gets no response,
    including its length prefix.

    ``data`` is sent as an empty payload if ``None``.
    """
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
    is_error = _decode_bool(state)
    stream = cenc.uint.decode(state)
    if is_error:
        error = _decode_error(state)
        return ResponseMessage(id=id, stream=stream, data=None, error=error)
    data = cenc.buffer.decode(state) if stream == 0 else None
    return ResponseMessage(id=id, stream=stream, data=data, error=None)


def encode_response(id: int, *, stream: int = 0, data: bytes | None = None) -> bytes:
    """Encode a response frame to the request with ``id``, including its
    length prefix.

    ``stream`` holds the ``StreamFlag`` bits, or 0 for a response without a
    stream. ``data`` is sent only without a stream, as an empty payload if
    ``None``.
    """
    return _encode_frame(
        _preencode_response, _encode_response, ResponseMessage(id, stream, data, None)
    )


def encode_error_response(id: int, message: str, code: str, errno: int = 0) -> bytes:
    """Encode a response frame that rejects the request with ``id``,
    including its length prefix.

    See ``RPCRemoteError`` for ``message``, ``code``, and ``errno``.
    """
    error = RPCRemoteError(message=message, code=code, errno=errno)
    return _encode_frame(
        _preencode_response, _encode_response, ResponseMessage(id, 0, None, error)
    )


def _preencode_stream(state, msg):
    cenc.uint.preencode(state, Type.STREAM)
    cenc.uint.preencode(state, msg.id)
    cenc.uint.preencode(state, msg.flags)
    if msg.flags & _ERROR:
        cenc.utf8.preencode(state, msg.error.message)
        cenc.utf8.preencode(state, msg.error.code)
        cenc.int.preencode(state, msg.error.errno)
    elif msg.flags & _DATA:
        cenc.buffer.preencode(state, msg.data or b"")


def _encode_stream(state, msg):
    cenc.uint.encode(state, Type.STREAM)
    cenc.uint.encode(state, msg.id)
    cenc.uint.encode(state, msg.flags)
    if msg.flags & _ERROR:
        cenc.utf8.encode(state, msg.error.message)
        cenc.utf8.encode(state, msg.error.code)
        cenc.int.encode(state, msg.error.errno)
    elif msg.flags & _DATA:
        cenc.buffer.encode(state, msg.data or b"")


def _decode_stream(state):
    id = cenc.uint.decode(state)
    flags = cenc.uint.decode(state)
    if flags & _ERROR:
        error = _decode_error(state)
        return StreamMessage(id=id, flags=flags, data=None, error=error)
    if flags & _DATA:
        data = cenc.buffer.decode(state)
        return StreamMessage(id=id, flags=flags, data=data, error=None)
    return StreamMessage(id=id, flags=flags, data=None, error=None)


def encode_stream(
    id: int,
    flags: int,
    *,
    data: bytes | None = None,
    error: RPCRemoteError | None = None,
) -> bytes:
    """Encode a stream frame of the request with ``id``, including its length
    prefix.

    ``flags`` holds the ``StreamFlag`` bits, including the direction. ``data``
    is sent only under ``StreamFlag.DATA``, as an empty payload if ``None``,
    and ``error`` under ``StreamFlag.ERROR``, which requires it.
    """
    flags = int(flags)
    if flags & _ERROR and error is None:
        raise ValueError("Stream flag ERROR requires an error")
    return _encode_frame(
        _preencode_stream, _encode_stream, StreamMessage(id, flags, data, error)
    )


def _preencode_cancel(state, msg):
    cenc.uint.preencode(state, Type.CANCEL)
    cenc.uint.preencode(state, msg.id)
    cenc.utf8.preencode(state, msg.error.message)
    cenc.utf8.preencode(state, msg.error.code)
    cenc.int.preencode(state, msg.error.errno)


def _encode_cancel(state, msg):
    cenc.uint.encode(state, Type.CANCEL)
    cenc.uint.encode(state, msg.id)
    cenc.utf8.encode(state, msg.error.message)
    cenc.utf8.encode(state, msg.error.code)
    cenc.int.encode(state, msg.error.errno)


def _decode_cancel(state):
    id = cenc.uint.decode(state)
    return CancelMessage(id=id, error=_decode_error(state))


def encode_cancel(id: int, error: RPCRemoteError) -> bytes:
    """Encode a cancel frame of the request with ``id``, including its length
    prefix.

    See ``RPCRemoteError`` for ``error``.
    """
    return _encode_frame(_preencode_cancel, _encode_cancel, CancelMessage(id, error))


def decode_frame(frame: bytes) -> DecodedMessage:
    """Decode one frame, including its length prefix, to a
    ``RequestMessage``, ``ResponseMessage``, ``StreamMessage``, or
    ``CancelMessage``.

    A field the frame does not carry decodes as ``None`` and an empty payload
    as ``b""``. Raises ``compact_encoding.OutOfBounds`` if the frame is
    truncated or a field runs past its length, ``UnknownMessageTypeError`` if
    its type tag is not one that ``Type`` defines, and ``ValueError`` for a
    boolean other than 0 or 1. Bytes in a string that are not valid UTF-8
    decode as replacement characters.
    """
    state = cenc.State(frame)
    length = cenc.uint32.decode(state)
    if state.remaining < length:
        raise cenc.OutOfBounds("Frame is shorter than its length")
    state.end = state.start + length
    type_ = cenc.uint.decode(state)
    if type_ == Type.REQUEST:
        return _decode_request(state)
    if type_ == Type.RESPONSE:
        return _decode_response(state)
    if type_ == Type.STREAM:
        return _decode_stream(state)
    if type_ == Type.CANCEL:
        return _decode_cancel(state)
    raise UnknownMessageTypeError(f"Unknown message type {type_}")
