from dataclasses import dataclass


@dataclass(frozen=True)
class RPCRemoteError:
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
