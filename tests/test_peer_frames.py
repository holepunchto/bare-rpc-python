import asyncio
import random

import pytest

from bare_rpc import (
    RPC,
    CancelMessage,
    ResponseMessage,
    RPCError,
    RPCRemoteError,
    StreamFlag,
    StreamMessage,
    decode_frame,
    encode_cancel,
    encode_error_response,
    encode_request,
    encode_response,
    encode_stream,
)
from pipe import End, pair

REQUEST = StreamFlag.REQUEST
RESPONSE = StreamFlag.RESPONSE
ERROR = RPCRemoteError("boom", "BOOM", 1)


def frame(body):
    return len(body).to_bytes(4, "little") + body


async def settle():
    for _ in range(20):
        await asyncio.sleep(0)


async def reply(req):
    await req.reply(b"ok")


class Gated(End):
    def __init__(self):
        super().__init__()
        self.gate = asyncio.Event()
        self.gate.set()

    async def write(self, data):
        await self.gate.wait()
        await super().write(data)


def gated_pair():
    local, peer = Gated(), End()
    local.peer, peer.peer = peer, local
    return local, peer


class Session:
    def __init__(
        self,
        *,
        on_request=reply,
        max_frame_size=1024,
        max_buffered_bytes=2**24,
        transport=pair,
    ):
        self.errors = []
        self.local, self.peer = transport()
        self.rpc = RPC(
            self.local,
            on_request=on_request,
            on_error=self.errors.append,
            max_frame_size=max_frame_size,
            max_buffered_bytes=max_buffered_bytes,
        )

    async def feed(self, *frames):
        for data in frames:
            if self.peer.closed:
                break
            await self.peer.write(data)
        await settle()

    def responses(self):
        return [
            message.id
            for message in map(decode_frame, self.local.written)
            if isinstance(message, ResponseMessage)
        ]

    async def works(self):
        await self.feed(encode_request(1000, 1, data=b"probe"))
        return 1000 in self.responses()

    def state(self):
        rpc = self.rpc
        return tuple(
            entries.copy()
            for entries in (
                rpc._pending,
                rpc._pending_response_streams,
                rpc._outgoing_streams,
                rpc._incoming_streams,
                rpc._unanswered,
            )
        )


def failure_of(*frames, on_request=reply):
    async def body():
        session = Session(on_request=on_request)
        for data in frames:
            await session.feed(data)
        await session.rpc.close()
        return session.errors, session.local.closed

    errors, closed = asyncio.run(asyncio.wait_for(body(), 2))
    assert closed
    assert len(errors) == 1
    assert isinstance(errors[0], RPCError)
    return errors[0]


def assert_ignored(*frames):
    async def body():
        session = Session()
        for data in frames:
            await session.feed(data)
        works = await session.works()
        state = session.state()
        await session.rpc.close()
        return session.errors, works, state

    errors, works, state = asyncio.run(asyncio.wait_for(body(), 2))
    assert errors == []
    assert works
    assert all(len(entries) == 0 for entries in state)


def test_an_oversized_frame_fails_on_its_length_prefix():
    async def body():
        session = Session(max_frame_size=64)
        pending = asyncio.create_task(session.rpc.request(1))
        await session.feed((65).to_bytes(4, "little"))
        with pytest.raises(RPCError) as err:
            await pending
        await session.rpc.close()
        return err.value, session.errors

    error, errors = asyncio.run(asyncio.wait_for(body(), 2))
    assert error.code == "FRAME_TOO_LARGE"
    assert errors == [error]


@pytest.mark.parametrize(
    "data",
    [
        frame(b"\x01"),
        frame(b"\x01\xfd\x01"),
        frame(b"\x01\x01\x05\x00\x05a"),
        frame(b"\x02\x01\x01\x00\x05a"),
        frame(b"\x03\x01\x81\x02\x05a"),
        frame(b"\x02\x01\x02\x00\x00"),
    ],
    ids=[
        "truncated",
        "bad varint",
        "data past frame",
        "error past frame",
        "stream error past frame",
        "boolean other than 0 or 1",
    ],
)
def test_an_undecodable_frame_fails_the_session(data):
    error = failure_of(data)
    assert error.code == "INVALID_MESSAGE"
    assert error.__cause__ is not None


def test_an_unknown_message_type_fails_the_session():
    error = failure_of(frame(b"\x63\x00"))
    assert error.code == "UNKNOWN_MESSAGE"


def test_invalid_utf_8_in_an_error_decodes_with_replacement_characters():
    async def body():
        session = Session()
        pending = asyncio.create_task(session.rpc.request(5))
        await settle()
        await session.feed(frame(b"\x02\x01\x01\x00\x02\xff\xfe\x01\xc3\x00"))
        with pytest.raises(RPCRemoteError) as err:
            await pending
        await session.rpc.close()
        return err.value, session.errors

    error, errors = asyncio.run(asyncio.wait_for(body(), 2))
    assert error == RPCRemoteError("\ufffd\ufffd", "\ufffd", 0)
    assert errors == []


def test_a_partial_frame_at_the_end_of_the_transport_fails_the_session():
    async def body():
        session = Session()
        await session.feed(encode_request(1, 5, data=b"x")[:-1])
        await session.peer.close()
        await settle()
        return session.errors

    errors = asyncio.run(asyncio.wait_for(body(), 2))
    assert len(errors) == 1
    assert isinstance(errors[0], RPCError)
    assert errors[0].code == "INVALID_MESSAGE"


def test_a_response_stream_works_when_its_open_precedes_the_request():
    async def on_request(req):
        out = await req.create_response_stream()
        await out.write(b"bar")
        await out.end()

    async def body():
        session = Session(on_request=on_request)
        await session.feed(
            encode_stream(1, RESPONSE | StreamFlag.OPEN),
            encode_request(1, 42, stream=StreamFlag.OPEN),
        )
        await session.rpc.close()
        return session.errors, session.local.written

    errors, sent = asyncio.run(asyncio.wait_for(body(), 2))
    assert errors == []
    assert sent == [
        encode_response(1, stream=StreamFlag.OPEN),
        encode_stream(1, RESPONSE | StreamFlag.DATA, data=b"bar"),
        encode_stream(1, RESPONSE | StreamFlag.END),
    ]


def test_a_response_stream_works_when_its_open_follows_the_request():
    responding = None

    async def on_request(req):
        await responding.wait()
        out = await req.create_response_stream()
        await out.end()

    async def body():
        nonlocal responding
        responding = asyncio.Event()
        session = Session(on_request=on_request)
        await session.feed(encode_request(1, 42, data=b"x"))
        await session.feed(encode_stream(1, RESPONSE | StreamFlag.OPEN))
        responding.set()
        await settle()
        await session.rpc.close()
        return session.errors, session.local.written

    errors, sent = asyncio.run(asyncio.wait_for(body(), 2))
    assert errors == []
    assert sent == [
        encode_response(1, stream=StreamFlag.OPEN),
        encode_stream(1, RESPONSE | StreamFlag.END),
    ]


def test_an_open_for_a_finished_request_is_ignored_and_keeps_no_state():
    async def body():
        session = Session()
        await session.feed(encode_request(1, 42, data=b"x"))
        await session.feed(encode_stream(1, RESPONSE | StreamFlag.OPEN))
        early = set(session.rpc._early_opens)
        await session.rpc.close()
        return session.errors, early

    assert asyncio.run(asyncio.wait_for(body(), 2)) == ([], set())


def test_early_opens_beyond_1024_fail_the_session():
    opens = [encode_stream(id, RESPONSE | StreamFlag.OPEN) for id in range(2000, 3025)]

    async def body():
        session = Session()
        await session.feed(*opens[:1024])
        works = await session.works()
        await session.feed(opens[1024])
        await session.rpc.close()
        return works, session.errors

    works, errors = asyncio.run(asyncio.wait_for(body(), 2))
    assert works
    assert len(errors) == 1
    assert errors[0].code == "INVALID_MESSAGE"


def test_chunks_before_a_request_stream_is_opened_are_dropped():
    async def on_request(req):
        chunks = [chunk async for chunk in req.request_stream]
        await req.reply(b"".join(chunks))

    async def body():
        session = Session(on_request=on_request)
        await session.feed(
            encode_request(1, 5, stream=StreamFlag.OPEN)
            + encode_stream(1, REQUEST | StreamFlag.DATA, data=b"early")
        )
        await session.feed(
            encode_stream(1, REQUEST | StreamFlag.DATA, data=b"late"),
            encode_stream(1, REQUEST | StreamFlag.END),
        )
        await session.rpc.close()
        return session.local.written

    assert asyncio.run(asyncio.wait_for(body(), 2)) == [
        encode_stream(1, REQUEST | StreamFlag.OPEN),
        encode_response(1, data=b"late"),
    ]


def test_a_request_stream_waits_for_the_peer_to_open_it():
    async def body():
        session = Session()
        outgoing, _ = await session.rpc.stream_request(5)
        writing = asyncio.create_task(outgoing.write(b"x"))
        await settle()
        before = list(session.local.written)
        await session.feed(encode_stream(1, REQUEST | StreamFlag.OPEN))
        await writing
        await outgoing.end()
        await settle()
        after = session.local.written[len(before) :]
        await session.rpc.close()
        return before, after

    before, after = asyncio.run(asyncio.wait_for(body(), 2))
    assert before == [encode_request(1, 5, stream=StreamFlag.OPEN)]
    assert after == [
        encode_stream(1, REQUEST | StreamFlag.DATA, data=b"x"),
        encode_stream(1, REQUEST | StreamFlag.END),
    ]


def test_ending_a_stream_waits_for_the_peer_to_open_it():
    async def body():
        session = Session()
        outgoing, _ = await session.rpc.stream_request(5)
        ending = asyncio.create_task(outgoing.end())
        await settle()
        before = list(session.local.written)
        await session.feed(encode_stream(1, REQUEST | StreamFlag.OPEN))
        await ending
        await settle()
        after = session.local.written[len(before) :]
        await session.rpc.close()
        return before, after

    before, after = asyncio.run(asyncio.wait_for(body(), 2))
    assert before == [encode_request(1, 5, stream=StreamFlag.OPEN)]
    assert after == [
        encode_stream(1, REQUEST | StreamFlag.END),
    ]


def test_a_response_stream_waits_for_the_peer_to_open_it():
    async def on_request(req):
        out = await req.create_response_stream()
        await out.write(b"x")
        await out.end()

    async def body():
        session = Session(on_request=on_request)
        await session.feed(encode_request(1, 5, data=b""))
        before = list(session.local.written)
        await session.feed(encode_stream(1, RESPONSE | StreamFlag.OPEN))
        after = session.local.written[len(before) :]
        await session.rpc.close()
        return before, after

    before, after = asyncio.run(asyncio.wait_for(body(), 2))
    assert before == [encode_response(1, stream=StreamFlag.OPEN)]
    assert after == [
        encode_stream(1, RESPONSE | StreamFlag.DATA, data=b"x"),
        encode_stream(1, RESPONSE | StreamFlag.END),
    ]


def test_destroying_a_stream_releases_a_write_waiting_for_the_open():
    async def body():
        session = Session()
        outgoing, _ = await session.rpc.stream_request(5)
        writing = asyncio.create_task(outgoing.write(b"x"))
        await settle()
        outgoing.destroy()
        with pytest.raises(RuntimeError):
            await writing
        await settle()
        sent = list(session.local.written)
        await session.rpc.close()
        return sent

    assert asyncio.run(asyncio.wait_for(body(), 2)) == [
        encode_request(1, 5, stream=StreamFlag.OPEN),
        encode_stream(1, REQUEST | StreamFlag.CLOSE),
    ]


def test_closing_releases_a_write_waiting_for_the_open():
    async def body():
        session = Session()
        outgoing, _ = await session.rpc.stream_request(5)
        writing = asyncio.create_task(outgoing.write(b"x"))
        await settle()
        await session.rpc.close()
        with pytest.raises(RPCError) as err:
            await writing
        return err.value

    assert asyncio.run(asyncio.wait_for(body(), 2)).code == "CHANNEL_CLOSED"


def test_the_reader_of_a_response_stream_opens_it_with_the_request():
    async def body():
        session = Session()
        response = asyncio.create_task(session.rpc.request_with_response_stream(5))
        await settle()
        sent = list(session.local.written)
        await session.rpc.close()
        await asyncio.gather(response, return_exceptions=True)
        return sent

    assert asyncio.run(asyncio.wait_for(body(), 2)) == [
        encode_request(1, 5, data=b""),
        encode_stream(1, RESPONSE | StreamFlag.OPEN),
    ]


def test_an_error_reply_after_a_response_stream_opens_fails_its_reader():
    async def body():
        session = Session()
        response = asyncio.create_task(session.rpc.request_with_response_stream(5))
        await settle()
        await session.feed(encode_response(1, stream=StreamFlag.OPEN))
        stream = await response
        await session.feed(encode_error_response(1, "boom", "BOOM", 1))
        with pytest.raises(RPCRemoteError) as err:
            await stream.read()
        await session.rpc.close()
        return err.value

    assert asyncio.run(asyncio.wait_for(body(), 2)) == ERROR


def test_a_response_stream_pauses_its_writer_by_buffered_bytes():
    chunk = bytes(4096)

    def flags(session):
        messages = map(decode_frame, session.local.written)
        return [
            message.flags
            for message in messages
            if isinstance(message, StreamMessage)
            and message.flags & (StreamFlag.PAUSE | StreamFlag.RESUME)
        ]

    async def body():
        session = Session(max_frame_size=8192)
        response = asyncio.create_task(session.rpc.request_with_response_stream(5))
        await settle()
        await session.feed(encode_response(1, stream=StreamFlag.OPEN))
        stream = await response
        await session.feed(
            *(
                encode_stream(1, RESPONSE | StreamFlag.DATA, data=chunk)
                for _ in range(3)
            )
        )
        below = flags(session)
        await session.feed(encode_stream(1, RESPONSE | StreamFlag.DATA, data=chunk))
        paused = flags(session)
        await stream.read()
        await settle()
        resumed = flags(session)
        await session.rpc.close()
        return below, paused, resumed

    below, paused, resumed = asyncio.run(asyncio.wait_for(body(), 2))
    assert below == []
    assert paused == [RESPONSE | StreamFlag.PAUSE]
    assert resumed == [RESPONSE | StreamFlag.PAUSE, RESPONSE | StreamFlag.RESUME]


def test_pending_work_fails_with_channel_closed_once_the_transport_ends():
    async def body():
        session = Session()
        rpc = session.rpc
        request = asyncio.create_task(rpc.request(5))
        response = asyncio.create_task(rpc.request_with_response_stream(5))
        await settle()
        await session.feed(encode_response(2, stream=StreamFlag.OPEN))
        stream = await response
        outgoing, _ = await rpc.stream_request(5)
        await session.feed(encode_stream(3, REQUEST | StreamFlag.OPEN))
        await session.peer.close()
        await settle()
        results = await asyncio.gather(
            request,
            stream.read(),
            outgoing.write(b"x"),
            rpc.request(5),
            return_exceptions=True,
        )
        return [getattr(result, "code", result) for result in results], session.errors

    codes, errors = asyncio.run(asyncio.wait_for(body(), 2))
    assert codes == ["CHANNEL_CLOSED"] * 4
    assert errors == []


def test_a_request_stream_handler_that_raises_rejects_the_request():
    async def on_request(req):
        raise RPCRemoteError("boom", "BOOM", 1)

    async def body():
        session = Session(on_request=on_request)
        await session.feed(encode_request(1, 5, stream=StreamFlag.OPEN))
        await session.rpc.close()
        return session.local.written

    assert asyncio.run(asyncio.wait_for(body(), 2)) == [
        encode_error_response(1, "boom", "BOOM", 1),
    ]


def test_a_handler_that_raises_after_answering_does_not_answer_again():
    async def on_request(req):
        await req.reply(b"ok")
        raise RuntimeError("late")

    async def body():
        session = Session(on_request=on_request)
        await session.feed(encode_request(1, 5, data=b""))
        await session.rpc.close()
        return session.errors, session.local.written

    assert asyncio.run(asyncio.wait_for(body(), 2)) == (
        [],
        [encode_response(1, data=b"ok")],
    )


def test_streams_that_the_peer_ends_without_closing_are_cleaned_up():
    async def on_request(req):
        chunks = [chunk async for chunk in req.request_stream]
        await req.reply(b"".join(chunks))

    async def body():
        session = Session(on_request=on_request)
        response = asyncio.create_task(session.rpc.request_with_response_stream(5))
        await settle()
        await session.feed(
            encode_response(1, stream=StreamFlag.OPEN),
            encode_stream(1, RESPONSE | StreamFlag.DATA, data=b"a"),
            encode_stream(1, RESPONSE | StreamFlag.END),
        )
        received = [chunk async for chunk in await response]
        await session.feed(encode_request(1, 5, stream=StreamFlag.OPEN))
        await session.feed(
            encode_stream(1, REQUEST | StreamFlag.DATA, data=b"b"),
            encode_stream(1, REQUEST | StreamFlag.END),
        )
        works = await session.works()
        state = session.state()
        await session.rpc.close()
        return session.errors, received, session.responses(), works, state

    errors, received, responses, works, state = asyncio.run(asyncio.wait_for(body(), 2))
    assert errors == []
    assert received == [b"a"]
    assert responses == [1, 1000]
    assert works
    assert all(len(entries) == 0 for entries in state)


def test_request_ids_count_up_without_reuse():
    async def body():
        session = Session()
        for _ in range(3):
            pending = asyncio.create_task(session.rpc.request(5))
            await settle()
            id = decode_frame(session.local.written[-1]).id
            await session.feed(encode_response(id, data=b""))
            await pending
        ids = [decode_frame(data).id for data in session.local.written]
        await session.rpc.close()
        return ids

    assert asyncio.run(asyncio.wait_for(body(), 2)) == [1, 2, 3]


@pytest.mark.parametrize(
    "data",
    [
        encode_response(0),
        encode_error_response(0, "boom", "BOOM"),
        encode_stream(0, REQUEST | StreamFlag.DATA, data=b"x"),
        encode_stream(1, REQUEST | RESPONSE | StreamFlag.END),
        encode_stream(1, StreamFlag.END),
        encode_stream(1, REQUEST),
        encode_stream(1, REQUEST | StreamFlag.ERROR, error=ERROR),
        encode_stream(1, REQUEST | StreamFlag.DATA | StreamFlag.ERROR, error=ERROR),
        encode_stream(1, REQUEST | StreamFlag.END | StreamFlag.PAUSE),
        encode_stream(1, REQUEST | StreamFlag.END | 0x400),
        encode_request(1, 5, stream=StreamFlag.CLOSE),
        encode_request(1, 5, stream=StreamFlag.OPEN | REQUEST),
        encode_request(0, 5, stream=StreamFlag.OPEN),
        encode_response(1, stream=StreamFlag.CLOSE),
        frame(b"\x02\x01\x01\x01\x00\x00\x00"),
    ],
    ids=[
        "response with id 0",
        "error response with id 0",
        "stream with id 0",
        "stream with both directions",
        "stream with no direction",
        "stream with no action",
        "stream with only an error",
        "stream with data and an error",
        "stream with two actions",
        "stream with an unknown bit",
        "request with an invalid stream",
        "request with a direction",
        "event with a stream",
        "response with an invalid stream",
        "error response with a stream",
    ],
)
def test_a_frame_that_a_valid_peer_never_sends_fails_the_session(data):
    assert failure_of(data).code == "INVALID_MESSAGE"


async def never_answer(req):
    await asyncio.Event().wait()


async def destroy_and_never_answer(req):
    req.request_stream.destroy()
    await asyncio.Event().wait()


async def keep_request_stream(req):
    assert req.request_stream is not None
    await req.reply(b"ok")


async def keep_response_stream(req):
    await req.create_response_stream()


@pytest.mark.parametrize(
    ("first", "on_request"),
    [
        (encode_request(1, 5, data=b"x"), never_answer),
        (encode_request(1, 5, stream=StreamFlag.OPEN), destroy_and_never_answer),
        (encode_request(1, 5, stream=StreamFlag.OPEN), keep_request_stream),
        (encode_request(1, 5, data=b"x"), keep_response_stream),
    ],
    ids=[
        "unanswered",
        "unanswered stream",
        "open request stream",
        "open response stream",
    ],
)
def test_a_request_that_reuses_a_live_id_fails_the_session(first, on_request):
    error = failure_of(first, encode_request(1, 5, data=b"x"), on_request=on_request)
    assert error.code == "INVALID_MESSAGE"


def test_a_request_may_reuse_the_id_of_a_finished_request():
    async def body():
        session = Session()
        await session.feed(encode_request(1, 5, data=b"x"))
        await session.feed(encode_request(1, 5, data=b"x"))
        await session.rpc.close()
        return session.errors, session.responses()

    assert asyncio.run(asyncio.wait_for(body(), 2)) == ([], [1, 1])


def test_frames_for_unknown_ids_are_ignored_and_keep_no_state():
    frames = [
        encode_response(7, data=b"x"),
        encode_response(7, stream=StreamFlag.OPEN),
        encode_error_response(7, "boom", "BOOM"),
    ]
    for direction in (REQUEST, RESPONSE):
        frames += [
            encode_stream(7, direction | StreamFlag.OPEN),
            encode_stream(7, direction | StreamFlag.DATA, data=b"x"),
            encode_stream(7, direction | StreamFlag.END),
            encode_stream(7, direction | StreamFlag.CLOSE),
            encode_stream(
                7, direction | StreamFlag.CLOSE | StreamFlag.ERROR, error=ERROR
            ),
            encode_stream(7, direction | StreamFlag.PAUSE),
            encode_stream(7, direction | StreamFlag.RESUME),
            encode_stream(7, direction | StreamFlag.DESTROY),
            encode_stream(
                7, direction | StreamFlag.DESTROY | StreamFlag.ERROR, error=ERROR
            ),
        ]

    assert_ignored(*frames)


def test_frames_for_finished_streams_are_ignored():
    async def body():
        session = Session()
        rpc = session.rpc

        response = asyncio.create_task(rpc.request_with_response_stream(5))
        await settle()
        outgoing, _ = await rpc.stream_request(5)

        await session.feed(
            encode_response(1, stream=StreamFlag.OPEN),
            encode_stream(1, RESPONSE | StreamFlag.DATA, data=b"a"),
            encode_stream(1, RESPONSE | StreamFlag.END),
            encode_stream(2, REQUEST | StreamFlag.DESTROY),
        )
        received = [chunk async for chunk in await response]

        await session.feed(
            encode_stream(1, RESPONSE | StreamFlag.DATA, data=b"b"),
            encode_stream(1, RESPONSE | StreamFlag.END),
            encode_stream(
                1, RESPONSE | StreamFlag.CLOSE | StreamFlag.ERROR, error=ERROR
            ),
            encode_stream(2, REQUEST | StreamFlag.PAUSE),
            encode_stream(2, REQUEST | StreamFlag.RESUME),
            encode_stream(2, REQUEST | StreamFlag.DESTROY, error=ERROR),
        )

        with pytest.raises(RuntimeError):
            await outgoing.write(b"late")

        works = await session.works()
        await rpc.close()
        return session.errors, received, works

    assert asyncio.run(asyncio.wait_for(body(), 2)) == ([], [b"a"], True)


ACTIONS = [
    StreamFlag.OPEN,
    StreamFlag.CLOSE,
    StreamFlag.CLOSE | StreamFlag.ERROR,
    StreamFlag.PAUSE,
    StreamFlag.RESUME,
    StreamFlag.DATA,
    StreamFlag.END,
    StreamFlag.DESTROY,
    StreamFlag.DESTROY | StreamFlag.ERROR,
]


def valid_frame(rng):
    id = rng.randint(1, 4)
    data = rng.randbytes(rng.randint(0, 8))
    flags = rng.choice([REQUEST, RESPONSE]) | rng.choice(ACTIONS)
    return rng.choice(
        [
            lambda: encode_request(id, 5, data=data),
            lambda: encode_request(id, 5, stream=StreamFlag.OPEN),
            lambda: encode_request(0, 5, data=data),
            lambda: encode_response(id, data=data),
            lambda: encode_response(id, stream=StreamFlag.OPEN),
            lambda: encode_error_response(id, "boom", "BOOM"),
            lambda: encode_stream(id, flags, data=data, error=ERROR),
        ]
    )()


def random_frame(rng):
    kind = rng.random()
    if kind < 0.05:
        return rng.randint(2000, 2**32 - 1).to_bytes(4, "little")
    if kind < 0.15:
        return frame(rng.randbytes(rng.randint(0, 12)))
    id = rng.randint(0, 4)
    flags = rng.choice(
        [
            rng.randint(0, 0x7FF),
            rng.choice([REQUEST, RESPONSE])
            | rng.choice([1, 2, 4, 8, 16, 32, 64, 0x82, 0xC0]),
        ]
    )
    data = rng.randbytes(rng.randint(0, 8))
    message = rng.choice(
        [
            lambda: encode_request(id, 5, stream=rng.choice([0, 1, flags]), data=data),
            lambda: encode_response(id, stream=rng.choice([0, 1, flags]), data=data),
            lambda: encode_error_response(id, "boom", "BOOM", rng.randint(-5, 5)),
            lambda: encode_stream(id, flags, data=data, error=ERROR),
        ]
    )()
    if kind < 0.5:
        return message
    body = bytearray(message[4:])
    for _ in range(rng.randint(1, 3)):
        mutation = rng.random()
        if mutation < 0.4 and body:
            body[rng.randrange(len(body))] = rng.randrange(256)
        elif mutation < 0.7 and body:
            del body[rng.randrange(len(body)) :]
        else:
            body += rng.randbytes(rng.randint(1, 4))
    return frame(bytes(body))


async def fuzz(rng):
    uncaught = []
    asyncio.get_running_loop().set_exception_handler(
        lambda loop, context: uncaught.append(context)
    )

    async def on_request(req):
        if req.request_stream is not None:
            req.request_stream.destroy()
        await req.reply(b"ok")

    session = Session(on_request=on_request)
    rpc = session.rpc
    ours = [
        asyncio.create_task(rpc.request(5)),
        asyncio.create_task(rpc.request_with_response_stream(5)),
    ]
    await settle()
    _, reply = await rpc.stream_request(5)
    ours.append(reply)

    generate = rng.choice([valid_frame, random_frame])
    for _ in range(rng.randint(1, 30)):
        await session.feed(generate(rng))

    if generate is valid_frame:
        assert session.errors == []

    if session.errors:
        assert len(session.errors) == 1
        assert isinstance(session.errors[0], RPCError)
    else:
        assert await session.works()

    pending, pending_streams, outgoing_streams, incoming_streams, unanswered = (
        session.state()
    )
    assert set(pending) <= {1, 3}
    assert set(pending_streams) <= {2}
    assert set(outgoing_streams) <= {(3, REQUEST)}
    assert set(incoming_streams) <= {(2, RESPONSE)}
    assert not unanswered
    assert rpc._early_opens <= {1, 2, 3, 4}

    await rpc.close()
    await asyncio.gather(*ours, return_exceptions=True)
    await settle()

    assert uncaught == []


def test_random_and_mutated_frames_fail_cleanly_or_keep_working():
    rng = random.Random(0x5EED)
    for _ in range(300):
        asyncio.run(asyncio.wait_for(fuzz(rng), 2))


def test_a_request_without_a_handler_is_tracked_until_answered():
    async def body():
        session = Session(on_request=None)
        await session.feed(
            encode_request(1, 5, data=b"x") + encode_request(1, 5, data=b"x")
        )
        await session.rpc.close()
        return session.errors

    errors = asyncio.run(asyncio.wait_for(body(), 2))
    assert [error.code for error in errors] == ["INVALID_MESSAGE"]


def test_a_streamed_request_without_a_handler_is_rejected():
    async def body():
        session = Session(on_request=None)
        await session.feed(encode_request(1, 5, stream=StreamFlag.OPEN))
        await session.rpc.close()
        return session.local.written

    [sent] = asyncio.run(asyncio.wait_for(body(), 2))
    assert decode_frame(sent).error.code == "UNKNOWN_COMMAND"


def test_a_stream_reply_to_a_plain_request_fails_it_and_destroys_the_stream():
    async def body():
        session = Session()
        pending = asyncio.create_task(session.rpc.request(5))
        await settle()
        await session.feed(encode_response(1, stream=StreamFlag.OPEN))
        with pytest.raises(RPCError) as err:
            await pending
        sent = session.local.written[1:]
        works = await session.works()
        await session.rpc.close()
        return err.value, sent, works, session.errors

    error, sent, works, errors = asyncio.run(asyncio.wait_for(body(), 2))
    assert error.code == "UNEXPECTED_STREAM"
    assert sent == [encode_stream(1, RESPONSE | StreamFlag.DESTROY)]
    assert works
    assert errors == []


@pytest.mark.parametrize(
    "answer, expected",
    [
        (encode_response(1), [encode_stream(1, REQUEST | StreamFlag.CLOSE)]),
        (
            encode_error_response(1, "boom", "BOOM", 1),
            [
                encode_stream(
                    1, REQUEST | StreamFlag.CLOSE | StreamFlag.ERROR, error=ERROR
                )
            ],
        ),
        (
            encode_response(1, stream=StreamFlag.OPEN),
            [
                encode_stream(1, RESPONSE | StreamFlag.DESTROY),
                encode_stream(
                    1,
                    REQUEST | StreamFlag.CLOSE | StreamFlag.ERROR,
                    error=RPCRemoteError(
                        "Expected a reply without a stream", "UNEXPECTED_STREAM", 0
                    ),
                ),
            ],
        ),
    ],
    ids=["reply", "error", "stream"],
)
def test_an_answer_closes_a_request_stream_the_peer_never_opened(answer, expected):
    async def body():
        session = Session()
        _, reply = await session.rpc.stream_request(5)
        await settle()
        await session.feed(answer)
        await asyncio.gather(reply, return_exceptions=True)
        sent = session.local.written[1:]
        await session.rpc.close()
        return sent

    assert asyncio.run(asyncio.wait_for(body(), 2)) == expected


def test_a_peer_that_ignores_pause_fails_the_session_with_stream_overflow():
    chunk = bytes(1000)

    async def body():
        session = Session(max_buffered_bytes=4000)
        response = asyncio.create_task(session.rpc.request_with_response_stream(5))
        await settle()
        await session.feed(encode_response(1, stream=StreamFlag.OPEN))
        stream = await response
        await session.feed(
            *(
                encode_stream(1, RESPONSE | StreamFlag.DATA, data=chunk)
                for _ in range(4)
            )
        )
        within = list(session.errors)
        await session.feed(encode_stream(1, RESPONSE | StreamFlag.DATA, data=b"x"))
        with pytest.raises(RPCError) as err:
            while True:
                await stream.read()
        await session.rpc.close()
        return within, session.errors, err.value

    within, errors, error = asyncio.run(asyncio.wait_for(body(), 2))
    assert within == []
    assert [error.code for error in errors] == ["STREAM_OVERFLOW"]
    assert error is errors[0]


def test_max_buffered_bytes_must_be_positive():
    async def body():
        with pytest.raises(ValueError):
            RPC(pair()[0], max_buffered_bytes=0)

    asyncio.run(body())


def test_a_request_stream_opens_when_the_handler_first_reads_it():
    reading = None

    async def on_request(req):
        await reading.wait()
        chunks = [chunk async for chunk in req.request_stream]
        await req.reply(b"".join(chunks))

    async def body():
        nonlocal reading
        reading = asyncio.Event()
        session = Session(on_request=on_request)
        await session.feed(
            encode_request(1, 5, stream=StreamFlag.OPEN),
            encode_stream(1, REQUEST | StreamFlag.DATA, data=b"early"),
        )
        before = list(session.local.written)
        reading.set()
        await settle()
        await session.feed(
            encode_stream(1, REQUEST | StreamFlag.DATA, data=b"late"),
            encode_stream(1, REQUEST | StreamFlag.END),
        )
        await session.rpc.close()
        return before, session.local.written

    before, sent = asyncio.run(asyncio.wait_for(body(), 2))
    assert before == []
    assert sent == [
        encode_stream(1, REQUEST | StreamFlag.OPEN),
        encode_response(1, data=b"late"),
    ]


def test_a_write_waits_until_its_frame_is_written_to_the_transport():
    async def body():
        session = Session(transport=gated_pair)
        outgoing, _ = await session.rpc.stream_request(5)
        await session.feed(encode_stream(1, REQUEST | StreamFlag.OPEN))
        session.local.gate.clear()
        writing = asyncio.create_task(outgoing.write(b"x"))
        await settle()
        waited = not writing.done()
        session.local.gate.set()
        await writing
        sent = session.local.written[-1]
        await session.rpc.close()
        return waited, sent

    waited, sent = asyncio.run(asyncio.wait_for(body(), 2))
    assert waited
    assert sent == encode_stream(1, REQUEST | StreamFlag.DATA, data=b"x")


def test_pending_and_later_writes_raise_the_error_that_failed_the_session():
    async def body():
        session = Session(transport=gated_pair)
        outgoing, _ = await session.rpc.stream_request(5)
        await session.feed(encode_stream(1, REQUEST | StreamFlag.OPEN))
        session.local.gate.clear()
        writing = asyncio.create_task(outgoing.write(b"x"))
        await settle()
        await session.feed(frame(b"\x63\x00"))
        with pytest.raises(RPCError) as pending:
            await writing
        with pytest.raises(RPCError) as later:
            await outgoing.write(b"y")
        session.local.gate.set()
        await session.rpc.close()
        return pending.value, later.value, session.errors

    pending, later, errors = asyncio.run(asyncio.wait_for(body(), 2))
    assert pending.code == "UNKNOWN_MESSAGE"
    assert pending is later is errors[0]


def test_a_cancel_with_id_0_fails_the_session():
    error = failure_of(encode_cancel(0, ERROR))
    assert error.code == "INVALID_MESSAGE"


def test_a_cancel_for_an_unknown_request_is_ignored():
    assert_ignored(encode_cancel(7, ERROR))


def test_a_cancelled_request_is_not_answered():
    writes = []

    async def on_request(req):
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            pass
        if req.command == 1:
            await req.reply(b"late")
        elif req.command == 2:
            stream = await req.create_response_stream()
            try:
                await stream.write(b"late")
            except RuntimeError as exc:
                writes.append(exc)
        else:
            raise RuntimeError("Late")

    async def body():
        session = Session(on_request=on_request)
        for request_id in (1, 2, 3):
            await session.feed(encode_request(request_id, request_id))
            await session.feed(encode_cancel(request_id, ERROR))
        written = list(session.local.written)
        state = session.state()
        await session.rpc.close()
        return written, state

    written, state = asyncio.run(asyncio.wait_for(body(), 2))
    assert written == []
    assert all(len(entries) == 0 for entries in state)
    assert len(writes) == 1


def test_a_cancel_ends_the_streams_of_the_request_without_frames():
    read = []
    drains = []

    async def drain(req, stream):
        try:
            async for _ in req.request_stream:
                pass
        except RPCRemoteError as exc:
            read.append(exc)
        try:
            await stream.write(b"late")
        except RuntimeError as exc:
            read.append(exc)

    async def on_request(req):
        stream = await req.create_response_stream()
        drains.append(asyncio.create_task(drain(req, stream)))

    async def body():
        session = Session(on_request=on_request)
        await session.feed(
            encode_request(1, 1, stream=StreamFlag.OPEN),
            encode_stream(1, RESPONSE | StreamFlag.OPEN),
        )
        before = len(session.local.written)
        await session.feed(encode_cancel(1, ERROR))
        await asyncio.gather(*drains)
        after = len(session.local.written)
        state = session.state()
        await session.rpc.close()
        return before, after, state

    before, after, state = asyncio.run(asyncio.wait_for(body(), 2))
    assert before == after
    assert all(len(entries) == 0 for entries in state)
    assert read[0] == ERROR
    assert isinstance(read[1], RuntimeError)


def cancels(session):
    return [
        message.id
        for message in map(decode_frame, session.local.written)
        if isinstance(message, CancelMessage)
    ]


def test_a_cancel_sets_the_error_of_a_handler_with_a_response_stream():
    observed = []

    async def on_request(req):
        await req.create_response_stream()
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            observed.append(req.cancellation_error)
            raise

    async def body():
        session = Session(on_request=on_request)
        await session.feed(encode_request(1, 1))
        await session.feed(encode_cancel(1, ERROR))
        await session.rpc.close()

    asyncio.run(asyncio.wait_for(body(), 2))
    assert observed == [ERROR]


def test_a_handler_that_replied_does_not_open_its_request_stream_after_a_cancel():
    read = []

    async def on_request(req):
        await req.reply(b"ok")
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            try:
                await req.request_stream.read()
            except RPCRemoteError as exc:
                read.append(exc)
            raise

    async def body():
        session = Session(on_request=on_request)
        await session.feed(encode_request(1, 1, stream=StreamFlag.OPEN))
        await session.feed(encode_cancel(1, ERROR))
        streams = [
            message
            for message in map(decode_frame, session.local.written)
            if isinstance(message, StreamMessage)
        ]
        await session.rpc.close()
        return streams

    streams = asyncio.run(asyncio.wait_for(body(), 2))
    assert read == [ERROR]
    assert streams == []


def test_a_cancel_that_crosses_the_open_of_a_response_stream_is_sent():
    async def body():
        session = Session()
        task = asyncio.create_task(session.rpc.request_with_response_stream(1))
        await settle()
        await session.peer.write(encode_response(1, stream=StreamFlag.OPEN))
        task.cancel()
        await settle()
        state = session.state()
        await session.rpc.close()
        return task, cancels(session), state

    task, sent, state = asyncio.run(asyncio.wait_for(body(), 2))
    assert task.cancelled()
    assert sent == [1]
    assert all(len(entries) == 0 for entries in state)


def test_a_cancel_that_crosses_the_reply_is_not_sent():
    async def body():
        session = Session()
        task = asyncio.create_task(session.rpc.request(1))
        await settle()
        await session.peer.write(encode_response(1, data=b"ok"))
        task.cancel()
        await settle()
        await session.rpc.close()
        return task, cancels(session)

    task, sent = asyncio.run(asyncio.wait_for(body(), 2))
    assert task.cancelled()
    assert sent == []


def test_a_timeout_that_fires_after_the_reply_is_not_sent():
    async def body():
        session = Session()
        task = asyncio.create_task(session.rpc.request(1, timeout=0))
        await asyncio.sleep(0)
        await session.peer.write(encode_response(1, data=b"ok"))
        result = await task
        await session.rpc.close()
        return result, cancels(session)

    result, sent = asyncio.run(asyncio.wait_for(body(), 2))
    assert result == b"ok"
    assert sent == []


def test_a_cancel_drops_the_answer_of_a_handler_that_already_returned():
    answered = asyncio.Event()
    observed = []

    async def answer_later(req):
        await asyncio.sleep(0.05)
        observed.append(req.cancellation_error)
        await req.reply(b"late")
        answered.set()

    async def on_request(req):
        observed.append(asyncio.create_task(answer_later(req)))

    async def body():
        session = Session(on_request=on_request)
        await session.feed(encode_request(1, 1))
        await session.feed(encode_cancel(1, ERROR))
        await answered.wait()
        responses = session.responses()
        await session.rpc.close()
        return responses

    assert asyncio.run(asyncio.wait_for(body(), 2)) == []
    assert observed[1] == ERROR
