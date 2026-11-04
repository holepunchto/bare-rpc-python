import asyncio

import pytest

from bare_rpc import RPC, ResponseMessage, RPCError, RPCRemoteError, decode_frame
from pipe import pair


def make_pair(*, on_request_a=None, on_request_b=None, on_event_a=None, sent_b=None):
    left, right = pair()

    if sent_b is not None:
        right.written = sent_b

    a = RPC(left, on_request=on_request_a, on_event=on_event_a)
    b = RPC(right, on_request=on_request_b)
    return a, b


def test_response_stream_roundtrip():
    async def on_request(req):
        out = await req.create_response_stream()
        await out.write(b"a")
        await out.write(b"b")
        await out.end()

    async def body():
        a, b = make_pair(on_request_b=on_request)
        stream = await a.request_with_response_stream(5, b"hi")
        received = [chunk async for chunk in stream]
        return received, a._incoming_streams, b._outgoing_streams

    received, a_in, b_out = asyncio.run(asyncio.wait_for(body(), 2))
    assert received == [b"a", b"b"]
    assert len(a_in) == 0  # reader cleaned up on END
    assert len(b_out) == 0  # writer cleaned up on end()


def test_reply_after_opening_a_response_stream_raises_already_sent():
    errors = []

    async def body():
        sent = []
        handled = asyncio.Event()
        flushed = asyncio.Event()

        async def on_request(req):
            out = await req.create_response_stream()
            await out.write(b"a")
            await out.end()
            try:
                await req.reply(b"late")
            except RPCError as exc:
                errors.append(exc.code)
            handled.set()

        a, b = make_pair(
            on_request_b=on_request,
            on_event_a=lambda event: flushed.set(),
            sent_b=sent,
        )

        stream = await a.request_with_response_stream(5)
        received = [chunk async for chunk in stream]

        await handled.wait()
        await b.event(0)
        await flushed.wait()

        return received, sent

    received, sent = asyncio.run(body())

    responses = [
        message
        for message in map(decode_frame, sent)
        if isinstance(message, ResponseMessage)
    ]

    assert received == [b"a"]
    assert len(responses) == 1
    assert errors == ["ALREADY_SENT"]


def test_response_stream_destroy_propagates():
    async def on_request(req):
        out = await req.create_response_stream()
        await out.write(b"a")
        out.destroy(RPCRemoteError("gone", "E", 3))

    async def body():
        a, b = make_pair(on_request_b=on_request)
        stream = await a.request_with_response_stream(5)
        got = []
        raised = None
        try:
            async for chunk in stream:
                got.append(chunk)
        except RPCRemoteError as exc:
            raised = exc
        return got, raised

    got, raised = asyncio.run(asyncio.wait_for(body(), 2))
    assert got == [b"a"]
    assert raised == RPCRemoteError("gone", "E", 3)


def test_destroying_a_response_stream_stops_the_writer():
    async def body():
        stopped = asyncio.get_running_loop().create_future()

        async def on_request(req):
            out = await req.create_response_stream()
            try:
                while True:
                    await out.write(b"a")
                    await asyncio.sleep(0)
            except RuntimeError as exc:
                stopped.set_result(exc)

        a, b = make_pair(on_request_b=on_request)
        stream = await a.request_with_response_stream(5)
        await stream.read()
        stream.destroy()
        return await stopped

    error = asyncio.run(asyncio.wait_for(body(), 2))
    assert type(error) is RuntimeError


def test_streams_with_the_same_id_on_both_sides_stay_apart():
    async def body():
        responding = asyncio.Event()

        async def on_request_a(req):
            chunks = [chunk async for chunk in req.request_stream]
            await req.reply(b"".join(chunks))

        async def on_request_b(req):
            out = await req.create_response_stream()
            await responding.wait()
            await out.write(b"response")
            await out.end()

        a, b = make_pair(on_request_a=on_request_a, on_request_b=on_request_b)

        response = await a.request_with_response_stream(5)
        outgoing, reply = await b.stream_request(6)

        await outgoing.write(b"request")
        await outgoing.end()

        responding.set()

        return [chunk async for chunk in response], await reply

    assert asyncio.run(asyncio.wait_for(body(), 2)) == ([b"response"], b"request")


def test_a_plain_reply_to_a_streamed_request_is_read_as_one_chunk():
    async def on_request(req):
        await req.reply(req.data)

    async def body():
        a, b = make_pair(on_request_b=on_request)
        full = await a.request_with_response_stream(5, b"reply")
        empty = await a.request_with_response_stream(5)
        return [chunk async for chunk in full], [chunk async for chunk in empty]

    assert asyncio.run(asyncio.wait_for(body(), 2)) == ([b"reply"], [])


def test_reject_before_stream_opens_fails_the_caller():
    async def on_request(req):
        raise RPCRemoteError("nope", "NO", 4)  # unary handler path rejects

    async def body():
        a, b = make_pair(on_request_b=on_request)
        raised = None
        try:
            await a.request_with_response_stream(5)
        except RPCRemoteError as exc:
            raised = exc
        return raised

    raised = asyncio.run(asyncio.wait_for(body(), 2))
    assert raised == RPCRemoteError("nope", "NO", 4)


def test_request_stream_roundtrip():
    async def on_request(req):
        chunks = [chunk async for chunk in req.request_stream]
        await req.reply(b"".join(chunks))

    async def body():
        a, b = make_pair(on_request_b=on_request)
        outgoing, await_reply = await a.stream_request(5)
        await outgoing.write(b"x")
        await outgoing.write(b"y")
        await outgoing.end()
        reply = await await_reply
        return reply

    assert asyncio.run(asyncio.wait_for(body(), 2)) == b"xy"


def test_duplex_roundtrip():
    async def on_request(req):
        outgoing = await req.create_response_stream()
        async for chunk in req.request_stream:
            await outgoing.write(chunk + b"!")
        await outgoing.end()

    async def body():
        a, b = make_pair(on_request_b=on_request)
        outgoing, incoming = await a.create_bidirectional_stream(5)
        await outgoing.write(b"a")
        await outgoing.write(b"b")
        await outgoing.end()
        return [chunk async for chunk in incoming]

    assert asyncio.run(asyncio.wait_for(body(), 2)) == [b"a!", b"b!"]


def test_backpressure_throttles_producer_over_a_pair():
    from bare_rpc.incoming_stream import HIGH_WATER_MARK

    size = 1024
    total = HIGH_WATER_MARK * 3 // size
    writes = []

    async def on_request(req):
        out = await req.create_response_stream()
        for i in range(total):
            await out.write(bytes([i % 256]) * size)
            writes.append(i)
            await asyncio.sleep(0)  # yield so the peer's PAUSE can reach us and cork
        await out.end()

    async def body():
        a, b = make_pair(on_request_b=on_request)
        stream = await a.request_with_response_stream(5)
        # Let the producer run without consuming: backpressure must stall it
        # (cork) well before it writes all `total` chunks.
        for _ in range(60):
            await asyncio.sleep(0)
        stalled = len(writes)
        received = [chunk async for chunk in stream]
        return stalled, received

    stalled, received = asyncio.run(asyncio.wait_for(body(), 2))
    assert stalled < total  # producer was throttled (corked) before writing everything
    assert (
        len(received) == total
    )  # after RESUME, every chunk is delivered in-order, no loss
    assert received == [bytes([i % 256]) * size for i in range(total)]


def test_read_returns_none_once_the_stream_ends():
    async def on_request(req):
        out = await req.create_response_stream()
        await out.write(b"a")
        await out.end()

    async def body():
        a, _b = make_pair(on_request_b=on_request)
        stream = await a.request_with_response_stream(5)
        return await stream.read(), await stream.read(), await stream.read()

    assert asyncio.run(asyncio.wait_for(body(), 2)) == (b"a", None, None)


def test_a_handler_that_raises_after_opening_a_response_stream_destroys_it():
    async def on_request(req):
        out = await req.create_response_stream()
        await out.write(b"a")
        raise RPCRemoteError("boom", "BOOM", 1)

    async def body():
        sent = []
        a, b = make_pair(on_request_b=on_request, sent_b=sent)
        stream = await a.request_with_response_stream(5)
        got = [await stream.read()]
        with pytest.raises(RPCRemoteError) as err:
            await stream.read()
        await a.close()
        await b.close()
        return got, err.value, sent

    got, error, sent = asyncio.run(asyncio.wait_for(body(), 2))
    responses = [m for m in map(decode_frame, sent) if isinstance(m, ResponseMessage)]
    assert got == [b"a"]
    assert error.code == "BOOM"
    assert len(responses) == 1


def test_a_stream_reply_to_a_plain_request_releases_the_writer():
    async def body():
        released = asyncio.get_running_loop().create_future()

        async def on_request(req):
            out = await req.create_response_stream()
            try:
                await out.write(b"a")
            except RuntimeError as exc:
                released.set_result(exc)

        a, b = make_pair(on_request_b=on_request)
        with pytest.raises(RPCError) as err:
            await a.request(5)
        await released
        await a.close()
        await b.close()
        return err.value

    assert asyncio.run(asyncio.wait_for(body(), 2)).code == "UNEXPECTED_STREAM"


def test_a_reply_without_reading_the_request_stream_drops_the_writes():
    async def on_request(req):
        await req.reply(b"ok")

    async def body():
        a, b = make_pair(on_request_b=on_request)
        outgoing, reply = await a.stream_request(5)
        pending = asyncio.create_task(outgoing.write(b"x"))
        result = await reply
        await pending
        await outgoing.write(b"y")
        await outgoing.end()
        await a.close()
        await b.close()
        return result

    assert asyncio.run(asyncio.wait_for(body(), 2)) == b"ok"


def test_a_write_after_end_is_dropped():
    async def on_request(req):
        chunks = [chunk async for chunk in req.request_stream]
        await req.reply(b",".join(chunks))

    async def body():
        a, b = make_pair(on_request_b=on_request)
        outgoing, reply = await a.stream_request(5)
        await outgoing.write(b"a")
        await outgoing.end()
        await outgoing.write(b"b")
        result = await reply
        await a.close()
        await b.close()
        return result

    assert asyncio.run(asyncio.wait_for(body(), 2)) == b"a"


def test_a_peer_that_respects_pause_stays_within_max_buffered_bytes():
    chunk = bytes(1024)
    total = 256

    async def on_request(req):
        out = await req.create_response_stream()
        for _ in range(total):
            await out.write(chunk)
        await out.end()

    async def body():
        errors = []
        left, right = pair()
        a = RPC(left, on_error=errors.append, max_buffered_bytes=32 * 1024)
        b = RPC(right, on_request=on_request)
        stream = await a.request_with_response_stream(5)
        for _ in range(200):
            await asyncio.sleep(0)
        received = 0
        async for data in stream:
            received += len(data)
        await a.close()
        await b.close()
        return errors, received

    assert asyncio.run(asyncio.wait_for(body(), 5)) == ([], total * len(chunk))


def test_a_concurrent_read_raises_and_leaves_the_waiting_reader_alone():
    async def body():
        sending = asyncio.Event()

        async def on_request(req):
            out = await req.create_response_stream()
            await sending.wait()
            await out.write(b"a")
            await out.end()

        a, b = make_pair(on_request_b=on_request)
        stream = await a.request_with_response_stream(5)
        first = asyncio.create_task(stream.read())
        await asyncio.sleep(0)
        with pytest.raises(RuntimeError):
            await stream.read()
        sending.set()
        chunk = await first
        await a.close()
        await b.close()
        return chunk

    assert asyncio.run(asyncio.wait_for(body(), 2)) == b"a"
