import asyncio

from bare_rpc import RPC, ResponseMessage, RPCRemoteError, decode_frame


def make_pair(*, on_request_a=None, on_request_b=None, on_event_a=None, sent_b=None):
    holder = {}

    async def send_a(frame):
        await holder["b"].receive(frame)

    async def send_b(frame):
        if sent_b is not None:
            sent_b.append(frame)
        await holder["a"].receive(frame)

    holder["a"] = RPC(send_a, on_request=on_request_a, on_event=on_event_a)
    holder["b"] = RPC(send_b, on_request=on_request_b)
    return holder["a"], holder["b"]


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


def test_reply_after_opening_a_response_stream_is_ignored():
    async def body():
        sent = []
        handled = asyncio.Event()
        flushed = asyncio.Event()

        async def on_request(req):
            out = await req.create_response_stream()
            await out.write(b"a")
            await out.end()
            await req.reply(b"late")
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


def test_response_stream_destroy_propagates():
    async def on_request(req):
        out = await req.create_response_stream()
        await out.write(b"a")
        await out.destroy(RPCRemoteError("gone", "E", 3))

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

    total = HIGH_WATER_MARK * 3
    writes = []

    async def on_request(req):
        out = await req.create_response_stream()
        for i in range(total):
            await out.write(bytes([i % 256]))
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
    assert received == [bytes([i % 256]) for i in range(total)]
