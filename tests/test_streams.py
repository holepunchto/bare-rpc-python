import asyncio

from bare_rpc import RPC, RPCRemoteError


def make_pair(*, on_request_a=None, on_request_b=None):
    holder = {}

    async def send_a(frame):
        await holder["b"].receive(frame)

    async def send_b(frame):
        await holder["a"].receive(frame)

    holder["a"] = RPC(send_a, on_request=on_request_a)
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


def test_backpressure_pause_resume_over_a_pair():
    from bare_rpc.incoming_stream import HIGH_WATER_MARK

    async def on_request(req):
        out = await req.create_response_stream()
        for i in range(HIGH_WATER_MARK + 2):
            await out.write(bytes([i]))
        await out.end()

    async def body():
        a, b = make_pair(on_request_b=on_request)
        stream = await a.request_with_response_stream(5)
        # do not consume yet; let the writer fill our buffer past the high-water mark
        for _ in range(50):
            await asyncio.sleep(0)
        received = [chunk async for chunk in stream]
        return received

    received = asyncio.run(asyncio.wait_for(body(), 2))
    assert len(received) == HIGH_WATER_MARK + 2
