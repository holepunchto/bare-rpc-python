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
