import asyncio

import bare_rpc as rpc
from bare_rpc import RPC


def make_pair(
    *, on_request_a=None, on_event_a=None, on_request_b=None, on_event_b=None
):
    holder = {}

    async def send_a(frame):
        await holder["b"].receive(frame)

    async def send_b(frame):
        await holder["a"].receive(frame)

    holder["a"] = RPC(send_a, on_request=on_request_a, on_event=on_event_a)
    holder["b"] = RPC(send_b, on_request=on_request_b, on_event=on_event_b)
    return holder["a"], holder["b"]


def test_request_response_roundtrip():
    async def on_request(req):
        assert req.command == 5
        assert req.data == b"ping"
        await req.reply(b"pong")

    async def body():
        a, _b = make_pair(on_request_b=on_request)
        return await a.request(5, b"ping")

    assert asyncio.run(body()) == b"pong"


def test_empty_payload_roundtrips_to_empty_bytes():
    async def on_request(req):
        assert req.data == b""  # None/empty request payload decodes to b""
        await req.reply()  # empty response

    async def body():
        a, _b = make_pair(on_request_b=on_request)
        return await a.request(7, None)

    assert asyncio.run(body()) == b""


def test_event_delivery():
    async def body():
        got = asyncio.get_running_loop().create_future()

        def on_event(ev):
            got.set_result((ev.command, ev.data))

        a, _b = make_pair(on_event_b=on_event)
        await a.event(3, b"note")
        return await asyncio.wait_for(got, 1.0)

    assert asyncio.run(body()) == (3, b"note")


def test_concurrent_requests_resolve_independently():
    async def on_request(req):
        await req.reply(bytes([req.data[0] * 2]))

    async def body():
        a, _b = make_pair(on_request_b=on_request)
        results = await asyncio.gather(
            a.request(1, bytes([10])),
            a.request(1, bytes([20])),
            a.request(1, bytes([30])),
        )
        return results

    assert asyncio.run(body()) == [bytes([20]), bytes([40]), bytes([60])]


def test_receive_reassembles_split_and_coalesced_frames():
    async def body():
        seen = []
        done = asyncio.Event()

        async def on_request(req):
            seen.append((req.id, req.command, req.data))
            if len(seen) == 2:
                done.set()

        async def send(_frame):
            pass

        r = RPC(send, on_request=on_request)
        f1 = rpc.encode_request(1, 5, data=b"hi")
        f2 = rpc.encode_request(2, 6, data=b"yo")
        # f1 split across two receives, then f2 coalesced with nothing extra
        await r.receive(f1[:3])
        await r.receive(f1[3:] + f2)
        await asyncio.wait_for(done.wait(), 1.0)
        return seen

    assert asyncio.run(body()) == [(1, 5, b"hi"), (2, 6, b"yo")]
