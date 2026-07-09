import asyncio

import bare_rpc as rpc
from bare_rpc import RPC, RPCRemoteError


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


def test_handler_raise_becomes_error_response():
    class Boom(Exception):
        code = "BAD"
        errno = 42

    async def on_request(_req):
        raise Boom("kaboom")

    async def body():
        a, _b = make_pair(on_request_b=on_request)
        try:
            await a.request(5, b"x")
        except RPCRemoteError as err:
            return err
        raise AssertionError("expected RPCRemoteError")

    err = asyncio.run(body())
    assert err.message == "kaboom"
    assert err.code == "BAD"
    assert err.errno == 42


def test_decode_failure_poisons_and_rejects_pending():
    async def body():
        errors = []
        sent = []

        async def send(frame):
            sent.append(frame)

        r = RPC(send, on_error=errors.append)
        # start a request so there is a pending future to reject
        task = asyncio.ensure_future(r.request(5, b"x"))
        await asyncio.sleep(0)  # let the request register + send
        # feed a complete frame whose body overruns during decode: length=1,
        # body is just the REQUEST type byte, so decoding the request fields
        # runs out of bytes -> OutOfBounds -> poison. (A frame that is merely
        # short of its declared length is buffered, not a decode failure.)
        await r.receive((1).to_bytes(4, "little") + bytes([1]))
        try:
            await task
        except Exception as exc:  # noqa: BLE001
            first = exc
        assert errors  # on_error was called
        # subsequent request raises immediately; event is a no-op; receive is a no-op
        raised = None
        try:
            await r.request(1)
        except Exception as exc:  # noqa: BLE001
            raised = exc
        await r.event(1)  # no raise
        await r.receive(b"whatever")  # no raise
        return first, raised

    first, raised = asyncio.run(body())
    assert first is raised  # same stored failure error


def test_oversize_frame_poisons():
    async def body():
        errors = []

        async def send(_frame):
            pass

        r = RPC(send, on_error=errors.append, max_frame_size=8)
        # length prefix claims 100 bytes -> 4 + 100 > 8
        await r.receive((100).to_bytes(4, "little"))
        return errors

    errors = asyncio.run(body())
    assert len(errors) == 1
    assert isinstance(errors[0], ValueError)
    assert "max_frame_size" in str(errors[0])


def test_send_raise_poisons_and_propagates():
    async def body():
        errors = []

        async def send(_frame):
            raise OSError("transport down")

        r = RPC(send, on_error=errors.append)
        raised = None
        try:
            await r.request(5, b"x")
        except OSError as exc:
            raised = exc
        return errors, raised

    errors, raised = asyncio.run(body())
    assert isinstance(raised, OSError)
    assert errors and errors[0] is raised


def test_request_with_no_handler_is_unanswered():
    async def body():
        a, _b = make_pair()  # no on_request on b
        try:
            await asyncio.wait_for(a.request(5, b"x"), 0.2)
        except asyncio.TimeoutError:
            return "unanswered"
        return "answered"

    assert asyncio.run(body()) == "unanswered"


def test_event_returns_before_its_send_completes():
    order = []

    async def send(frame):
        order.append("send-start")
        await asyncio.sleep(0)
        order.append("send-done")

    async def body():
        r = RPC(send)
        await r.event(5, b"x")
        order.append("event-returned")
        for _ in range(20):
            if "send-done" in order:
                break
            await asyncio.sleep(0)
        return order

    result = asyncio.run(body())
    assert "send-done" in result
    assert result.index("event-returned") < result.index("send-done")


def test_frames_are_sent_in_call_order():
    sent = []

    async def send(frame):
        sent.append(frame)

    async def body():
        r = RPC(send)
        await r.event(1, b"a")
        task = asyncio.ensure_future(r.request(2, b"b"))
        for _ in range(20):
            if len(sent) >= 2:
                break
            await asyncio.sleep(0)
        task.cancel()
        return sent

    sent = asyncio.run(body())
    assert rpc.decode_frame(sent[0]).command == 1  # event enqueued first
    assert rpc.decode_frame(sent[1]).command == 2  # request enqueued second


def test_close_stops_the_writer_and_drops_later_sends():
    sent = []

    async def send(frame):
        sent.append(frame)

    async def body():
        r = RPC(send)
        assert r._writer_task is None
        await r.event(1, b"a")
        assert r._writer_task is not None  # writer starts on first send
        for _ in range(20):
            if sent:
                break
            await asyncio.sleep(0)
        r.close()
        await r.event(2, b"b")  # dropped (closed)
        for _ in range(20):
            await asyncio.sleep(0)
        return sent, r._writer_task

    sent, writer = asyncio.run(body())
    assert len(sent) == 1  # only the pre-close event was sent
    assert writer.done()  # writer exited after close


def test_request_after_close_raises_not_hangs():
    async def send(_frame):
        pass

    async def body():
        r = RPC(send)
        r.close()
        raised = None
        try:
            await asyncio.wait_for(r.request(1, b"x"), 0.5)
        except RuntimeError as exc:
            raised = exc
        except asyncio.TimeoutError:
            raised = "hung"
        return raised

    raised = asyncio.run(body())
    assert isinstance(raised, RuntimeError)


def test_close_rejects_in_flight_request():
    async def send(_frame):
        pass

    async def body():
        r = RPC(send)
        task = asyncio.ensure_future(r.request(1, b"x"))
        await asyncio.sleep(0)  # let it register + enqueue
        r.close()
        raised = None
        try:
            await asyncio.wait_for(task, 0.5)
        except RuntimeError as exc:
            raised = exc
        except asyncio.TimeoutError:
            raised = "hung"
        return raised

    raised = asyncio.run(body())
    assert isinstance(raised, RuntimeError)


def test_event_handler_error_poisons_the_connection():
    async def body():
        errors = []
        done = asyncio.Event()

        def on_event(_ev):
            raise ValueError("bad event")

        def on_error(err):
            errors.append(err)
            done.set()

        async def send(_frame):
            pass

        r = RPC(send, on_event=on_event, on_error=on_error)
        # an event frame (id 0) whose sync handler raises -> _run_event -> _fail
        await r.receive(rpc.encode_event(3, b"note"))
        await asyncio.wait_for(done.wait(), 1.0)
        # post-poison, a subsequent request raises the stored error
        raised = None
        try:
            await r.request(1)
        except Exception as exc:  # noqa: BLE001
            raised = exc
        return errors, raised

    errors, raised = asyncio.run(body())
    assert len(errors) == 1
    assert isinstance(raised, ValueError)
    assert str(raised) == "bad event"
