import asyncio

import pytest

from bare_rpc import RPC, CancelMessage, ResponseMessage, RPCError, decode_frame
from pipe import End, pair


async def settle():
    for _ in range(20):
        await asyncio.sleep(0)


def make_pair(on_request):
    left, right = pair()
    return RPC(left), RPC(right, on_request=on_request), left, right


def sent(end, kind):
    return [m for m in map(decode_frame, end.written) if isinstance(m, kind)]


def test_cancelling_the_awaiting_task_cancels_the_handler():
    async def body():
        started = asyncio.Event()
        observed = asyncio.get_running_loop().create_future()

        async def on_request(req):
            started.set()
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                observed.set_result(req.cancellation_error)
                await req.reply(b"late")
                raise

        client, server, left, right = make_pair(on_request)
        task = asyncio.create_task(client.request(1))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        error = await observed
        await settle()
        responses = sent(right, ResponseMessage)
        await client.close()
        return error, responses, sent(left, CancelMessage)

    error, responses, cancels = asyncio.run(asyncio.wait_for(body(), 2))
    assert error.code == "CANCELLED"
    assert responses == []
    assert [cancel.id for cancel in cancels] == [1]


def test_a_request_that_times_out_is_cancelled():
    async def body():
        observed = asyncio.get_running_loop().create_future()

        async def on_request(req):
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                observed.set_result(req.cancellation_error)
                raise

        client, *_ = make_pair(on_request)
        with pytest.raises(RPCError) as raised:
            await client.request(1, timeout=0.05)
        error = await observed
        await client.close()
        return raised.value, error

    raised, error = asyncio.run(asyncio.wait_for(body(), 2))
    assert raised.code == "TIMEOUT"
    assert error.code == "TIMEOUT"


def test_cancelling_the_reply_of_a_streamed_request_fails_its_stream():
    async def body():
        read = asyncio.get_running_loop().create_future()
        drains = []

        async def drain(req):
            try:
                async for _ in req.request_stream:
                    pass
            except Exception as exc:
                read.set_result(exc)

        async def on_request(req):
            drains.append(asyncio.create_task(drain(req)))

        client, *_ = make_pair(on_request)
        outgoing, reply = await client.stream_request(1)
        await outgoing.write(b"hi")
        reply.cancel()
        await asyncio.sleep(0)
        with pytest.raises(RPCError) as raised:
            await outgoing.write(b"more")
        error = await read
        await client.close()
        return raised.value, error

    raised, error = asyncio.run(asyncio.wait_for(body(), 2))
    assert raised.code == "CANCELLED"
    assert error.code == "CANCELLED"


def test_closing_the_session_cancels_the_handler():
    async def body():
        started = asyncio.Event()
        observed = asyncio.get_running_loop().create_future()

        async def on_request(req):
            started.set()
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                observed.set_result(req.cancellation_error)
                raise

        client, server, *_ = make_pair(on_request)
        task = asyncio.create_task(client.request(1))
        await started.wait()
        await server.close()
        error = await observed
        with pytest.raises(RPCError):
            await task
        return error

    assert asyncio.run(asyncio.wait_for(body(), 2)) is None


def test_closing_the_session_cancels_the_event_handler():
    async def body():
        started = asyncio.Event()
        cancelled = asyncio.Event()

        async def on_event(event):
            started.set()
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                cancelled.set()
                raise

        left, right = pair()
        client = RPC(left)
        server = RPC(right, on_event=on_event)
        await client.event(1)
        await started.wait()
        await server.close()
        await cancelled.wait()

    asyncio.run(asyncio.wait_for(body(), 2))


def test_a_handler_that_closes_the_session_is_cancelled_once_close_returns():
    async def body():
        steps = []
        done = asyncio.Event()

        async def on_request(req):
            try:
                await server.close()
                steps.append("closed")
                await asyncio.sleep(10)
                steps.append("not cancelled")
            except asyncio.CancelledError:
                steps.append("cancelled")
                raise
            finally:
                done.set()

        client, server, *_ = make_pair(on_request)
        task = asyncio.create_task(client.request(1))
        await done.wait()
        task.cancel()
        return steps

    assert asyncio.run(asyncio.wait_for(body(), 2)) == ["closed", "cancelled"]


class FailingClose(End):
    async def close(self):
        await super().close()
        raise OSError("Close failed")


def test_a_handler_is_cancelled_when_the_close_it_awaits_raises():
    async def body():
        steps = []
        done = asyncio.Event()

        async def on_request(req):
            try:
                await server.close()
            except OSError:
                steps.append("close failed")
            try:
                await asyncio.sleep(10)
                steps.append("not cancelled")
            except asyncio.CancelledError:
                steps.append("cancelled")
                raise
            finally:
                done.set()

        left, right = FailingClose(), End()
        left.peer, right.peer = right, left
        client = RPC(right)
        server = RPC(left, on_request=on_request)
        task = asyncio.create_task(client.request(1))
        await done.wait()
        task.cancel()
        return steps

    assert asyncio.run(asyncio.wait_for(body(), 2)) == ["close failed", "cancelled"]
