from collections.abc import Awaitable
from typing import Protocol

from .incoming import IncomingEvent, IncomingRequest, _unknown_command


class Handler(Protocol):
    """Answers the requests and receives the events of the peer of an ``RPC``.

    Pass it to ``RPC`` as ``handler``. Either method may be a coroutine
    function. A class that subclasses ``Handler`` can implement only one of
    them: By default, requests are rejected with an ``RPCRemoteError`` with
    code ``UNKNOWN_COMMAND`` and events are dropped.
    """

    def handle_request(self, request: IncomingRequest) -> Awaitable[None] | None:
        """Answer ``request``. Raising before it has been answered rejects it
        with the exception."""
        raise _unknown_command(request.command)

    def handle_event(self, event: IncomingEvent) -> Awaitable[None] | None:
        """Receive ``event``. Raising fails the session."""
