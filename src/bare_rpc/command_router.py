from collections.abc import Awaitable, Callable

from .handler import Handler
from .incoming import IncomingEvent, IncomingRequest, _unknown_command
from .rpc import _maybe_await

_RequestHandler = Callable[[IncomingRequest], bytes | None | Awaitable[bytes | None]]
_EventHandler = Callable[[IncomingEvent], Awaitable[None] | None]


class CommandRouter(Handler):
    """A ``Handler`` that sends each request and event to the handler
    registered for its command.

    Pass it to ``RPC`` as ``handler``.
    """

    def __init__(self) -> None:
        self._request_handlers: dict[int, _RequestHandler] = {}
        self._event_handlers: dict[int, _EventHandler] = {}

    def on_request(self, command: int, handler: _RequestHandler) -> None:
        """Answer requests for ``command`` with ``handler``, replacing any
        handler registered for it.

        ``handler`` is called with the ``IncomingRequest`` and returns the
        payload to reply with, unless it answers the request itself. It may be
        a coroutine function.
        """
        self._request_handlers[command] = handler

    def on_event(self, command: int, handler: _EventHandler) -> None:
        """Deliver events for ``command`` to ``handler``, replacing any handler
        registered for it.

        ``handler`` is called with the ``IncomingEvent`` and may be a coroutine
        function.
        """
        self._event_handlers[command] = handler

    async def handle_request(self, request: IncomingRequest) -> None:
        """Answer ``request`` with the handler for its command.

        Raises ``RPCRemoteError`` with code ``UNKNOWN_COMMAND`` if no
        handler is registered for it.
        """
        handler = self._request_handlers.get(request.command)

        if handler is None:
            raise _unknown_command(request.command)

        result = await _maybe_await(handler(request))

        if not request.sent:
            await request.reply(result)

    async def handle_event(self, event: IncomingEvent) -> None:
        """Deliver ``event`` to the handler for its command, or drop it if
        there is none."""
        handler = self._event_handlers.get(event.command)

        if handler is not None:
            await _maybe_await(handler(event))
