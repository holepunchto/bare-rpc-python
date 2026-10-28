class Type:
    """Message type tags, the first field of every frame body."""

    REQUEST = 1
    """A request, or an event when its id is 0."""

    RESPONSE = 2
    """A response to a request."""

    STREAM = 3
    """A frame on an open stream."""


class StreamFlag:
    """Bits of the ``stream`` and ``flags`` fields.

    The low byte names the stream action and ``REQUEST`` and ``RESPONSE`` mark
    the direction the frame flows in.
    """

    OPEN = 0x1
    """Open a stream."""

    CLOSE = 0x2
    """Close a stream."""

    PAUSE = 0x4
    """Ask the sender to stop writing."""

    RESUME = 0x8
    """Ask the sender to continue writing."""

    DATA = 0x10
    """The frame carries payload data."""

    END = 0x20
    """The sender has no more data."""

    DESTROY = 0x40
    """Destroy a stream."""

    ERROR = 0x80
    """The frame carries an error."""

    REQUEST = 0x100
    """The frame flows in the request direction."""

    RESPONSE = 0x200
    """The frame flows in the response direction."""
