class Type:
    """Message type tags, the first field of every frame body.

    Attributes:
        REQUEST: A request, or an event when its id is 0.
        RESPONSE: A response to a request.
        STREAM: A frame on an open stream.
    """

    REQUEST = 1
    RESPONSE = 2
    STREAM = 3


class StreamFlag:
    """Bits of the ``stream`` and ``flags`` fields.

    The low byte names the stream action and ``REQUEST``/``RESPONSE`` mark the
    direction the frame flows in.

    Attributes:
        OPEN: Open a stream.
        CLOSE: Close a stream.
        PAUSE: Ask the sender to stop writing.
        RESUME: Ask the sender to continue writing.
        DATA: The frame carries payload data.
        END: The sender has no more data.
        DESTROY: Destroy a stream.
        ERROR: The frame carries an error.
        REQUEST: The frame flows in the request direction.
        RESPONSE: The frame flows in the response direction.
    """

    OPEN = 0x1
    CLOSE = 0x2
    PAUSE = 0x4
    RESUME = 0x8
    DATA = 0x10
    END = 0x20
    DESTROY = 0x40
    ERROR = 0x80
    REQUEST = 0x100
    RESPONSE = 0x200
