# bare-rpc-python

Pure-Python port of the [bare-rpc](https://github.com/holepunchto/bare-rpc) message and frame wire codec, wire-compatible with the JavaScript reference. Built on [compact-encoding-python](https://github.com/holepunchto/compact-encoding-python).

## Usage

```python
import bare_rpc as rpc

frame = rpc.encode_request(1, 42, data=b"hi")   # -> bytes
msg = rpc.decode_frame(frame)                    # -> RequestMessage(...)
```
