from streamable._stream import stream
from streamable._tools._star import star

Stream = stream

stream.__module__ = __name__
star.__module__ = __name__

__all__ = ["stream", "star"]

__version__ = "2.0.0rc7"
