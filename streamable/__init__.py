from streamable._stream import stream
from streamable._tools._star import star
from streamable._tools._observation import Observation

Stream = stream

stream.__module__ = __name__
star.__module__ = __name__
Observation.__module__ = __name__

__all__ = ["stream", "star", "Observation"]

__version__ = "2.0.0rc12"
