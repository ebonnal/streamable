from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Generic, TypeVar

if TYPE_CHECKING:  # pragma: no cover
    from streamable._stream import (
        BufferStream,
        CatchStream,
        DoStream,
        FilterStream,
        FlattenStream,
        GroupStream,
        MapStream,
        ObserveStream,
        SkipStream,
        stream,
        ThrottleStream,
        TakeStream,
    )


V = TypeVar("V")


class Visitor(ABC, Generic[V]):
    __slots__ = ()

    # fmt: off
    @abstractmethod
    def visit_stream(self, stream: "stream") -> V: ...
    # fmt: on

    def visit_buffer_stream(self, stream: "BufferStream") -> V:
        return self.visit_stream(stream)

    def visit_catch_stream(self, stream: "CatchStream") -> V:
        return self.visit_stream(stream)

    def visit_filter_stream(self, stream: "FilterStream") -> V:
        return self.visit_stream(stream)

    def visit_flatten_stream(self, stream: "FlattenStream") -> V:
        return self.visit_stream(stream)

    def visit_do_stream(self, stream: "DoStream") -> V:
        return self.visit_stream(stream)

    def visit_group_stream(self, stream: "GroupStream") -> V:
        return self.visit_stream(stream)

    def visit_observe_stream(self, stream: "ObserveStream") -> V:
        return self.visit_stream(stream)

    def visit_map_stream(self, stream: "MapStream") -> V:
        return self.visit_stream(stream)

    def visit_skip_stream(self, stream: "SkipStream") -> V:
        return self.visit_stream(stream)

    def visit_take_stream(self, stream: "TakeStream") -> V:
        return self.visit_stream(stream)

    def visit_throttle_stream(self, stream: "ThrottleStream") -> V:
        return self.visit_stream(stream)
