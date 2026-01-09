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
    def visit_stream(self, s: "stream") -> V: ...
    # fmt: on

    def visit_buffer_stream(self, s: "BufferStream") -> V:
        return self.visit_stream(s)

    def visit_catch_stream(self, s: "CatchStream") -> V:
        return self.visit_stream(s)

    def visit_filter_stream(self, s: "FilterStream") -> V:
        return self.visit_stream(s)

    def visit_flatten_stream(self, s: "FlattenStream") -> V:
        return self.visit_stream(s)

    def visit_do_stream(self, s: "DoStream") -> V:
        return self.visit_stream(s)

    def visit_group_stream(self, s: "GroupStream") -> V:
        return self.visit_stream(s)

    def visit_observe_stream(self, s: "ObserveStream") -> V:
        return self.visit_stream(s)

    def visit_map_stream(self, s: "MapStream") -> V:
        return self.visit_stream(s)

    def visit_skip_stream(self, s: "SkipStream") -> V:
        return self.visit_stream(s)

    def visit_take_stream(self, s: "TakeStream") -> V:
        return self.visit_stream(s)

    def visit_throttle_stream(self, s: "ThrottleStream") -> V:
        return self.visit_stream(s)
