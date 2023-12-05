from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from kioss._visit._base import AVisitor

V = TypeVar("V")


class AAcceptor(ABC):
    @abstractmethod
    def _accept(self, visitor: "AVisitor[V]") -> V:
        raise NotImplementedError()
