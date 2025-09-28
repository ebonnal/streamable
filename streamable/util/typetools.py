import types
from typing import Type


def make_generic(cls: Type) -> None:
    cls.__class_getitem__ = classmethod(types.GenericAlias)
