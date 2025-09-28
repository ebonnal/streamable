from typing import Any, Type


def _class_getitem(item: Any) -> None:
    return None


def make_generic(cls: Type) -> None:
    cls.__class_getitem__ = _class_getitem
