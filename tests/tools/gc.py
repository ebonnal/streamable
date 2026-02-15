from contextlib import contextmanager
import gc
from typing import Optional, Type, TypeVar

T = TypeVar("T")

get_referees = gc.get_referents


@contextmanager
def disabled_gc():
    gc.disable()
    try:
        yield
    finally:
        gc.enable()


def find_object(id_: int, type: Type[T]) -> Optional[T]:
    return next(
        (obj for obj in gc.get_objects() if id(obj) == id_ and isinstance(obj, type)),
        None,
    )
