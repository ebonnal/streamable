import gc
from contextlib import contextmanager
from typing import Optional, Type, TypeVar

T = TypeVar("T")


@contextmanager
def disabled_gc():
    gc.collect()
    gc.disable()
    try:
        yield
    finally:
        gc.enable()
        gc.collect()


def find_object(id_: int, type: Type[T]) -> Optional[T]:
    return next(
        (obj for obj in gc.get_objects() if id(obj) == id_ and isinstance(obj, type)),
        None,
    )
