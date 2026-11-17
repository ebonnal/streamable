import gc
from typing import Optional, Type, TypeVar

T = TypeVar("T")


def find_object(id_: int, type: Type[T]) -> Optional[T]:
    return next(
        (obj for obj in gc.get_objects() if id(obj) == id_ and isinstance(obj, type)),
        None,
    )
