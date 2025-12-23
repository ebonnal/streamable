"""Type definitions for test parametrization."""

from typing import AsyncIterable, Iterable, Type, Tuple, Union

IterableType = Union[Type[Iterable], Type[AsyncIterable]]
ITERABLE_TYPES: Tuple[IterableType, ...] = (Iterable, AsyncIterable)
