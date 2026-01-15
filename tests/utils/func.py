"""Test functions for creating sync and async transformations and side effects."""

import asyncio
import random
import time
from typing import Any, Callable, Coroutine, Iterator, Type, TypeVar, Union

from streamable._tools._async import AsyncFunction
from tests.utils.error import TestError
from tests.utils.source import N

T = TypeVar("T")
R = TypeVar("R")


def identity(x: T) -> T:
    """Identity function."""
    return x


async def async_identity(x: T) -> T:
    """Async identity function."""
    return x


def square(x):
    """Square a number."""
    return x**2


def inverse(n: Union[int, float]) -> float:
    """Inverse a number."""
    return 1 / n


async def async_square(x):
    """Async square function."""
    return x**2


def throw(exc: Union[Type[Exception], Exception]):
    """Raise an exception."""
    if isinstance(exc, Exception):
        raise exc
    else:
        raise exc()


def throw_func(exc: Type[Exception]) -> Callable[[T], T]:
    """Return a function that raises the given exception."""
    return lambda _: throw(exc)


def async_throw_func(exc: Type[Exception]) -> AsyncFunction[T, T]:
    """Return an async function that raises the given exception."""

    async def f(_: T) -> T:
        raise exc

    return f


def throw_for_odd_func(exc):
    """Return a function that raises exception for odd numbers."""
    return lambda i: throw(exc) if i % 2 == 1 else i


def async_throw_for_odd_func(exc):
    """Return an async function that raises exception for odd numbers."""

    async def f(i):
        return throw(exc) if i % 2 == 1 else i

    return f


SLOW_IDENTITY_DURATION = 0.05


def slow_identity(x: T) -> T:
    """Identity function that sleeps for a fixed duration."""
    time.sleep(SLOW_IDENTITY_DURATION)
    return x


async def async_slow_identity(x: T) -> T:
    """Async identity function that sleeps for a fixed duration."""
    await asyncio.sleep(SLOW_IDENTITY_DURATION)
    return x


def identity_sleep(seconds: float) -> float:
    """Sleep and return the seconds value."""
    time.sleep(seconds)
    return seconds


def inverse_sleep(seconds: float) -> float:
    """Sleep for 1/seconds and return it."""
    inv = inverse(seconds)
    time.sleep(inv)
    return inv


async def async_identity_sleep(seconds: float) -> float:
    """Async sleep and return the seconds value."""
    await asyncio.sleep(seconds)
    return seconds


async def async_inverse_sleep(seconds: float) -> float:
    """Sleep for 1/seconds and return it."""
    inv = inverse(seconds)
    await asyncio.sleep(inv)
    return inv


def randomly_slowed(
    func: Callable[[T], R], min_sleep: float = 0.001, max_sleep: float = 0.01
) -> Callable[[T], R]:
    """Wrap a function to add random sleep delay."""

    def wrap(x: T) -> R:
        time.sleep(min_sleep + random.random() * (max_sleep - min_sleep))
        return func(x)

    return wrap


def async_randomly_slowed(
    async_func: AsyncFunction[T, R],
    min_sleep: float = 0.001,
    max_sleep: float = 0.01,
) -> AsyncFunction[T, R]:
    """Wrap an async function to add random sleep delay."""

    async def wrap(x: T) -> R:
        await asyncio.sleep(min_sleep + random.random() * (max_sleep - min_sleep))
        return await async_func(x)

    return wrap


def range_raising_at_exhaustion(
    start: int, end: int, step: int, exception: Exception
) -> Iterator[int]:
    """Range iterator that raises exception after exhaustion."""
    yield from range(start, end, step)
    raise exception


def src_raising_at_exhaustion() -> Iterator[int]:
    """Source iterator that raises TestError after exhaustion."""
    return range_raising_at_exhaustion(0, N, 1, TestError())


def noarg_asyncify(fn: Callable[[], T]) -> Callable[[], Coroutine[Any, Any, T]]:
    async def wrap() -> T:
        return fn()

    return wrap
