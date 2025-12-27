"""Test functions for creating sync and async transformations and side effects."""

import asyncio
import random
import time
from typing import Callable, Iterator, Type, TypeVar

from streamable._tools._async import AsyncFunction
from streamable._tools._iter import (
    AsyncIterator,
    Iterable,
    SyncAsyncIterable,
    SyncToAsyncIterator,
)
from tests.utils.error import TestError
from tests.utils.source import N

T = TypeVar("T")
R = TypeVar("R")


# ============================================================================
# Identity Functions
# ============================================================================


def identity(x: T) -> T:
    """Identity function."""
    return x


async def async_identity(x: T) -> T:
    """Async identity function."""
    return x


# ============================================================================
# Math Functions
# ============================================================================


def square(x):
    """Square a number."""
    return x**2


async def async_square(x):
    """Async square function."""
    return x**2


# ============================================================================
# Exception Functions
# ============================================================================


def throw(exc: Type[Exception]):
    """Raise an exception."""
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


# ============================================================================
# Slow Functions
# ============================================================================

slow_identity_duration = 0.05


def slow_identity(x: T) -> T:
    """Identity function that sleeps for a fixed duration."""
    time.sleep(slow_identity_duration)
    return x


async def async_slow_identity(x: T) -> T:
    """Async identity function that sleeps for a fixed duration."""
    await asyncio.sleep(slow_identity_duration)
    return x


# ============================================================================
# Randomly Slowed Functions
# ============================================================================


def randomly_slowed(
    func: Callable[[T], R], min_sleep: float = 0.001, max_sleep: float = 0.05
) -> Callable[[T], R]:
    """Wrap a function to add random sleep delay."""

    def wrap(x: T) -> R:
        time.sleep(min_sleep + random.random() * (max_sleep - min_sleep))
        return func(x)

    return wrap


def async_randomly_slowed(
    async_func: AsyncFunction[T, R],
    min_sleep: float = 0.001,
    max_sleep: float = 0.05,
) -> AsyncFunction[T, R]:
    """Wrap an async function to add random sleep delay."""

    async def wrap(x: T) -> R:
        await asyncio.sleep(min_sleep + random.random() * (max_sleep - min_sleep))
        return await async_func(x)

    return wrap


# ============================================================================
# Sleep Functions
# ============================================================================


def identity_sleep(seconds: float) -> float:
    """Sleep and return the seconds value."""
    time.sleep(seconds)
    return seconds


async def async_identity_sleep(seconds: float) -> float:
    """Async sleep and return the seconds value."""
    await asyncio.sleep(seconds)
    return seconds


# ============================================================================
# Iterator Functions
# ============================================================================


def range_raising_at_exhaustion(
    start: int, end: int, step: int, exception: Exception
) -> Iterator[int]:
    """Range iterator that raises exception after exhaustion."""
    yield from range(start, end, step)
    raise exception


def src_raising_at_exhaustion() -> Iterator[int]:
    """Source iterator that raises TestError after exhaustion."""
    return range_raising_at_exhaustion(0, N, 1, TestError())


class SyncToBiIterable(SyncAsyncIterable[T]):
    """Wrapper to make a sync iterable also async-iterable."""

    def __init__(self, iterable: Iterable[T]):
        self.iterable = iterable

    def __iter__(self) -> Iterator[T]:
        return self.iterable.__iter__()

    def __aiter__(self) -> AsyncIterator[T]:
        return SyncToAsyncIterator(self.iterable.__iter__())


sync_to_bi_iterable: Callable[[Iterable[T]], SyncAsyncIterable[T]] = SyncToBiIterable
