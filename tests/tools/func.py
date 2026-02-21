import asyncio
import random
import time
from typing import Any, Callable, Coroutine, Iterator, Type, TypeVar, Union

from streamable._tools._async import AsyncFunction
from tests.tools.error import TestError
from tests.tools.source import N

T = TypeVar("T")
R = TypeVar("R")


def identity(x: T) -> T:
    return x


async def async_identity(x: T) -> T:
    return x


def square(x):
    return x**2


def inverse(n: Union[int, float]) -> float:
    return 1 / n


async def async_square(x):
    return x**2


def throw(exc: Union[Type[Exception], Exception]):
    if isinstance(exc, Exception):
        raise exc
    else:
        raise exc()


def throw_func(exc: Type[Exception]) -> Callable[[T], T]:
    return lambda _: throw(exc)


def async_throw_func(exc: Type[Exception]) -> AsyncFunction[T, T]:
    async def f(_: T) -> T:
        raise exc

    return f


def throw_for_odd_func(exc):
    return lambda i: throw(exc) if i % 2 == 1 else i


def async_throw_for_odd_func(exc):
    async def f(i):
        return throw(exc) if i % 2 == 1 else i

    return f


SLOW_IDENTITY_DURATION = 0.05


def slow_identity(x: T) -> T:
    time.sleep(SLOW_IDENTITY_DURATION)
    return x


async def async_slow_identity(x: T) -> T:
    await asyncio.sleep(SLOW_IDENTITY_DURATION)
    return x


def identity_sleep(seconds: float) -> float:
    time.sleep(seconds)
    return seconds


def inverse_sleep(seconds: float) -> float:
    inv = inverse(seconds)
    time.sleep(inv)
    return inv


async def async_identity_sleep(seconds: float) -> float:
    await asyncio.sleep(seconds)
    return seconds


async def async_inverse_sleep(seconds: float) -> float:
    inv = inverse(seconds)
    await asyncio.sleep(inv)
    return inv


def randomly_slowed(
    func: Callable[[T], R], min_sleep: float = 0.001, max_sleep: float = 0.01
) -> Callable[[T], R]:
    def wrap(x: T) -> R:
        time.sleep(min_sleep + random.random() * (max_sleep - min_sleep))
        return func(x)

    return wrap


def async_randomly_slowed(
    async_func: AsyncFunction[T, R],
    min_sleep: float = 0.001,
    max_sleep: float = 0.01,
) -> AsyncFunction[T, R]:
    async def wrap(x: T) -> R:
        await asyncio.sleep(min_sleep + random.random() * (max_sleep - min_sleep))
        return await async_func(x)

    return wrap


def range_raising_at_exhaustion(
    start: int, end: int, step: int, exception: Exception
) -> Iterator[int]:
    yield from range(start, end, step)
    raise exception


def src_raising_at_exhaustion() -> Iterator[int]:
    return range_raising_at_exhaustion(0, N, 1, TestError())


def noarg_asyncify(fn: Callable[[], T]) -> Callable[[], Coroutine[Any, Any, T]]:
    async def wrap() -> T:
        return fn()

    return wrap
