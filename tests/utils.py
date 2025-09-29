import asyncio
import random
import time
import timeit
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Callable,
    Coroutine,
    Iterable,
    Iterator,
    List,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from streamable.stream import Stream
from streamable.util.asynctools import awaitable_to_coroutine
from streamable.util.iterabletools import BiIterable

T = TypeVar("T")
R = TypeVar("R")

IterableType = Union[Type[Iterable], Type[AsyncIterable]]
ITERABLE_TYPES: Tuple[IterableType, ...] = (Iterable, AsyncIterable)


async def _aiter_to_list(aiterable: AsyncIterable[T]) -> List[T]:
    return [elem async for elem in aiterable]


def aiterable_to_list(aiterable: AsyncIterable[T]) -> List[T]:
    return asyncio.run(_aiter_to_list(aiterable))


def stopiteration_for_iter_type(itype: IterableType) -> Type[Exception]:
    if issubclass(itype, AsyncIterable):
        return StopAsyncIteration
    return StopIteration


def to_list(stream: Stream[T], itype: IterableType) -> List[T]:
    assert isinstance(stream, Stream)
    if itype is AsyncIterable:
        return aiterable_to_list(stream)
    else:
        return list(stream)


def bi_iterable_to_iter(
    iterable: Union[BiIterable[T], Stream[T]], itype: IterableType
) -> Union[Iterator[T], AsyncIterator[T]]:
    if itype is AsyncIterable:
        return iterable.__aiter__()
    else:
        return iter(iterable)


def anext_or_next(it: Union[Iterator[T], AsyncIterator[T]]) -> T:
    if isinstance(it, AsyncIterator):
        return asyncio.run(awaitable_to_coroutine(it.__anext__()))
    else:
        return next(it)


def alist_or_list(iterable: Union[Iterable[T], AsyncIterable[T]]) -> List[T]:
    if isinstance(iterable, AsyncIterable):
        return aiterable_to_list(iterable)
    else:
        return list(iterable)


def timestream(
    stream: Stream[T], times: int = 1, itype: IterableType = Iterable
) -> Tuple[float, List[T]]:
    res: List[T] = []

    def iterate():
        nonlocal res
        res = to_list(stream, itype=itype)

    return timeit.timeit(iterate, number=times) / times, res


def identity_sleep(seconds: float) -> float:
    time.sleep(seconds)
    return seconds


async def async_identity_sleep(seconds: float) -> float:
    await asyncio.sleep(seconds)
    return seconds


# simulates an I/0 bound function
slow_identity_duration = 0.05


def slow_identity(x: T) -> T:
    time.sleep(slow_identity_duration)
    return x


async def async_slow_identity(x: T) -> T:
    await asyncio.sleep(slow_identity_duration)
    return x


def identity(x: T) -> T:
    return x


# fmt: off
async def async_identity(x: T) -> T: return x
# fmt: on


def square(x):
    return x**2


async def async_square(x):
    return x**2


def throw(exc: Type[Exception]):
    raise exc()


def throw_func(exc: Type[Exception]) -> Callable[[T], T]:
    return lambda _: throw(exc)


def async_throw_func(exc: Type[Exception]) -> Callable[[T], Coroutine[Any, Any, T]]:
    async def f(_: T) -> T:
        raise exc

    return f


def throw_for_odd_func(exc):
    return lambda i: throw(exc) if i % 2 == 1 else i


def async_throw_for_odd_func(exc):
    async def f(i):
        return throw(exc) if i % 2 == 1 else i

    return f


class TestError(Exception):
    pass


DELTA_RATE = 0.4
# size of the test collections
N = 256

src = range(N)

even_src = range(0, N, 2)


def randomly_slowed(
    func: Callable[[T], R], min_sleep: float = 0.001, max_sleep: float = 0.05
) -> Callable[[T], R]:
    def wrap(x: T) -> R:
        time.sleep(min_sleep + random.random() * (max_sleep - min_sleep))
        return func(x)

    return wrap


def async_randomly_slowed(
    async_func: Callable[[T], Coroutine[Any, Any, R]],
    min_sleep: float = 0.001,
    max_sleep: float = 0.05,
) -> Callable[[T], Coroutine[Any, Any, R]]:
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
