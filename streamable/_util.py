import logging
import sys
from typing import Any, AsyncIterable, Awaitable, Callable, Iterable, Optional, Type, TypeVar, Union

LOGGER = logging.getLogger("streamable")
LOGGER.propagate = False
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s: %(levelname)s: %(message)s")
handler.setFormatter(formatter)
LOGGER.addHandler(handler)
LOGGER.setLevel(logging.INFO)


T = TypeVar("T")
R = TypeVar("R")


def sidify(func: Callable[[T], Any]) -> Callable[[T], T]:
    def wrap(arg):
        func(arg)
        return arg

    return wrap


def reraise_as(
    func: Callable[[T], R], source: Type[Exception], target: Type[Exception]
) -> Callable[[T], R]:
    def wrap(arg):
        try:
            return func(arg)
        except source as e:
            raise target() from e

    return wrap


def validate_iterable(expected_iterable: Any) -> bool:
    """
    Raises:
        TypeError: If the expected_iterable does not implement __iter__ method.
    """
    if not isinstance(expected_iterable, Iterable):
        raise TypeError(
            f"Provided object of {type(expected_iterable)} is not an iterable because it does not implement the __iter__ method."
        )

def validate_aiterable(expected_aiterable: Any) -> bool:
    """
    Raises:
        TypeError: If the expected_iterable does not implement __aiter__ method.
    """
    if not isinstance(expected_aiterable, AsyncIterable):
        raise TypeError(
            f"Provided object is not an async iterable because it does not implement the __aiter__ method."
        )


def validate_concurrency(concurrency: int):
    if concurrency < 1:
        raise ValueError(
            f"`concurrency` should be greater or equal to 1, but got {concurrency}."
        )


def validate_group_size(size: Optional[int]):
    if size is not None and size < 1:
        raise ValueError(f"group's size should be None or >= 1 but got {size}.")


def validate_group_seconds(seconds: float):
    if seconds <= 0:
        raise ValueError(f"group's seconds should be > 0 but got {seconds}.")


def validate_slow_frequency(frequency: float):
    if frequency <= 0:
        raise ValueError(
            f"frequency is the maximum number of elements to yield per second, it must be > 0  but got {frequency}."
        )


def validate_limit_count(count: int):
    if count < 0:
        raise ValueError(f"limit's count must be positive but got {count}.")
    if count >= sys.maxsize:
        raise ValueError(
            f"limit's count must be less than sys.maxsize but got {count}."
        )


def colorize_in_red(s: str) -> str:
    return f"\033[91m{s}\033[0m"


def colorize_in_grey(s: str) -> str:
    return f"\033[90m{s}\033[0m"


def bold(s: str) -> str:
    return f"\033[1m{s}\033[0m"


def get_name(o: object):
    try:
        return o.__name__  # type: ignore
    except AttributeError:
        return o.__class__.__name__ + "(...)"

# async

import asyncio
from typing import AsyncIterator, Iterator, TypeVar

T = TypeVar("T")
U = TypeVar("U")

class AsyncIteratorToIterator(Iterator[T]):
    """
    Wraps an async iterator as a regular iterator.
    """

    def __init__(self, async_iterator: AsyncIterator[T]):
        self.async_iterator = async_iterator
    
    def __iter__(self) -> Iterator[T]:
        return self

    def __next__(self) -> T:
        try:
            return asyncio.run(anext(self.async_iterator))
        except StopAsyncIteration:
            raise StopIteration()

def running_coroutine(func: Union[Callable[[T], U], Callable[[T], Awaitable[U]]]) -> Callable[[T], U]:
    """
    Takes a function `func` and returns a function that does the same as `func` except when func would have returned
    an Awaitable, in that case it runs it and returns the result instead.
    """
    def f(arg: T):
        res = func(arg)
        if isinstance(res, Awaitable):
            return asyncio.run(res)
        return res
    return f

# def as_returning_coroutine(func: Union[Callable[[T], U], Callable[[T], Awaitable[U]]]) -> Callable[[T], Awaitable[U]]:
#     """
#     Takes a function `func` and returns a function that does the same as `func` except when it does not return
#     an awaitable, in that case it returns the result wrapped as an Awaitable.
#     """
#     return func
#     # async def f(arg: T):
#     #     res = func(arg)
#     #     if not isinstance(res, Awaitable):
#     #         async def const():
#     #             return res
#     #         return const()
#     #     return res
#     # return f