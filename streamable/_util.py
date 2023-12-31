import logging
from typing import Any, Callable, Iterable, Iterator, Type, TypeVar, Union

from typing_extensions import TypeGuard

LOGGER = logging.getLogger("streamable")
LOGGER.propagate = False
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
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


def map_exception(
    func: Callable[[T], R], source: Type[Exception], target: Type[Exception]
) -> Callable[[T], R]:
    def wrap(arg):
        try:
            return func(arg)
        except source as e:
            raise target() from e

    return wrap


def iterate(it: Union[Iterator[T], Iterable[T]]) -> None:
    for _ in it:
        pass


def identity(obj: T) -> T:
    return obj


def ducktype_assert_iterable(expected_iterator: Any) -> TypeGuard[Iterable]:
    """
    Raises:
        TypeError: If the expected_iterator does not implement __iter__ and __next__ methods.
    """
    try:
        expected_iterator.__iter__
    except AttributeError:
        raise TypeError(
            f"Provided object is not an iterator because it does not implement the __iter__ methods."
        )
    return True


def colorize_in_red(s: str) -> str:
    return f"\033[91m{s}\033[0m"


def colorize_in_grey(s: str) -> str:
    return f"\033[90m{s}\033[0m"


def bold(s: str) -> str:
    return f"\033[1m{s}\033[0m"


import threading


class LimitedYieldsIteratorWrapper(Iterator[T]):
    class NoYieldAvailable(Exception):
        pass

    def __init__(self, iterator: Iterator[T], initial_available_yields: int):
        self.iterator = iterator
        self._available_yields = initial_available_yields

    def __next__(self) -> T:
        while True:
            with threading.Lock():
                if self._available_yields > 0:
                    self._available_yields -= 1
                    return next(self.iterator)
            raise LimitedYieldsIteratorWrapper.NoYieldAvailable

    def increment_available_yields(self) -> None:
        with threading.Lock():
            self._available_yields += 1
