import logging
import sys
from typing import Any, Callable, Iterable, Iterator, Type, TypeVar, Union

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


def validate_iterable(expected_iterator: Any) -> bool:
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


def validate_concurrency(concurrency: int):
    if concurrency < 1:
        raise ValueError(
            f"`concurrency` should be greater or equal to 1, but got {concurrency}."
        )


def validate_batch_size(size: int):
    if size < 1:
        raise ValueError(f"batch's size should be >= 1 but got {size}.")


def validate_batch_seconds(seconds: float):
    if seconds <= 0:
        raise ValueError(f"batch's seconds should be > 0 but got {seconds}.")


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
    if o is None:
        return "None"
    try:
        return o.__name__  # type: ignore
    except AttributeError:
        return o.__class__.__name__ + "(...)"
