import logging
import sys
from typing import Any, Callable, Coroutine, Optional, Type, TypeVar

_logger: Optional[logging.Logger] = None


def get_logger() -> logging.Logger:
    global _logger
    if not _logger:
        _logger = logging.getLogger("streamable")
        _logger.propagate = False
        _handler = logging.StreamHandler()
        _formatter = logging.Formatter("%(asctime)s: %(levelname)s: %(message)s")
        _handler.setFormatter(_formatter)
        _logger.addHandler(_handler)
        _logger.setLevel(logging.INFO)
    return _logger


class NoopStopIteration(Exception):
    pass


T = TypeVar("T")
R = TypeVar("R")


def sidify(func: Callable[[T], Any]) -> Callable[[T], T]:
    def wrap(arg: T):
        func(arg)
        return arg

    return wrap


def async_sidify(
    func: Callable[[T], Coroutine]
) -> Callable[[T], Coroutine[Any, Any, T]]:
    async def wrap(arg: T) -> T:
        coroutine = func(arg)
        if not isinstance(coroutine, Coroutine):
            raise TypeError(
                f"The function is expected to be an async function, i.e. it must be a function returning a Coroutine object, but returned a {type(coroutine)}."
            )
        await coroutine
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
        TypeError: If the expected_iterable does not implement __iter__ and __next__ methods.
    """
    try:
        expected_iterable.__iter__
    except AttributeError:
        raise TypeError(
            f"Provided object is not an iterable because it does not implement the __iter__ methods."
        )
    return True


def validate_concurrency(concurrency: int):
    if concurrency < 1:
        raise ValueError(
            f"`concurrency` should be greater or equal to 1, but got {concurrency}."
        )


def validate_group_size(size: Optional[int]):
    if size is not None and size < 1:
        raise ValueError(f"`size` should be None or >= 1 but got {size}.")


def validate_group_seconds(seconds: float):
    if seconds <= 0:
        raise ValueError(f"`seconds` should be > 0 but got {seconds}.")


def validate_slow_frequency(frequency: float):
    if frequency <= 0:
        raise ValueError(
            f"`frequency` is the maximum number of elements to yield per second, it must be > 0  but got {frequency}."
        )


def validate_truncate_args(
    count: Optional[int] = None, when: Optional[Callable[[T], Any]] = None
):
    if count is None:
        if when is None:
            raise ValueError(f"`count` and `when` can't be both None.")
    elif count < 0:
        raise ValueError(f"`count` must be positive but got {count}.")
    elif count >= sys.maxsize:
        raise ValueError(f"`count` must be less than sys.maxsize but got {count}.")
