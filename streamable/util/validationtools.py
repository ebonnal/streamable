import datetime
import sys
from typing import Any, Callable, Iterator, Optional, TypeVar

T = TypeVar("T")


def validate_iterator(iterator: Iterator):
    if not isinstance(iterator, Iterator):
        raise TypeError(
            f"`iterator` should be an Iterator, but got a {type(iterator)}."
        )


def validate_concurrency(concurrency: int) -> None:
    if concurrency < 1:
        raise ValueError(
            f"`concurrency` should be greater or equal to 1, but got {concurrency}."
        )


def validate_buffersize(buffersize: int) -> None:
    if buffersize < 1:
        raise ValueError(
            f"`buffersize` should be greater or equal to 1, but got {buffersize}."
        )


def validate_via(via: str) -> None:
    if via not in ["thread", "process"]:
        raise TypeError(f"`via` should be 'thread' or 'process', but got {repr(via)}.")


def validate_group_size(size: Optional[int]) -> None:
    if size is not None and size < 1:
        raise ValueError(f"`size` should be None or >= 1 but got {size}.")


def validate_group_interval(interval: Optional[datetime.timedelta]) -> None:
    if interval is not None and interval <= datetime.timedelta(0):
        raise ValueError(f"`interval` should be positive but got {repr(interval)}.")


def validate_count(count: int):
    if count < 0:
        raise ValueError(f"`count` must be >= 0 but got {count}.")
    if count >= sys.maxsize:
        raise ValueError(f"`count` must be less than sys.maxsize but got {count}.")


def validate_throttle_per_period(per_period_arg_name: str, value: int) -> None:
    if value < 1:
        raise ValueError(
            f"`{per_period_arg_name}` is the maximum number of elements to yield {' '.join(per_period_arg_name.split('_'))}, it must be >= 1  but got {value}."
        )


def validate_throttle_interval(interval: datetime.timedelta) -> None:
    if interval < datetime.timedelta(0):
        raise ValueError(
            f"`interval` is the minimum span of time between yields, it must not be negative but got {repr(interval)}."
        )


def validate_truncate_args(
    count: Optional[int] = None, when: Optional[Callable[[T], Any]] = None
) -> None:
    if count is None:
        if when is None:
            raise ValueError(f"`count` and `when` can't be both None.")
    else:
        validate_count(count)
