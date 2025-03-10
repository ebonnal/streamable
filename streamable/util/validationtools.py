import datetime
from typing import Iterable, Iterator, Optional, TypeVar

T = TypeVar("T")


def validate_iterator(iterator: Iterator):
    if not isinstance(iterator, Iterator):
        raise TypeError(f"`iterator` must be an Iterator but got a {type(iterator)}")


def validate_iterable(iterable: Iterable):
    if not isinstance(iterable, Iterable):
        raise TypeError(f"`iterable` must be an Iterator but got a {type(iterable)}")


def validate_base(base: int):
    if base <= 0:
        raise ValueError(f"`base` must be > 0 but got {base}")


def validate_concurrency(concurrency: int) -> None:
    if concurrency < 1:
        raise ValueError(f"`concurrency` must be >= 1 but got {concurrency}")


def validate_buffersize(buffersize: int) -> None:
    if buffersize < 1:
        raise ValueError(f"`buffersize` must be >= 1 but got {buffersize}")


def validate_via(via: str) -> None:
    if via not in ["thread", "process"]:
        raise TypeError(f"`via` must be 'thread' or 'process' but got {repr(via)}")


def validate_group_size(size: Optional[int]) -> None:
    if size is not None and size < 1:
        raise ValueError(f"`size` must be None or >= 1 but got {size}")


def validate_group_interval(interval: Optional[datetime.timedelta]) -> None:
    if interval is not None and interval <= datetime.timedelta(0):
        raise ValueError(f"`interval` must None or > 0 but got {repr(interval)}")


def validate_count(count: int):
    if count < 0:
        raise ValueError(f"`count` must be >= 0 but got {count}")


def validate_positive_count(count: int):
    if count < 1:
        raise ValueError(f"`count` must be >= 1 but got {count}")


def validate_optional_count(count: Optional[int]):
    if count is not None:
        validate_count(count)


def validate_optional_positive_count(count: Optional[int]):
    if count is not None:
        validate_positive_count(count)


def validate_throttle_per(per: Optional[datetime.timedelta]) -> None:
    if per is not None and per < datetime.timedelta(0):
        raise ValueError(f"`per` must be >= 0 but got {repr(per)}")
