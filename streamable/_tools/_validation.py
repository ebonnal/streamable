from concurrent.futures import Executor
import datetime
from inspect import iscoroutinefunction
from typing import (
    Any,
    Callable,
)


def validate_concurrency_executor(
    concurrency: Executor, fn: Callable[[Any], Any], fn_name: str
) -> None:
    if iscoroutinefunction(fn):
        raise TypeError(
            f"`concurrency` must be an int if `{fn_name}` is a coroutine function but got {repr(concurrency)}"
        )


def validate_positive_timedelta(interval: datetime.timedelta, *, name: str) -> None:
    if interval <= datetime.timedelta(0):
        raise ValueError(
            f"`{name}` must be a positive timedelta but got {repr(interval)}"
        )


def validate_int(integer: int, *, gte: int, name: str) -> None:
    if integer < gte:
        raise ValueError(f"`{name}` must be >= {gte} but got {integer}")
