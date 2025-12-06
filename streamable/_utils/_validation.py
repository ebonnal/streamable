from concurrent.futures import Executor
import datetime
from contextlib import suppress
from inspect import iscoroutinefunction
from typing import (
    Any,
    Callable,
    TypeVar,
)

with suppress(ImportError):
    pass

T = TypeVar("T")


def validate_concurrency_executor(
    concurrency: Executor, fn: Callable[[Any], Any], fn_name: str
) -> None:
    if iscoroutinefunction(fn):
        raise TypeError(
            f"`concurrency` must be an int if `{fn_name}` is a coroutine function but got {repr(concurrency)}"
        )


def validate_positive_timedelta(_: datetime.timedelta, *, name: str) -> None:
    if _ <= datetime.timedelta(0):
        raise ValueError(f"`{name}` must be a positive timedelta but got {repr(_)}")


def validate_int(_: int, *, gte: int, name: str) -> None:
    if _ < gte:
        raise ValueError(f"`{name}` must be >= {gte} but got {_}")
