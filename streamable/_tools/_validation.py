from concurrent.futures import Executor
import datetime
from inspect import iscoroutinefunction
from typing import (
    Any,
    AsyncIterable,
    Callable,
    Iterable,
)


def validate_concurrency_executor(
    concurrency: Executor, fn: Callable[[Any], Any], fn_name: str
) -> None:
    if iscoroutinefunction(fn):
        raise TypeError(
            f"`concurrency` must be an int if `{fn_name}` is a coroutine function but got: {repr(concurrency)}"
        )


def validate_positive_timedelta(interval: datetime.timedelta, *, name: str) -> None:
    if interval <= datetime.timedelta(0):
        raise ValueError(
            f"`{name}` must be a positive timedelta but got: {repr(interval)}"
        )


def validate_int(integer: int, *, gte: int, name: str) -> None:
    if integer < gte:
        raise ValueError(f"`{name}` must be >= {gte} but got: {integer}")


def validate_sync_flatten_iterable(iterable: Any) -> None:
    if isinstance(iterable, Iterable):
        return
    if isinstance(iterable, AsyncIterable):
        raise TypeError(
            "async iterables flattening is only possible during async iteration"
        )
    raise TypeError(f"flatten expects iterables but got: {repr(iterable)}")


def validate_async_flatten_iterable(iterable: Any) -> None:
    if isinstance(iterable, (AsyncIterable, Iterable)):
        return
    raise TypeError(f"flatten expects iterables but got: {repr(iterable)}")
