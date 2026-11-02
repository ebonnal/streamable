import datetime
from concurrent.futures import Executor
from inspect import iscoroutinefunction
from typing import (
    AsyncIterable,
    Callable,
    Iterable,
    Union,
)


def validate_concurrency_executor(
    concurrency: Executor, fn: Callable[..., object], fn_name: str
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


def validate_iterable(iterable: Union[Iterable, AsyncIterable]) -> None:
    if not isinstance(iterable, (Iterable, AsyncIterable)):
        raise TypeError(f"flatten expects iterables but got: {repr(iterable)}")
