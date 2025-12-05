import datetime
from contextlib import suppress
from inspect import iscoroutinefunction
from typing import (
    Any,
    Callable,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

with suppress(ImportError):
    pass

T = TypeVar("T")


def validate_concurrency(concurrency: int) -> None:
    if concurrency is None or concurrency < 1:
        raise ValueError(f"`concurrency` must be >= 1 but got {repr(concurrency)}")


def validate_concurrency_int_or_executor(
    concurrency: Union[int, Any], fn: Callable[[Any], Any], fn_name: str
) -> None:
    if not isinstance(concurrency, int) and iscoroutinefunction(fn):
        raise TypeError(
            f"if `{fn_name}` is a coroutine function then `concurrency` must be an int but got {repr(concurrency)}"
        )
    elif isinstance(concurrency, int):
        validate_concurrency(concurrency)


def validate_group_size(size: Optional[int]) -> None:
    if size is not None and size < 1:
        raise ValueError(f"`size` must be None or >= 1 but got {repr(size)}")


def validate_positive_interval(interval: datetime.timedelta, *, name: str) -> None:
    if interval <= datetime.timedelta(0):
        raise ValueError(
            f"`{name}` must be a positive timedelta but got {repr(interval)}"
        )


def validate_optional_positive_interval(
    interval: Optional[datetime.timedelta], *, name: str
) -> None:
    if interval is not None:
        validate_positive_interval(interval, name=name)


def validate_count_or_callable(_: Union[int, Callable], *, name: str) -> None:
    if not isinstance(_, int) and not callable(_):
        raise TypeError(f"`{name}` must be an int or a callable, but got {repr(_)}")
    if isinstance(_, int):
        validate_count(_, name=name)


def validate_count(count: int, *, name: str) -> None:
    if count < 0:
        raise ValueError(f"`{name}` must be >= 0 but got {repr(count)}")


def validate_positive_count(count: int, *, name: str) -> None:
    if count < 1:
        raise ValueError(f"`{name}` must be >= 1 but got {repr(count)}")


def _is_exception_subclass(error: Any) -> bool:
    return type(error) is type and issubclass(error, Exception)


def validate_errors(
    errors: Union[Type[Exception], Tuple[Type[Exception], ...]],
) -> None:
    if _is_exception_subclass(errors):
        return
    if isinstance(errors, tuple) and all(map(_is_exception_subclass, errors)):
        return
    raise TypeError(
        f"`errors` must be an `Exception` subclass or a tuple of such classes but got {repr(errors)}"
    )
