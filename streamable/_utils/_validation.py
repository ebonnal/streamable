import datetime
from contextlib import suppress
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
    from typing import Literal

T = TypeVar("T")


def validate_concurrency(concurrency: int) -> None:
    if concurrency is None or concurrency < 1:
        raise ValueError(f"`concurrency` must be >= 1 but got {concurrency}")


def validate_via(via: "Literal['thread', 'process']") -> None:
    if via not in ["thread", "process"]:
        raise TypeError(f"`via` must be 'thread' or 'process' but got {repr(via)}")


def validate_group_size(size: Optional[int]) -> None:
    if size is not None and size < 1:
        raise ValueError(f"`size` must be None or >= 1 but got {size}")


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
        raise TypeError(f"`{name}` must be an int or a callable, but got {_}")
    if isinstance(_, int):
        validate_count(_, name=name)


def validate_count(count: int, *, name: str) -> None:
    if count < 0:
        raise ValueError(f"`{name}` must be >= 0 but got {count}")


def validate_callable(func: int, *, name: str) -> None:
    if not callable(func):
        raise ValueError(f"`{name}` must be >= 0 but got {func}")


def validate_positive_count(count: int, *, name: str) -> None:
    if count < 1:
        raise ValueError(f"`{name}` must be >= 1 but got {count}")


def validate_optional_count(count: Optional[int], *, name: str) -> None:
    if count is not None:
        validate_count(count, name=name)


def validate_optional_positive_count(count: Optional[int], *, name: str) -> None:
    if count is not None:
        validate_positive_count(count, name=name)


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
        f"`errors` must be an `Exception` subclass or a tuple of such classes but got {errors}"
    )
