import datetime
from contextlib import suppress
from typing import (
    Iterable,
    Optional,
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


def validate_optional_positive_interval(
    interval: Optional[datetime.timedelta], *, name: str = "interval"
) -> None:
    if interval is not None and interval <= datetime.timedelta(0):
        raise ValueError(
            f"`{name}` must be None or a positive timedelta but got {repr(interval)}"
        )


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


def validate_errors(
    errors: Union[Optional[Type[Exception]], Iterable[Optional[Type[Exception]]]],
) -> None:
    if errors is not None:
        if not (type(errors) is type and issubclass(errors, Exception)):
            if not isinstance(errors, Iterable):
                raise TypeError(
                    f"`errors` must be None, or a subclass of `Exception`, or an iterable of optional subclasses of `Exception`, but got {type(errors)}"
                )
