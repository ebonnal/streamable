import asyncio
from functools import partial
from typing import (
    Any,
    Callable,
    Coroutine,
    Generic,
    Tuple,
    TypeVar,
    overload,
)

T = TypeVar("T")
R = TypeVar("R")


def _get_name(obj: Any) -> str:
    return getattr(obj, "__name__", obj.__class__.__name__)


def _nostop(func: Callable[[T], R], arg: T) -> R:
    try:
        return func(arg)
    except (StopIteration, StopAsyncIteration) as e:
        raise RuntimeError(f"{_get_name(func)} raised {e.__class__.__name__}") from e


def nostop(func: Callable[[T], R]) -> Callable[[T], R]:
    return partial(_nostop, func)


def anostop(
    async_func: Callable[[T], Coroutine[Any, Any, R]],
) -> Callable[[T], Coroutine[Any, Any, R]]:
    async def wrap(elem: T) -> R:
        try:
            return await async_func(elem)
        except (StopIteration, StopAsyncIteration) as e:
            raise RuntimeError(
                f"{_get_name(async_func)} raised {e.__class__.__name__}"
            ) from e

    return wrap


class _Sidify(Generic[T]):
    def __init__(self, func: Callable[[T], Any]) -> None:
        self.func = func

    def __call__(self, arg: T) -> T:
        self.func(arg)
        return arg


def sidify(func: Callable[[T], Any]) -> Callable[[T], T]:
    return _Sidify(func)


def async_sidify(
    func: Callable[[T], Coroutine],
) -> Callable[[T], Coroutine[Any, Any, T]]:
    async def wrap(arg: T) -> T:
        await func(arg)
        return arg

    return wrap


class _Star(Generic[R]):
    def __init__(self, func: Callable[..., R]) -> None:
        self.func = func

    def __call__(self, args: Tuple) -> R:
        return self.func(*args)


T1 = TypeVar("T1")
T2 = TypeVar("T2")
T3 = TypeVar("T3")
T4 = TypeVar("T4")
T5 = TypeVar("T5")
T6 = TypeVar("T6")
T7 = TypeVar("T7")
T8 = TypeVar("T8")
T9 = TypeVar("T9")


# fmt: off
@overload
def star(func: Callable[[T], R]) -> Callable[[Tuple], R]: ...
@overload
def star(func: Callable[[T1, T2], R]) -> Callable[[Tuple[T1, T2]], R]: ...
@overload
def star(func: Callable[[T1, T2, T3], R]) -> Callable[[Tuple[T1, T2, T3]], R]: ...
@overload
def star(func: Callable[[T1, T2, T3, T4], R]) -> Callable[[Tuple[T1, T2, T3, T4]], R]: ...
@overload
def star(func: Callable[[T1, T2, T3, T4, T5], R]) -> Callable[[Tuple[T1, T2, T3, T4, T5]], R]: ...
@overload
def star(func: Callable[[T1, T2, T3, T4, T5, T6], R]) -> Callable[[Tuple[T1, T2, T3, T4, T5, T6]], R]: ...
@overload
def star(func: Callable[[T1, T2, T3, T4, T5, T6, T7], R]) -> Callable[[Tuple[T1, T2, T3, T4, T5, T6, T7]], R]: ...
@overload
def star(func: Callable[[T1, T2, T3, T4, T5, T6, T7, T8], R]) -> Callable[[Tuple[T1, T2, T3, T4, T5, T6, T7, T8]], R]: ...
@overload
def star(func: Callable[[T1, T2, T3, T4, T5, T6, T7, T8, T9], R]) -> Callable[[Tuple[T1, T2, T3, T4, T5, T6, T7, T8, T9]], R]: ...
@overload
def star(func: Callable[..., R]) -> Callable[[Tuple], R]: ...
# fmt: on


def star(func: Callable[..., R]) -> Callable[[Tuple], R]:
    """
    Transforms a function that takes several positional arguments into a function that takes a tuple.

    ```
    @star
    def add(a: int, b: int) -> int:
        return a + b

    assert add((2, 5)) == 7

    assert star(lambda a, b: a + b)((2, 5)) == 7
    ```
    """
    return _Star(func)


class _Syncify(Generic[T, R]):
    def __init__(
        self,
        event_loop: asyncio.AbstractEventLoop,
        async_func: Callable[[T], Coroutine[Any, Any, R]],
    ) -> None:
        self.async_func = async_func
        self.event_loop = event_loop

    def __call__(self, arg: T) -> R:
        return self.event_loop.run_until_complete(self.async_func(arg))


def syncify(
    event_loop: asyncio.AbstractEventLoop,
    async_func: Callable[[T], Coroutine[Any, Any, R]],
) -> Callable[[T], R]:
    return _Syncify(event_loop, async_func)


async def _async_call(func: Callable[[T], R], o: T) -> R:
    return func(o)


def asyncify(func: Callable[[T], R]) -> Callable[[T], Coroutine[Any, Any, R]]:
    return partial(_async_call, func)
