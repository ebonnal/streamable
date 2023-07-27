from typing import Callable, Iterable, Iterator, TypeVar, Union

T = TypeVar("T")
R = TypeVar("R")


def sidify(func: Callable[[T], R]) -> Callable[[T], T]:
    def wrap(arg):
        func(arg)
        return arg

    return wrap


def iterate(it: Union[Iterator[T], Iterable[T]]) -> None:
    for _ in it:
        pass


def identity(obj: T) -> T:
    return obj
