from typing import Callable, TypeVar

T = TypeVar("T")
R = TypeVar("R")


def sidify(func: Callable[[T], R]) -> Callable[[T], T]:
    def wrap(arg):
        func(arg)
        return arg

    return wrap
