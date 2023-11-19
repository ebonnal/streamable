from typing import Any, Callable, Iterable, Iterator, Type, TypeVar, Union

T = TypeVar("T")
R = TypeVar("R")


def sidify(func: Callable[[T], Any]) -> Callable[[T], T]:
    def wrap(arg):
        func(arg)
        return arg

    return wrap


def map_exception(
    func: Callable[[T], R], source: Type[Exception], target: Type[Exception]
) -> Callable[[T], R]:
    def wrap(arg):
        try:
            return func(arg)
        except source as e:
            raise target() from e

    return wrap


def iterate(it: Union[Iterator[T], Iterable[T]]) -> None:
    for _ in it:
        pass


def identity(obj: T) -> T:
    return obj


def duck_check_type_is_iterator(expected_iterator: Any) -> None:
    """
    Raises:
        TypeError: If the expected_iterator does not implement __iter__ and __next__ methods.
    """

    try:
        expected_iterator.__iter__
        implements__iter__ = True
    except AttributeError:
        implements__iter__ = False
    try:
        expected_iterator.__next__
        implements__next__ = True
    except AttributeError:
        implements__next__ = False

    if not implements__iter__ and not implements__next__:
        raise TypeError(
            f"Provided object is not an iterator because it does not implement __next__ and __iter__ methods"
        )
    if not implements__iter__:
        raise TypeError(
            f"Provided object is not an iterator because it implements the __next__ but not the __iter__ one."
        )
    if not implements__next__:
        raise TypeError(
            f"Provided object is not an iterator because it implements the __iter__ but not the __next__ one."
        )
