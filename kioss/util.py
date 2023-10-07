import itertools
from queue import Empty, Queue
from typing import Any, Callable, Iterable, Iterator, TypeVar, Union, Type
import uuid

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


class QueueIterator(Iterator[T]):
    INITIAL_BACKOFF_period = 0.005

    def __init__(self, queue: Queue, sentinel: Any) -> None:
        self.queue = queue
        self.sentinel = sentinel
        self.backoff = QueueIterator.INITIAL_BACKOFF_period

    def __iter__(self) -> Iterator[T]:
        return self

    def __next__(self) -> T:
        try:
            elem = self.queue.get(timeout=self.backoff)
            self.backoff = self.INITIAL_BACKOFF_period
        except Empty:
            self.backoff *= 2
            return next(self)
        if elem == self.sentinel:
            self.queue.put(self.sentinel)
            raise StopIteration()
        return elem
