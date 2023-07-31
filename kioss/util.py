from queue import Empty, Queue
from typing import Any, Callable, Iterable, Iterator, TypeVar, Union

T = TypeVar("T")


def sidify(func: Callable[[T], Any]) -> Callable[[T], T]:
    def wrap(arg):
        func(arg)
        return arg

    return wrap


def iterate(it: Union[Iterator[T], Iterable[T]]) -> None:
    for _ in it:
        pass


def identity(obj: T) -> T:
    return obj


class QueueIterator(Iterator[T]):
    INITIAL_BACKOFF_SECS = 0.005

    def __init__(self, queue: Queue, sentinel: Any) -> None:
        self.queue = queue
        self.sentinel = sentinel
        self.backoff = QueueIterator.INITIAL_BACKOFF_SECS

    def __iter__(self) -> Iterator[T]:
        return self

    def __next__(self) -> T:
        try:
            elem = self.queue.get(timeout=self.backoff)
            self.backoff = self.INITIAL_BACKOFF_SECS
        except Empty:
            self.backoff *= 2
            return next(self)
        if elem == self.sentinel:
            self.queue.put(self.sentinel)
            raise StopIteration()
        return elem
