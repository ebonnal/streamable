import itertools
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from queue import Queue
from typing import Callable, Iterable, Iterator, Tuple, TypeVar, Union, cast

from streamable._execution._core import Iterator

T = TypeVar("T")
R = TypeVar("R")


@dataclass
class _ExceptionContainer(Exception):
    exception: Exception


class RaisingIterator(Iterator[T]):
    def __init__(
        self,
        iterator: Iterator[Union[T, _ExceptionContainer]],
    ):
        self.iterator = iterator

    def __next__(self) -> T:
        elem = next(self.iterator)
        if isinstance(elem, _ExceptionContainer):
            raise elem.exception
        return elem


_BUFFER_SIZE_FACTOR = 3


class ThreadedMappingIterable(Iterable[Union[R, _ExceptionContainer]]):
    def __init__(self, iterator: Iterator[T], func: Callable[[T], R], n_workers: int):
        self.iterator = iterator
        self.func = func
        self.n_workers = n_workers
        self.buffer_size = n_workers * _BUFFER_SIZE_FACTOR

    def __iter__(self) -> Iterator[Union[R, _ExceptionContainer]]:
        with ThreadPoolExecutor(max_workers=self.n_workers) as executor:
            futures: "Queue[Future]" = Queue(maxsize=self.buffer_size)
            # queue and yield (FIFO)
            while True:
                # queue tasks up to queue's maxsize
                while not futures.full():
                    try:
                        elem = next(self.iterator)
                    except StopIteration:
                        # the upstream iterator is exhausted
                        break
                    futures.put(executor.submit(self.func, elem))
                if futures.empty():
                    break
                try:
                    yield futures.get().result()
                except Exception as e:
                    yield _ExceptionContainer(e)


class ThreadedFlatteningIterable(Iterable[Union[T, _ExceptionContainer]]):
    def __init__(self, iterables_iterator: Iterator[Iterable[T]], n_workers: int):
        self.iterables_iterator = iterables_iterator
        self.n_workers = n_workers
        self.buffer_size = n_workers * _BUFFER_SIZE_FACTOR

    def __iter__(self) -> Iterator[Union[T, _ExceptionContainer]]:
        with ThreadPoolExecutor(max_workers=self.n_workers) as executor:
            iterator_and_future_pairs: "Queue[Tuple[Iterator[T], Future]]" = Queue(
                maxsize=self.buffer_size
            )
            # queue and yield (FIFO)
            while True:
                # queue tasks up to queue's maxsize
                while not iterator_and_future_pairs.full():
                    try:
                        iterable = next(self.iterables_iterator)
                    except StopIteration:
                        break
                    iterator = iter(iterable)
                    future = executor.submit(
                        cast(Callable[[Iterable[T]], T], next), iterator
                    )
                    iterator_and_future_pairs.put((iterator, future))

                if iterator_and_future_pairs.empty():
                    break
                iterator, future = iterator_and_future_pairs.get()
                try:
                    yield future.result()
                except StopIteration:
                    continue
                except Exception as e:
                    yield _ExceptionContainer(e)
                self.iterables_iterator = itertools.chain(
                    self.iterables_iterator, [iterator]
                )
