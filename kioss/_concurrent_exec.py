import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from queue import Queue
from typing import (
    Callable,
    Iterable,
    Iterator,
    Optional,
    Set,
    TypeVar,
)

from kioss._exec import IteratorWrapper

T = TypeVar("T")
R = TypeVar("R")


@dataclass
class ExceptionContainer(Exception):
    exception: Exception


class ThreadedMappingIteratorWrapper(IteratorWrapper[R]):
    def __init__(self, iterator: Iterator[T], func: Callable[[T], R], n_workers: int):
        super().__init__(iter(ThreadedMappingIterable(iterator, func, n_workers)))

    def __next__(self) -> R:
        elem = super().__next__()
        if isinstance(elem, ExceptionContainer):
            raise elem.exception
        return elem


class ThreadedMappingIterable(Iterable[R]):
    _MAX_QUEUE_SIZE = 32

    def __init__(self, iterator: Iterator[T], func: Callable[[T], R], n_workers: int):
        self.iterator = iterator
        self.func = func
        self.n_workers = n_workers

    def __iter__(self):
        futures: "Queue[Future]" = Queue()
        iterator_exhausted = False
        n_yields = 0
        n_iterated_elems = 0
        with ThreadPoolExecutor(max_workers=self.n_workers) as executor:
            while True:
                try:
                    while (
                        not iterator_exhausted
                        and executor._work_queue.qsize()
                        < ThreadedMappingIterable._MAX_QUEUE_SIZE
                        and n_iterated_elems - n_yields
                        < ThreadedMappingIterable._MAX_QUEUE_SIZE
                    ):
                        try:
                            elem = next(self.iterator)
                            n_iterated_elems += 1
                            futures.put(executor.submit(self.func, elem))
                        except StopIteration:
                            iterator_exhausted = True
                    while True:
                        if n_yields < n_iterated_elems:
                            n_yields += 1
                            yield futures.get().result()
                        if iterator_exhausted and n_iterated_elems == n_yields:
                            return
                        if (
                            not iterator_exhausted
                            and executor._work_queue.qsize()
                            < ThreadedMappingIterable._MAX_QUEUE_SIZE // 2
                        ):
                            break
                except Exception as e:
                    yield ExceptionContainer(e)


class ThreadedFlatteningIteratorWrapper(ThreadedMappingIteratorWrapper[T]):
    _SKIP = []
    _BUFFER_SIZE = 32
    _INIT_RETRY_BACKFOFF = 0.0005

    class IteratorIteratorNextsShuffler(Iterator[Callable[[], T]]):
        def __init__(self, iterator_iterator: Iterator[Iterator[T]]):
            self.iterator_iterator = iterator_iterator
            self.iterator_iterator_exhausted = False
            self.iterators_pool: Set[Iterator[T]] = set()
            self.iterators_being_iterated: Set[Iterator[T]] = set()

        def __next__(self):
            backoff = ThreadedFlatteningIteratorWrapper._INIT_RETRY_BACKFOFF
            while True:
                while (
                    not self.iterator_iterator_exhausted
                    and len(self.iterators_pool)
                    < ThreadedFlatteningIteratorWrapper._BUFFER_SIZE
                ):
                    try:
                        elem = next(self.iterator_iterator)
                        if not isinstance(elem, Iterator):
                            raise TypeError(
                                f"Elements to be flattened have to be, but got '{elem}' of type{type(elem)}"
                            )
                        self.iterators_pool.add(elem)
                    except StopIteration:
                        self.iterator_iterator_exhausted = True

                try:
                    next_iterator_elem = self.iterators_pool.pop()
                    self.iterators_being_iterated.add(next_iterator_elem)
                    backoff = ThreadedFlatteningIteratorWrapper._INIT_RETRY_BACKFOFF
                except KeyError:  # KeyError: 'pop from an empty set'
                    if (
                        self.iterator_iterator_exhausted
                        and len(self.iterators_being_iterated) == 0
                    ):
                        raise StopIteration()
                    time.sleep(backoff)
                    backoff *= 2
                    continue

                def f():
                    exhausted = False
                    to_be_raised: Optional[Exception] = None
                    try:
                        elem = next(next_iterator_elem)
                    except StopIteration:
                        exhausted = True
                    except Exception as e:
                        to_be_raised = e
                    self.iterators_being_iterated.remove(next_iterator_elem)
                    if exhausted:
                        return ThreadedFlatteningIteratorWrapper._SKIP
                    else:
                        self.iterators_pool.add(next_iterator_elem)
                    if to_be_raised is not None:
                        raise to_be_raised
                    return elem

                return f

    def __init__(self, iterator: Iterator[Iterator[T]], n_workers: int):
        super().__init__(
            ThreadedFlatteningIteratorWrapper.IteratorIteratorNextsShuffler(iterator),
            func=lambda f: f(),
            n_workers=n_workers,
        )

    def __next__(self) -> T:
        while True:
            elem = super().__next__()
            if elem != ThreadedFlatteningIteratorWrapper._SKIP:
                return elem
