import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from queue import Queue
from typing import Callable, Iterable, Iterator, Optional, Set, TypeVar, Union

from kioss import _util
from kioss._execution._core import IteratorWrapper

T = TypeVar("T")
R = TypeVar("R")


@dataclass
class _ExceptionContainer(Exception):
    exception: Exception


class _Skip:
    pass


class ThreadedMappingIteratorWrapper(IteratorWrapper[R]):
    def __init__(
        self,
        iterator: Iterator[T],
        func: Callable[[T], Union[R, _Skip]],
        n_workers: int,
    ):
        super().__init__(iter(ThreadedMappingIterable(iterator, func, n_workers)))

    def __next__(self) -> R:
        while True:
            elem = next(self.iterator)
            if isinstance(elem, _ExceptionContainer):
                raise elem.exception
            if elem != ThreadedFlatteningIteratorWrapper._SKIP:
                return elem


BUFFER_SIZE_FACTOR = 16


class ThreadedMappingIterable(Iterable[Union[R, _ExceptionContainer]]):
    def __init__(self, iterator: Iterator[T], func: Callable[[T], R], n_workers: int):
        self.iterator = iterator
        self.func = func
        self.n_workers = n_workers
        self.max_queue_size = n_workers * BUFFER_SIZE_FACTOR

    def __iter__(self) -> Iterator[Union[R, _ExceptionContainer]]:
        futures: "Queue[Future]" = Queue()
        iterator_exhausted = False
        n_yields = 0
        n_iterated_elems = 0
        with ThreadPoolExecutor(max_workers=self.n_workers) as executor:
            while True:
                try:
                    while (
                        not iterator_exhausted
                        and executor._work_queue.qsize() < self.max_queue_size
                        and n_iterated_elems - n_yields < self.max_queue_size
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
                            and executor._work_queue.qsize() < self.max_queue_size // 2
                        ):
                            break
                except Exception as e:
                    yield _ExceptionContainer(e)


class ThreadedFlatteningIteratorWrapper(ThreadedMappingIteratorWrapper[T]):
    _SKIP: _Skip = _Skip()
    _INIT_RETRY_BACKFOFF = 0.0005

    class IteratorIteratorNextsShuffler(Iterator[Callable[[], Union[T, _Skip]]]):
        def __init__(self, iterator_iterator: Iterator[Iterable[T]], pool_size: int):
            self.iterator_iterator = iterator_iterator
            self.iterator_iterator_exhausted = False
            self.pool_size = pool_size
            self.iterators_pool: Set[Iterator[T]] = set()
            self.iterators_being_iterated: Set[Iterator[T]] = set()

        def __next__(self) -> Callable[[], Union[T, _Skip]]:
            backoff = ThreadedFlatteningIteratorWrapper._INIT_RETRY_BACKFOFF
            while True:
                while (
                    not self.iterator_iterator_exhausted
                    and len(self.iterators_pool) < self.pool_size
                ):
                    try:
                        elem = next(self.iterator_iterator)
                        _util.ducktype_assert_iterable(elem)
                        self.iterators_pool.add(iter(elem))
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

                def f() -> Union[T, _Skip]:
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

    def __init__(self, iterator: Iterator[Iterable[T]], n_workers: int):
        super().__init__(
            ThreadedFlatteningIteratorWrapper.IteratorIteratorNextsShuffler(
                iterator, pool_size=n_workers * BUFFER_SIZE_FACTOR
            ),
            func=lambda f: f(),
            n_workers=n_workers,
        )
