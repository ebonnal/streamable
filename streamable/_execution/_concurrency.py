import itertools
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Callable, Iterable, Iterator, List, Optional, Set, TypeVar, Union

from streamable import _util
from streamable._execution._core import IteratorWrapper

T = TypeVar("T")
R = TypeVar("R")


@dataclass
class _ExceptionContainer(Exception):
    exception: Exception

    @staticmethod
    def wrap(func: Callable[[T], R]) -> Callable[[T], Union[R, "_ExceptionContainer"]]:
        def f(elem: T) -> Union[R, "_ExceptionContainer"]:
            try:
                return func(elem)
            except Exception as e:
                return _ExceptionContainer(e)

        return f


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
            if not isinstance(elem, _Skip):
                return elem


BUFFER_SIZE_FACTOR = 8


class ThreadedMappingIterable(Iterable[Union[R, _ExceptionContainer]]):
    def __init__(self, iterator: Iterator[T], func: Callable[[T], R], n_workers: int):
        self.iterator = iterator
        self.func = func
        self.n_workers = n_workers

    def __iter__(self) -> Iterator[Union[R, _ExceptionContainer]]:
        with ThreadPoolExecutor(max_workers=self.n_workers) as executor:
            while True:
                futures: List[Future] = [
                    executor.submit(self.func, elem)
                    for elem in itertools.islice(
                        self.iterator, self.n_workers * BUFFER_SIZE_FACTOR
                    )
                ]
                if not len(futures):
                    break
                yield from map(_ExceptionContainer.wrap(Future.result), futures)


class ThreadedFlatteningIteratorWrapper(ThreadedMappingIteratorWrapper[T]):
    class ShufflingNextsIterator(Iterator[Callable[[], Union[T, _Skip]]]):
        _INIT_RETRY_BACKFOFF = 0.0005

        def __init__(self, iterables_iterator: Iterator[Iterable[T]], pool_size: int):
            self.iterables_iterator = iterables_iterator
            self.iterator_iterator_exhausted = False
            self.pool_size = pool_size
            self.iterators_pool: Set[Iterator[T]] = set()
            self.iterators_being_iterated: Set[Iterator[T]] = set()

        def __next__(self) -> Callable[[], Union[T, _Skip]]:
            backoff = self._INIT_RETRY_BACKFOFF
            while True:
                while (
                    not self.iterator_iterator_exhausted
                    and len(self.iterators_pool) < self.pool_size
                ):
                    try:
                        iterable = next(self.iterables_iterator)
                        _util.ducktype_assert_iterable(iterable)
                        self.iterators_pool.add(iter(iterable))
                    except StopIteration:
                        self.iterator_iterator_exhausted = True
                try:
                    next_iterator_elem = self.iterators_pool.pop()
                    self.iterators_being_iterated.add(next_iterator_elem)
                    backoff = self._INIT_RETRY_BACKFOFF
                except KeyError:  # KeyError: 'pop from an empty set'
                    if (
                        self.iterator_iterator_exhausted
                        and len(self.iterators_being_iterated) == 0
                        and len(self.iterators_pool) == 0
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
                        return _Skip()
                    else:
                        self.iterators_pool.add(next_iterator_elem)
                    if to_be_raised is not None:
                        raise to_be_raised
                    return elem

                return f

    def __init__(self, iterator: Iterator[Iterable[T]], n_workers: int):
        super().__init__(
            ThreadedFlatteningIteratorWrapper.ShufflingNextsIterator(
                iterator, pool_size=n_workers * BUFFER_SIZE_FACTOR
            ),
            func=lambda f: f(),
            n_workers=n_workers,
        )
