import itertools
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Callable, Iterable, Iterator, List, TypeVar, Union, cast

from streamable import _util
from streamable._execution._core import Iterator

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


class ThreadedFlatteningIterable(Iterable[Union[T, _ExceptionContainer]]):
    def __init__(self, iterables_iterator: Iterator[Iterable[T]], n_workers: int):
        self.iterables_iterator = iterables_iterator
        self.n_workers = n_workers

    def __iter__(self) -> Iterator[Union[R, _ExceptionContainer]]:
        with ThreadPoolExecutor(max_workers=self.n_workers) as executor:
            while True:
                iterators = [
                    iter(iterable)
                    for iterable in itertools.islice(
                        self.iterables_iterator, self.n_workers * BUFFER_SIZE_FACTOR
                    )
                ]
                for iterator in iterators:
                    _util.validate_iterable(iterator)
                futures: List[Future] = [
                    executor.submit(cast(Callable[[Iterable[T]], T], next), iterator)
                    for iterator in iterators
                ]
                if not len(futures):
                    break

                for iterator, future in zip(iterators, futures):
                    try:
                        yield future.result()
                    except StopIteration:
                        continue
                    except Exception as e:
                        yield _ExceptionContainer(e)
                    self.iterables_iterator = itertools.chain(
                        self.iterables_iterator, [iterator]
                    )
