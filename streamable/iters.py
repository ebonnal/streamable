import asyncio
import itertools
import time
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from typing import (
    Any,
    Callable,
    Coroutine,
    Deque,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

T = TypeVar("T")
U = TypeVar("U")

from streamable import util


class CatchingIterator(Iterator[T]):
    def __init__(
        self,
        iterator: Iterator[T],
        kind: Type[Exception],
        finally_raise: bool,
    ) -> None:
        self.iterator = iterator
        self.kind = kind
        self.finaly_raise = finally_raise
        self._first_catched_error: Optional[Exception] = None
        self._first_error_has_been_raised = False

    def __next__(self) -> T:
        while True:
            try:
                return next(self.iterator)
            except StopIteration:
                if (
                    self._first_catched_error is not None
                    and self.finaly_raise
                    and not self._first_error_has_been_raised
                ):
                    self._first_error_has_been_raised = True
                    raise self._first_catched_error
                raise
            except Exception as exception:
                if isinstance(exception, self.kind):
                    if self._first_catched_error is None:
                        self._first_catched_error = exception
                else:
                    raise exception


class FlatteningIterator(Iterator[U]):
    def __init__(self, iterator: Iterator[Iterable[U]]) -> None:
        self.iterator = iterator
        self._current_iterator_elem: Iterator[U] = iter([])

    def __next__(self) -> U:
        try:
            return next(self._current_iterator_elem)
        except StopIteration:
            while True:
                elem = next(self.iterator)
                util.validate_iterable(elem)
                self._current_iterator_elem = util.reraise_as(
                    iter, StopIteration, util.NoopStopIteration
                )(elem)
                try:
                    return next(self._current_iterator_elem)
                except StopIteration:
                    pass


class GroupingIterator(Iterator[List[T]]):
    def __init__(
        self,
        iterator: Iterator[T],
        size: int,
        seconds: float,
        by: Optional[Callable[[T], Any]],
    ) -> None:
        self.iterator = iterator
        self.size = size
        self.seconds = seconds
        self.by = by
        self._to_be_raised: Optional[Exception] = None
        self._is_exhausted = False
        self._last_yielded_group_at = time.time()
        self._groups_by: Dict[Any, List[T]] = {}

    def _group_key(self, elem: T) -> Any:
        if self.by:
            return self.by(elem)

    def _group_next_elem(self) -> None:
        elem = next(self.iterator)
        key = self._group_key(elem)
        if key in self._groups_by:
            self._groups_by[key].append(elem)
        else:
            self._groups_by[key] = [elem]

    def _seconds_have_elapsed(self) -> bool:
        return (time.time() - self._last_yielded_group_at) >= self.seconds

    def _pop_full_group(self) -> Optional[List[T]]:
        for key, group in self._groups_by.items():
            if len(group) >= self.size:
                return self._groups_by.pop(key)
        return None

    def _pop_first_group(self) -> List[T]:
        first_key = next(iter(self._groups_by), ...)
        return self._groups_by.pop(first_key)

    def _pop_largest_group(self) -> List[T]:
        largest_group_key: Any = next(iter(self._groups_by), ...)

        for key, group in self._groups_by.items():
            if len(group) > len(self._groups_by[largest_group_key]):
                largest_group_key = key

        return self._groups_by.pop(largest_group_key)

    def _return_group(self, group: List[T]) -> List[T]:
        self._last_yielded_group_at = time.time()
        return group

    def __next__(self) -> List[T]:
        if self._is_exhausted:
            if self._groups_by:
                return self._return_group(self._pop_first_group())
            raise StopIteration

        if self._to_be_raised:
            if self._groups_by:
                return self._return_group(self._pop_first_group())
            e = self._to_be_raised
            self._to_be_raised = None
            raise e

        try:
            self._group_next_elem()

            full_group: Optional[List[T]] = self._pop_full_group()
            while not full_group and not self._seconds_have_elapsed():
                self._group_next_elem()
                full_group = self._pop_full_group()

            if full_group:
                return self._return_group(full_group)
            return self._return_group(self._pop_largest_group())

        except StopIteration:
            self._is_exhausted = True
            return next(self)

        except Exception as e:
            if not self._groups_by:
                raise e
            self._to_be_raised = e
            return next(self)


class TruncatingOnCountIterator(Iterator[T]):
    def __init__(self, iterator: Iterator[T], count: int) -> None:
        self.iterator = iterator
        self.count = count
        self._n_yields = 0

    def __next__(self):
        if self._n_yields == self.count:
            raise StopIteration()
        try:
            return next(self.iterator)
        finally:
            self._n_yields += 1


class TruncatingOnPredicateIterator(Iterator[T]):
    def __init__(self, iterator: Iterator[T], when: Callable[[T], Any]) -> None:
        self.iterator = iterator
        self.when = when
        self.satisfied = False

    def __next__(self):
        if self.satisfied:
            raise StopIteration()
        elem = next(self.iterator)
        if self.when(elem):
            self.satisfied = True
            return next(self)
        return elem


class ObservingIterator(Iterator[T]):
    def __init__(self, iterator: Iterator[T], what: str) -> None:
        self.iterator = iterator
        self.what = what
        self._n_yields = 0
        self._n_errors = 0
        self._last_log_after_n_calls = 0
        self._start_time = time.time()

    def _log(self) -> None:
        errors_summary = f"errors={self._n_errors}"
        yields_summary = f"{self._n_yields} {self.what} yielded"
        elapsed_time = f"duration={datetime.fromtimestamp(time.time()) - datetime.fromtimestamp(self._start_time)}"

        util.get_logger().info(
            "[%s %s] %s", elapsed_time, errors_summary, yields_summary
        )

    def _n_calls(self) -> int:
        return self._n_yields + self._n_errors

    def __next__(self) -> T:
        try:
            elem = next(self.iterator)
            self._n_yields += 1
            return elem
        except StopIteration:
            if self._n_calls() != self._last_log_after_n_calls:
                self._last_log_after_n_calls = self._n_calls()
                self._log()
            raise
        except Exception as e:
            self._n_errors += 1
            raise e
        finally:
            if self._n_calls() >= 2 * self._last_log_after_n_calls:
                self._log()
                self._last_log_after_n_calls = self._n_calls()


class SlowingIterator(Iterator[T]):
    def __init__(self, iterator: Iterator[T], frequency: float) -> None:
        self.iterator = iterator
        self.period = 1 / frequency

    def __next__(self) -> T:
        start_time = time.time()
        next_elem = next(self.iterator)
        sleep_duration = self.period - (time.time() - start_time)
        if sleep_duration > 0:
            time.sleep(sleep_duration)
        return next_elem


class RaisingIterator(Iterator[T]):
    @dataclass
    class ExceptionContainer:
        exception: Exception

    def __init__(
        self,
        iterator: Iterator[Union[T, ExceptionContainer]],
    ) -> None:
        self.iterator = iterator

    def __next__(self) -> T:
        elem = next(self.iterator)
        if isinstance(elem, self.ExceptionContainer):
            raise elem.exception
        return elem


class ConcurrentMappingIterable(Iterable[Union[U, RaisingIterator.ExceptionContainer]]):
    def __init__(
        self,
        iterator: Iterator[T],
        transformation: Callable[[T], U],
        concurrency: int,
        buffer_size: int,
    ) -> None:
        self.iterator = iterator
        self.transformation = transformation
        self.concurrency = concurrency
        self.buffer_size = buffer_size

    def __iter__(self) -> Iterator[Union[U, RaisingIterator.ExceptionContainer]]:
        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            futures: Deque[Future] = deque()
            element_to_yield: List[Union[U, RaisingIterator.ExceptionContainer]] = []
            # wait, queue, yield (FIFO)
            while True:
                if futures:
                    try:
                        element_to_yield.append(futures.popleft().result())
                    except Exception as e:
                        element_to_yield.append(RaisingIterator.ExceptionContainer(e))

                # queue tasks up to buffer_size
                while len(futures) < self.buffer_size:
                    try:
                        elem = next(self.iterator)
                    except StopIteration:
                        # the upstream iterator is exhausted
                        break
                    futures.append(executor.submit(self.transformation, elem))
                if element_to_yield:
                    yield element_to_yield.pop()
                if not futures:
                    break


class AsyncConcurrentMappingIterable(
    Iterable[Union[U, RaisingIterator.ExceptionContainer]]
):
    def __init__(
        self,
        iterator: Iterator[T],
        transformation: Callable[[T], Coroutine[Any, Any, U]],
        buffer_size: int,
    ) -> None:
        self.iterator = iterator
        self.transformation = transformation
        self.buffer_size = buffer_size

    async def _safe_transformation(
        self, elem: T
    ) -> Union[U, RaisingIterator.ExceptionContainer]:
        try:
            coroutine = self.transformation(elem)
            if not isinstance(coroutine, Coroutine):
                raise TypeError(
                    f"The function is expected to be an async function, i.e. it must be a function returning a Coroutine object, but returned a {type(coroutine)}."
                )
            return await coroutine
        except Exception as e:
            return RaisingIterator.ExceptionContainer(e)

    def __iter__(self) -> Iterator[Union[U, RaisingIterator.ExceptionContainer]]:
        loop = asyncio.get_event_loop()
        awaitables: Deque[
            asyncio.Task[Union[U, RaisingIterator.ExceptionContainer]]
        ] = deque()
        element_to_yield: List[Union[U, RaisingIterator.ExceptionContainer]] = []
        # wait, queue, yield (FIFO)
        while True:
            if awaitables:
                element_to_yield.append(loop.run_until_complete(awaitables.popleft()))
            # queue tasks up to buffer_size
            while len(awaitables) < self.buffer_size:
                try:
                    elem = next(self.iterator)
                except StopIteration:
                    # the upstream iterator is exhausted
                    break
                awaitables.append(loop.create_task(self._safe_transformation(elem)))
            if element_to_yield:
                yield element_to_yield.pop()
            if not awaitables:
                break


class ConcurrentFlatteningIterable(
    Iterable[Union[T, RaisingIterator.ExceptionContainer]]
):
    def __init__(
        self,
        iterables_iterator: Iterator[Iterable[T]],
        concurrency: int,
        buffer_size: int,
    ) -> None:
        self.iterables_iterator = iterables_iterator
        self.concurrency = concurrency
        self.buffer_size = buffer_size

    def _requeue(self, iterator: Iterator[T]) -> None:
        self.iterables_iterator = itertools.chain(self.iterables_iterator, [iterator])

    def __iter__(self) -> Iterator[Union[T, RaisingIterator.ExceptionContainer]]:
        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            iterator_and_future_pairs: Deque[Tuple[Iterator[T], Future]] = deque()
            element_to_yield: List[Union[T, RaisingIterator.ExceptionContainer]] = []
            # wait, queue, yield (FIFO)
            while True:
                if iterator_and_future_pairs:
                    iterator, future = iterator_and_future_pairs.popleft()
                    try:
                        element_to_yield.append(future.result())
                        self._requeue(iterator)
                    except StopIteration:
                        pass
                    except Exception as e:
                        element_to_yield.append(RaisingIterator.ExceptionContainer(e))
                        self._requeue(iterator)

                # queue tasks up to buffer_size
                while len(iterator_and_future_pairs) < self.buffer_size:
                    try:
                        iterable = next(self.iterables_iterator)
                    except StopIteration:
                        break
                    try:
                        iterator = util.reraise_as(
                            iter, StopIteration, util.NoopStopIteration
                        )(iterable)
                    except Exception as e:
                        yield RaisingIterator.ExceptionContainer(e)
                        continue
                    future = executor.submit(
                        cast(Callable[[Iterable[T]], T], next), iterator
                    )
                    iterator_and_future_pairs.append((iterator, future))
                if element_to_yield:
                    yield element_to_yield.pop()
                if not iterator_and_future_pairs:
                    break
