import asyncio
import logging
from concurrent.futures import Future
from functools import partial
from inspect import iscoroutinefunction
from typing import (
    Any,
    AsyncGenerator,
    AsyncIterable,
    Callable,
    Coroutine,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    cast,
)

import pytest

from streamable import star, stream
from streamable._tools import _afuture, _future
from streamable._tools._async import safe_get_event_loop
from streamable._tools._func import sidify, suppressing
from streamable._tools._iter import AsyncToSyncIterator, GuardedAsyncIterator
from streamable._tools._logging import logfmt_str_escape, setup_logger
from tests.tools.error import TestError, collecting_unraisables
from tests.tools.func import async_identity, identity, run_in_thread, throw
from tests.tools.iter import to_aiter
from tests.tools.monitor import (
    CloseFailingMonitoredIntAsyncIterable,
    MonitoredIntAsyncIterable,
    MonitoredIntAsyncIterator,
)
from tests.tools.skip import mark_refcounting_dependent
from tests.tools.source import INTEGERS

T = TypeVar("T")

##############
# validation #
##############


@pytest.mark.parametrize(
    "method, args",
    (
        (stream.map, [identity]),
        (stream.map, [async_identity]),
        (stream.do, [identity]),
        (stream.do, [async_identity]),
        (stream.flatten, []),
        (stream.flatten, []),
    ),
)
def test_validate_concurrency(method: Callable[..., Any], args: List[Any]) -> None:
    # should be raising ValueError for concurrency=0.
    with pytest.raises(ValueError, match="`concurrency` must be >= 1 but got: 0"):
        method(stream(INTEGERS), *args, concurrency=0)


########
# func #
########


def test_sidify() -> None:
    def f(x: int) -> int:
        return x**2

    assert f(2) == 4
    assert sidify(f)(2) == 2

    # test decoration
    @sidify
    def g(x):
        return x**2

    assert g(2) == 2


@pytest.mark.asyncio
async def test_star() -> None:
    def add(a: int, b: int) -> int:
        return a + b

    starred: Callable[[Tuple[int, int]], int] = star(add)
    assert starred((2, 5)) == 7

    lambda_starred: Callable[[Tuple[Any, Any]], Any] = star(lambda a, b: a + b)
    assert lambda_starred((2, 5)) == 7

    @star
    def decorated_add(a: int, b: int) -> int:
        return a + b

    assert decorated_add((2, 5)) == 7

    async def async_add(a: int, b: int) -> int:
        return a + b

    astarred: Callable[[Tuple[int, int]], Coroutine[object, object, int]] = star(
        async_add
    )
    assert iscoroutinefunction(astarred)

    assert (await astarred((2, 5))) == 7

    # sync function without `.__name__`
    assert star(partial(add, 2))((5,)) == 7

    # async function without `.__name__`
    assert (await star(partial(async_add, 2))((5,))) == 7


def test_starmap() -> None:
    s: stream[List[int]] = (
        stream(range(4)).group(2, by=type).map(star(lambda key, group: group))
    )
    assert list(s) == [[0, 1], [2, 3]]

    def fn(key: type, group: List[int]) -> List[int]:
        return group

    s = stream(range(4)).group(2, by=type).map(star(fn))

    assert list(s) == [[0, 1], [2, 3]]

    async def afn(key: type, group: List[int]) -> List[int]:
        return group

    s = stream(range(4)).group(2, by=type).map(star(afn))
    assert list(s) == [[0, 1], [2, 3]]


##################
# future results #
##################


def test_map_fdfo_late_arriving_done_future_is_dropped() -> None:
    future_results: _future.FDFOFutureResults[int] = _future.FDFOFutureResults()
    future: "Future[int]" = Future()
    future_results.add(future)
    future_results.clear()
    # a future gets done post-clear
    future.set_result(0)
    # that newly done future is dropped and not stored
    assert future_results._done_futures.empty()


@pytest.mark.asyncio
async def test_map_fdfo_late_arriving_done_future_is_dropped_async() -> None:
    future_results: _afuture.FDFOFutureResults[int] = _afuture.FDFOFutureResults()
    future: "asyncio.Future[int]" = asyncio.Future()
    future_results.add(future)
    future_results.clear()
    # a future gets done post-clear
    future.set_result(0)
    # that newly done future is dropped and not stored
    assert future_results._done_futures.empty()


###########
# logging #
###########


def test_logfmt_str_escape():
    """Test logfmt string escaping."""
    assert logfmt_str_escape("") == '""'
    assert logfmt_str_escape("ints") == "ints"
    assert logfmt_str_escape("in ts") == '"in ts"'
    assert logfmt_str_escape("in\\ts") == r'"in\\ts"'
    assert logfmt_str_escape('"ints"') == r'"\"ints\""'
    assert logfmt_str_escape("in=ts") == '"in=ts"'
    assert logfmt_str_escape("in\tts") == '"in ts"'
    assert logfmt_str_escape("in\nts") == '"in ts"'
    assert logfmt_str_escape("in\rts") == '"in ts"'
    assert logfmt_str_escape("in\r\nts") == '"in ts"'


def test_setup_logger_keeps_existing_handlers() -> None:
    logger = logging.getLogger("streamable")
    pre_setup_handlers = list(logger.handlers)
    assert pre_setup_handlers
    setup_logger()
    assert logger.handlers == pre_setup_handlers


######################
# sync to async iter #
######################


@pytest.mark.asyncio
async def test_sync_to_async_iterator_close_propagation_to_source() -> None:
    src = MonitoredIntAsyncIterable()
    s = stream(src)
    aiterator = s.__aiter__()
    assert await aiterator.__anext__() == 0
    await aiterator.aclose()
    assert src.iterators[0].close_attempted


######################
# async to sync iter #
######################


@pytest.mark.parametrize("n_pulls", [1, 2])
def test_async_to_sync_iterator_already_closed(n_pulls: int) -> None:
    it = AsyncToSyncIterator(GuardedAsyncIterator(to_aiter([0])))
    for _ in range(n_pulls):
        next(it, None)
    it.close()
    it.close()
    with pytest.raises(StopIteration):
        next(it)


@mark_refcounting_dependent
@pytest.mark.parametrize("loop_already_set", [True, False])
def test_async_to_sync_iterator_closes_its_loop_when_abandoned(
    loop_already_set: bool,
) -> None:
    loop: Optional[asyncio.AbstractEventLoop] = None

    def next_and_abandon() -> None:
        nonlocal loop
        if loop_already_set:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        it = cast(AsyncToSyncIterator, iter(stream(INTEGERS).map(async_identity)))
        next(it)
        loop = it._lazy_loop

    run_in_thread(next_and_abandon)
    assert isinstance(loop, asyncio.AbstractEventLoop)
    assert loop.is_closed() is not loop_already_set
    loop.close()


@pytest.mark.asyncio
async def test_async_to_sync_iterator_within_running_loop() -> None:
    non_running_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(non_running_loop)
    with pytest.raises(
        RuntimeError, match="event loop already running: do an async iteration instead"
    ):
        list(stream(INTEGERS).map(async_identity))
    non_running_loop.close()


def test_async_to_sync_iterator_cancels_anext_on_keyboard_interrupt() -> None:
    cancelled: bool = False

    async def cancel_during_sleep(_: object) -> None:
        nonlocal cancelled
        asyncio.get_running_loop().call_soon(lambda: throw(KeyboardInterrupt()))
        try:
            await asyncio.sleep(1)
        except asyncio.CancelledError:
            # long cancellation
            await asyncio.sleep(0.1)
            cancelled = True
            raise

    def interrupted_next() -> AsyncToSyncIterator[int]:
        it = iter(stream([0]).do(cancel_during_sleep))
        assert isinstance(it, AsyncToSyncIterator)
        with pytest.raises(KeyboardInterrupt):
            next(it)
        return it

    it = run_in_thread(interrupted_next)
    # the pending `__anext__` task got cancelled
    assert cancelled
    with pytest.raises(StopIteration):
        next(it)
    with pytest.raises(StopIteration):
        next(it)
    it.close()
    assert it._lazy_loop.is_closed()


def test_async_to_sync_iterator_with_private_loop() -> None:
    def exhaust() -> None:
        s = stream(INTEGERS).map(async_identity)
        # iteration uses a private loop
        s()
        assert safe_get_event_loop() is None

        # new iteration using a fresh private loop
        it1 = cast(AsyncToSyncIterator, iter(s))
        assert it1._loop is None
        assert next(it1) == 0
        assert it1._loop is not None
        # loop created is private
        assert safe_get_event_loop() is None

        # new iteration coexisting using a dedicated fresh private loop
        it2 = cast(AsyncToSyncIterator, iter(s))
        assert it2._loop is None
        assert next(it2) == 0
        assert it2._loop is not None
        assert it2._loop is not it1._loop
        assert safe_get_event_loop() is None

        assert list(it2) == list(INTEGERS)[1:]
        # exhaustion closes the private loop
        assert it2._lazy_loop.is_closed()
        with pytest.raises(StopIteration):
            next(it2)

        assert not it1._lazy_loop.is_closed()
        it1.close()
        assert it1._lazy_loop.is_closed()
        # noop
        it2.close()

    # in a thread without event loop: the iterators use a private loop
    run_in_thread(exhaust)


def next_and_del(src: AsyncIterable) -> asyncio.AbstractEventLoop:
    it = cast(AsyncToSyncIterator, iter(stream(src)))
    next(it)
    loop = it._lazy_loop
    del it
    return loop


def next_and_del_within_another_loop(
    src: AsyncIterable,
) -> asyncio.AbstractEventLoop:
    its = [cast(AsyncToSyncIterator, iter(stream(src)))]
    next(its[0])
    loop = its[0]._lazy_loop

    async def drop() -> None:
        its.clear()

    asyncio.run(drop())
    return loop


def next_and_close_loop_and_del(
    src: AsyncIterable, pull: bool
) -> asyncio.AbstractEventLoop:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    it = iter(stream(src))
    if pull:
        next(it)
    loop.close()
    del it
    return loop


def next_and_del_while_its_not_owned_loop_runs(
    src: AsyncIterable,
    within_thread: bool,
) -> asyncio.AbstractEventLoop:
    loop = asyncio.new_event_loop()
    loop.set_debug(True)  # debug mode to surface non-thread-safe calls
    asyncio.set_event_loop(loop)
    it = iter(stream(src))
    next(it)

    async def drop() -> None:
        def drop() -> None:
            nonlocal it
            del it

        if within_thread:
            run_in_thread(drop)
        else:
            drop()
        await asyncio.sleep(0)

    loop.run_until_complete(drop())
    return loop


def next_and_del_with_not_owned_loop_while_another_runs(
    src: AsyncIterable,
) -> asyncio.AbstractEventLoop:
    loop = asyncio.new_event_loop()
    another_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    it = iter(stream(src))
    next(it)

    async def drop() -> None:
        nonlocal it
        del it
        await asyncio.sleep(0)

    another_loop.run_until_complete(drop())
    another_loop.close()
    return loop


@mark_refcounting_dependent
@pytest.mark.parametrize(
    "src_type, scenario, owned_src_is_closed, loop_is_closed, unraisables_exc_types",
    [
        (MonitoredIntAsyncIterable, next_and_del, True, True, []),
        (MonitoredIntAsyncIterable, next_and_del_within_another_loop, False, True, []),
        (
            MonitoredIntAsyncIterable,
            partial(next_and_close_loop_and_del, pull=True),
            False,
            True,
            [],
        ),
        (
            MonitoredIntAsyncIterable,
            partial(next_and_close_loop_and_del, pull=False),
            False,
            True,
            [],
        ),
        (CloseFailingMonitoredIntAsyncIterable, next_and_del, True, True, [TestError]),
        (
            MonitoredIntAsyncIterable,
            partial(next_and_del_while_its_not_owned_loop_runs, within_thread=True),
            True,
            False,
            [],
        ),
        (
            MonitoredIntAsyncIterable,
            partial(next_and_del_while_its_not_owned_loop_runs, within_thread=False),
            True,
            False,
            [],
        ),
        (
            MonitoredIntAsyncIterable,
            next_and_del_with_not_owned_loop_while_another_runs,
            False,
            False,
            [],
        ),
    ],
)
def test_async_to_sync_iterator_del(
    src_type: "Type[MonitoredIntAsyncIterable]",
    scenario: Callable[[AsyncIterable[int]], asyncio.AbstractEventLoop],
    owned_src_is_closed: bool,
    loop_is_closed: bool,
    unraisables_exc_types: List["Type[BaseException]"],
) -> None:
    src = src_type()
    with collecting_unraisables() as unraisables:
        loop = run_in_thread(scenario, src)
    assert src.iterators[0].close_attempted is owned_src_is_closed
    assert loop.is_closed() is loop_is_closed
    assert [
        type(unraisable.exc_value) for unraisable in unraisables
    ] == unraisables_exc_types
    loop.close()


def test_async_to_sync_iterator_close_from_its_loop() -> None:
    def close_from_its_loop() -> None:
        s = stream(range(10)).map(async_identity)
        iterator = s.__iter__()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        assert iterator.__next__() == 0

        async def sync_close() -> None:
            iterator.close()

        loop.run_until_complete(sync_close())

    with pytest.raises(RuntimeError, match="event loop already running: cannot close"):
        run_in_thread(close_from_its_loop)


async def twice_interrupted_zero(elem: int) -> None:
    if elem == 0:
        await twice_interrupted(elem)


async def twice_interrupted(elem: int) -> None:
    loop = asyncio.get_running_loop()
    loop.call_soon(lambda: throw(KeyboardInterrupt()))
    try:
        await asyncio.sleep(1)
    except asyncio.CancelledError:
        # second interruption, while the cancellation is being honored
        loop.call_soon(lambda: throw(KeyboardInterrupt))
        await asyncio.sleep(0.05)
        raise


def test_async_to_sync_iterator_twice_interrupted_and_closed() -> None:
    def interrupt_twice_and_close() -> None:
        it = iter(stream([0, 1]).do(twice_interrupted))
        assert isinstance(it, AsyncToSyncIterator)
        with pytest.raises(KeyboardInterrupt):
            next(it)
        assert it._pending_pull is not None and not it._pending_pull.done()
        it.close()
        assert it._lazy_loop.is_closed()

    run_in_thread(interrupt_twice_and_close)


def test_async_to_sync_iterator_twice_interrupted_next() -> None:
    def interrupt_twice_and_next() -> None:
        it = iter(stream([0, 1, 2]).do(twice_interrupted_zero))
        assert isinstance(it, AsyncToSyncIterator)
        with pytest.raises(KeyboardInterrupt):
            next(it)
        assert it._pending_pull is not None and not it._pending_pull.done()
        with pytest.raises(asyncio.CancelledError):
            next(it)
        assert next(it) == 1

    run_in_thread(interrupt_twice_and_next)


def test_suppressing() -> None:
    assert suppressing(ValueError, int, "1") == 1
    assert suppressing(ValueError, int, "-") is None


@mark_refcounting_dependent
@pytest.mark.parametrize("concurrency", [1, 4])
@pytest.mark.parametrize("should_close", [False, True])
@pytest.mark.parametrize("get_loop", [lambda: None, asyncio.new_event_loop])
def test_async_to_sync_iterator_finalizes_dropped_agens(
    concurrency: int,
    should_close: bool,
    get_loop: Callable[[], Optional[asyncio.AbstractEventLoop]],
) -> None:
    asyncio.set_event_loop(get_loop())
    finalised: List[bool] = []

    async def agen(_: int) -> AsyncGenerator[int, None]:
        try:
            yield 0
            yield 1
        finally:
            # finalization taking a few loop cycles
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            finalised.append(True)

    src = MonitoredIntAsyncIterator(n_yields=10)
    it = iter(stream(src).map(agen).flatten(concurrency=concurrency))
    assert next(it) == 0

    # only agen finalizers should be awaited, not other tasks, so should not hang on:
    private_loop = cast(AsyncToSyncIterator, it)._lazy_loop
    long_running_task = asyncio.ensure_future(asyncio.sleep(3600), loop=private_loop)

    if should_close:
        it.close()
    del it
    assert len(finalised) == concurrency
    assert not long_running_task.done()

    # client provided async closable source is left unclosed
    assert not src.close_attempted
