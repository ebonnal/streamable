import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from inspect import iscoroutinefunction
from threading import Thread
from typing import Any, Callable, List, Optional, cast

import pytest

from streamable import star, stream
from streamable._tools._func import sidify
from streamable._tools._iter import AsyncToSyncIterator, GuardedAsyncIterator
from streamable._tools._logging import logfmt_str_escape, setup_logger
from tests.tools.func import async_identity, identity, throw
from tests.tools.iter import async_iter
from tests.tools.source import INTEGERS

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

    assert star(add)((2, 5)) == 7

    @star
    def decorated_add(a: int, b: int) -> int:
        return a + b

    assert decorated_add((2, 5)) == 7

    async def async_add(a: int, b: int) -> int:
        return a + b

    assert iscoroutinefunction(star(async_add))

    assert (await star(async_add)((2, 5))) == 7

    # sync function without `.__name__`
    assert star(partial(add, 2))((5,)) == 7

    # async function without `.__name__`
    assert (await star(partial(async_add, 2))((5,))) == 7


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


########
# iter #
########


@pytest.mark.parametrize("n_pulls", [1, 2])
def test_async_to_sync_iterator_already_closed(n_pulls: int) -> None:
    it = AsyncToSyncIterator(GuardedAsyncIterator(async_iter([0])))
    for _ in range(n_pulls):
        next(it, None)
    it.close()
    it.close()
    with pytest.raises(StopIteration):
        next(it)


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
        loop = it._loop

    thread = Thread(target=next_and_abandon)
    thread.start()
    thread.join()
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

    with ThreadPoolExecutor(1) as executor:
        # work within a fresh thread with no event loop already set
        it = executor.submit(interrupted_next).result()
    # the pending `__anext__` task got cancelled
    assert cancelled
    # the iterator got closed
    assert it._closure_requested
    # and its private loop too
    assert it._loop.is_closed()
    with pytest.raises(StopIteration):
        next(it)
