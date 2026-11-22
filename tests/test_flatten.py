import asyncio
import time
from collections import Counter, OrderedDict, defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from typing import (
    AbstractSet,
    Any,
    AsyncIterable,
    AsyncIterator,
    Callable,
    ItemsView,
    Iterable,
    Iterator,
    KeysView,
    List,
    Mapping,
    MutableMapping,
    MutableSequence,
    MutableSet,
    Sequence,
    Tuple,
    Union,
    ValuesView,
    cast,
)

import pytest

from streamable import stream
from tests.tools.error import TestError, collecting_unretrievables
from tests.tools.func import (
    async_identity,
    identity,
)
from tests.tools.iter import (
    ITERABLE_TYPES,
    IterableType,
    RaisingOnAiterAsyncIterable,
    RaisingOnIterIterable,
    aclose_or_close,
    aiter_or_iter,
    alist_or_list,
    anext_or_next,
    stopiteration_type,
    to_aiter,
)


def test_flatten_typing() -> None:
    class UserIterable:
        def __iter__(self) -> Iterator[int]:
            return iter([0])  # pragma: no cover

    class UserAsyncIterable:
        def __aiter__(self) -> AsyncIterator[int]:
            return to_aiter([0])  # pragma: no cover

    s: stream[int] = stream([[0]]).map(iter).flatten()
    s = stream([[0]]).map(list).flatten()
    s = stream([[0]]).map(set).flatten()
    s = stream([[0]]).map(tuple).flatten()
    s = stream(([0], [0])).flatten()
    s = stream([[0]]).map(lambda _: map(identity, _)).flatten()
    s = stream([[0]]).map(lambda _: filter(identity, _)).flatten()
    s = stream([[0]]).map(to_aiter).flatten()
    s = stream([{0: "a"}]).flatten()
    s = stream([defaultdict(str, {0: "a"})]).flatten()
    s = stream([OrderedDict({0: "a"})]).flatten()
    s = stream([Counter([0])]).flatten()
    s = stream([{0: "a"}.keys()]).flatten()
    s = stream([{"a": 0}.values()]).flatten()
    s = stream([deque([0])]).flatten()
    s = stream([frozenset([0])]).flatten()
    s = stream[MutableMapping[int, str]]([{0: "a"}]).flatten()
    s = stream[Mapping[int, str]]([{0: "a"}]).flatten()
    s = stream[KeysView[int]]([{0: "a"}.keys()]).flatten()
    s = stream[ValuesView[int]]([{"a": 0}.values()]).flatten()
    s = stream[MutableSet[int]]([{0}]).flatten()
    s = stream[AbstractSet[int]]([{0}]).flatten()
    s = stream[MutableSequence[int]]([[0]]).flatten()
    s = stream([UserIterable()]).flatten()
    s = stream([UserAsyncIterable()]).flatten()
    s = stream((range(1), range(1))).flatten()  # noqa: F841
    items: stream[Tuple[int, str]] = stream([{0: "a"}.items()]).flatten()
    items = stream[ItemsView[int, str]]([{0: "a"}.items()]).flatten()  # noqa: F841


@pytest.mark.parametrize(
    "itype, to_iter",
    [("sync", identity), ("async", identity), ("async", to_aiter)],
)
def test_flatten_without_concurrency(
    itype: IterableType, to_iter: Callable[[Any], Any]
) -> None:
    """Flatten with concurrency=1 should yield elements in order of nested for loop."""
    s = stream((range(3), range(3, 6), range(6, 9))).map(to_iter).flatten()
    assert alist_or_list(s, itype) == list(range(9))


@pytest.mark.parametrize(
    "itype, to_iter",
    [("sync", identity), ("async", identity), ("async", to_aiter)],
)
def test_flatten_with_concurrency(
    itype: IterableType,
    to_iter: Callable[[Iterable[int]], Union[Iterator[int], AsyncIterator[int]]],
) -> None:
    s = (
        stream(
            [
                range(4),  # 1st
                range(4, 5),  # 2nd
                range(5, 8),  # 3rd
                range(8, 10),  # 4th
            ]
        )
        .map(to_iter)
        .flatten(concurrency=2)
    )

    assert alist_or_list(s, itype) == [
        # the 2 iterables currently flattened are 1st and 2nd
        0,  # 1st elem from 1st iterable
        4,  # 1st elem from 2nd iterable
        1,  # 2nd elem from 1st iterable
        # the 2nd iterable exhausts, the 3rd will be enter the flattened next
        2,  # 3rd elem from 1st iterable
        5,  # 1st elem from 3rd iterable
        3,  # 4th elem from 1st iterable
        6,  # 2nd elem from 3rd iterable
        # the 1st iterable exhausts, the 4nd will be enter the flattened
        7,  # 3rd elem from 3rd iterable
        8,  # 1st elem from 4th iterable
        9,  # 2nd elem from 4th iterable
    ]


@pytest.mark.parametrize(
    "itype, to_iter",
    [
        ("sync", stream.__iter__),
        ("async", stream.__iter__),
        ("async", stream.__aiter__),
    ],
)
def test_flatten_with_concurrency_as_completed(
    itype: IterableType,
    to_iter: Callable[[Iterable[float]], Union[Iterator[float], AsyncIterator[float]]],
) -> None:
    s = (
        stream(
            [
                [0, 0.1, 1.1],
                [1, 0.5],
            ]
        )
        .map(stream)
        .map(lambda s: s.do(asyncio.sleep if itype == "async" else time.sleep))
        .map(to_iter)
        .flatten(concurrency=2, as_completed=True)
    )

    assert alist_or_list(s, itype) == [0, 0.1, 1, 1.1, 0.5]


@pytest.mark.parametrize("concurrency", (1, 2))
@pytest.mark.parametrize(
    "itype, to_iter",
    [("sync", identity), ("async", identity), ("async", to_aiter)],
)
@pytest.mark.parametrize("as_completed", [False, True])
def test_flatten_with_exceptions(
    concurrency: int,
    itype: IterableType,
    to_iter: Callable[[Any], Any],
    as_completed: bool,
) -> None:
    """Flatten should be resilient to exceptions raised upstream or by `next(elem)`."""
    value_errors: List[Exception] = []
    type_errors: List[Exception] = []
    test_errors: List[Exception] = []
    # '0' will lead to `TypeError`s when flatten gets next inner iterable
    # RaisingOnIterIterable/RaisingOnAiterAsyncIterable will lead to `TestError`s when flatten gets iterator from next inner iterable
    # '-' will lead to `ValueError`s when flatten calls next on element
    elems = cast(
        Tuple[str, ...],
        (
            "0-1",
            "234",
            "",
            "",
            "---",
            "---",
            RaisingOnIterIterable(),
            0,
            "5--",
            RaisingOnAiterAsyncIterable(),
        ),
    )
    s = (
        stream(elems)
        .map(lambda chars: to_iter(map(int, chars)) if type(chars) is str else chars)
        .flatten(concurrency=concurrency, as_completed=as_completed)
        .catch(ValueError, do=value_errors.append)
        .catch(TypeError, do=type_errors.append)
        .catch(TestError, do=test_errors.append)
    )
    if concurrency == 1:
        assert alist_or_list(s, itype) == list(range(6))
    elif as_completed:
        assert set(alist_or_list(s, itype)) == set(range(6))
    else:
        it = aiter_or_iter(s, itype)
        for (
            expected_next_elem,
            expected_n_value_errors,
            expected_n_type_errors,
            expected_n_test_errors,
        ) in (
            (0, 0, 0, 0),
            (2, 0, 0, 0),
            (3, 1, 0, 0),
            (1, 1, 0, 0),
            (4, 1, 0, 0),
            (5, 7, 1, 1),
        ):
            assert (
                anext_or_next(it, itype) == expected_next_elem
                and len(value_errors) == expected_n_value_errors
                and len(type_errors) == expected_n_type_errors
                and len(test_errors) == expected_n_test_errors
            )
        with pytest.raises(stopiteration_type(itype)):
            anext_or_next(it, itype)
    assert len(value_errors) == "".join(e for e in elems if type(e) is str).count("-")
    assert len(type_errors) == len(
        [c for c in elems if not isinstance(c, (Iterable, AsyncIterable))]
    )
    assert len(test_errors) == sum(
        1
        for c in elems
        if isinstance(c, (RaisingOnIterIterable, RaisingOnAiterAsyncIterable))
    )


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
@pytest.mark.parametrize("as_completed", [False, True])
def test_flatten_concurrent_aclose_with_upstream_errors_in_flight(
    itype: IterableType,
    as_completed: bool,
) -> None:
    src = cast(List[List[int]], [[0, 2], [1, 3], 0, 0])
    s = stream(src).map(iter).flatten(concurrency=3, as_completed=as_completed)
    it = aiter_or_iter(s, itype)
    assert anext_or_next(it, itype) == 0
    aclose_or_close(it, itype)


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
@pytest.mark.parametrize("concurrency", (1, 2))
@pytest.mark.parametrize("as_completed", [False, True])
def test_flatten_on_non_iterable(
    itype: IterableType, concurrency: int, as_completed: bool
) -> None:
    s = stream([1, [1]]).flatten(concurrency=concurrency, as_completed=as_completed)  # type: ignore
    it = aiter_or_iter(s, itype)
    with pytest.raises(TypeError, match="flatten expects iterables but got: 1"):
        anext_or_next(it, itype)
    assert anext_or_next(it, itype) == 1


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
@pytest.mark.parametrize(
    "concurrency, expected", [(1, [0, 1, 0, 1]), (2, [0, 0, 1, 1])]
)
@pytest.mark.parametrize(
    "to_inner", [to_aiter, lambda elems: stream(elems).map(async_identity)]
)
@pytest.mark.parametrize("as_completed, collect", [(False, list), (True, Counter)])
def test_flatten_async_inner_iterables(
    itype: IterableType,
    concurrency: int,
    expected: List[int],
    to_inner: Callable[[List[int]], Any],
    as_completed: bool,
    collect: Callable[[Iterable[int]], Sequence[int]],
) -> None:
    s = stream([to_inner([0, 1]), to_inner([0, 1])]).flatten(
        concurrency=concurrency, as_completed=as_completed
    )
    assert collect(alist_or_list(s, itype)) == collect(expected)


@pytest.mark.parametrize(
    "operate", [identity, lambda s: s + [], lambda s: stream([s]).flatten()]
)
@pytest.mark.parametrize("as_completed, collect", [(False, list), (True, Counter)])
def test_flatten_concurrently_many_inner_streams_involving_async(
    operate: Callable[[stream], stream],
    as_completed: bool,
    collect: Callable[[Iterable[int]], Sequence[int]],
) -> None:
    ints = range(10)
    inner = operate(stream(ints).map(async_identity))
    num_inner_iters = 8
    src = [inner] * num_inner_iters
    # enough concurrency for errors to be raised if the same loop was run from multiple threads
    concurrency = 4
    for _ in range(10):
        assert collect(
            stream(src).flatten(concurrency=concurrency, as_completed=as_completed)
        ) == collect(
            i
            for _ in range(num_inner_iters // concurrency)
            for i in ints
            for _ in range(concurrency)
        )


@pytest.mark.asyncio
async def test_flatten_async_inner_iter_during_sync_iteration_in_running_loop() -> None:
    with pytest.raises(
        RuntimeError, match="event loop already running: do an async iteration instead"
    ):
        next(iter(stream([to_aiter([0])]).flatten()))


@pytest.mark.parametrize("itype", ITERABLE_TYPES)
@pytest.mark.parametrize("as_completed, collect", [(False, list), (True, Counter)])
def test_flatten_provided_executor(
    itype: IterableType,
    as_completed: bool,
    collect: Callable[[Iterable[int]], Sequence[int]],
) -> None:
    with ThreadPoolExecutor(2) as executor:
        s = stream([[0, 2, 4], [1, 3, 5], [6, 7, 8]]).flatten(
            concurrency=executor, as_completed=as_completed
        )
        it = aiter_or_iter(s, itype)
        assert collect(alist_or_list(it, itype)) == collect(range(9))
        aclose_or_close(it, itype)
        assert not executor._shutdown


@pytest.mark.parametrize("as_completed", [False, True])
@pytest.mark.asyncio
async def test_flatten_close_leaves_no_unretrieved_errors(
    as_completed: bool,
) -> None:
    with collecting_unretrievables() as unretrieved_errors:
        it = (
            stream([range(4), stream("-").map(int), map(int, "-")])
            .flatten(concurrency=3, as_completed=as_completed)
            .__aiter__()
        )
        assert await it.__anext__() == 0
        await asyncio.sleep(0)
        await it.aclose()
        del it
        # there will be no "exception was never retrieved" warning
        assert not unretrieved_errors
