from collections import Counter
from functools import partial
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Callable,
    Iterable,
    Iterator,
    List,
    Union,
    cast,
)

import pytest

from streamable import stream
from streamable._tools._iter import async_iter
from tests.utils.functions import (
    async_slow_identity,
    identity,
    slow_identity,
    slow_identity_duration,
    sync_to_bi_iterable,
)
from tests.utils.iteration import (
    ITERABLE_TYPES,
    IterableType,
    alist,
    alist_or_list,
    anext_or_next,
    aiter_or_iter,
)
from tests.utils.source import N, ints_src
from tests.utils.timing import timestream


def test_flatten_typing() -> None:
    flattened_iterator_stream: stream[str] = stream("abc").map(iter).flatten()  # noqa: F841
    flattened_list_stream: stream[str] = stream("abc").map(list).flatten()  # noqa: F841
    flattened_set_stream: stream[str] = stream("abc").map(set).flatten()  # noqa: F841
    flattened_map_stream: stream[str] = (  # noqa: F841
        stream("abc").map(lambda char: map(lambda x: x, char)).flatten()
    )
    flattened_filter_stream: stream[str] = (  # noqa: F841
        stream("abc").map(lambda char: filter(lambda _: True, char)).flatten()
    )

    flattened_asynciter_stream: stream[str] = (  # noqa: F841
        stream("abc").map(iter).map(async_iter).flatten()
    )
    flattened_range_stream: stream[int] = (  # noqa: F841
        stream((ints_src, ints_src)).flatten()
    )


@pytest.mark.parametrize(
    "concurrency, itype, to_iter",
    [
        (concurrency, itype, to_iter)
        for concurrency in (1, 2)
        for itype in ITERABLE_TYPES
        for to_iter in (identity, async_iter)
    ],
)
def test_flatten(
    concurrency: int,
    itype: IterableType,
    to_iter: Callable[[Any], Any],
) -> None:
    n_iterables = 32
    it = list(range(N // n_iterables))
    double_it = it + it
    iterables_stream = stream(
        [sync_to_bi_iterable(double_it)]
        + [sync_to_bi_iterable(it) for _ in range(n_iterables)]
    )
    if concurrency == 1:
        # At concurrency == 1, `flatten` method should yield all the upstream iterables' elements in the order of a nested for loop.
        assert alist_or_list(
            iterables_stream.map(to_iter).flatten(concurrency=concurrency),
            itype=itype,
        ) == [elem for iterable in iterables_stream for elem in iterable]
    else:
        # At concurrency > 1, the `flatten` method should yield all the upstream iterables' elements.
        assert Counter(
            alist_or_list(
                iterables_stream.map(to_iter).flatten(concurrency=concurrency),
                itype=itype,
            )
        ) == Counter(list(it) * n_iterables + double_it)

    # At any concurrency the `flatten` method should continue flattening even if an iterable' __next__ raises an exception.
    assert alist_or_list(
        stream([[4, 3, 2, 0], [1, 0, -1], [0, -2, -3]])
        .map(lambda iterable: sync_to_bi_iterable(map(lambda n: 1 / n, iterable)))
        .map(to_iter)
        .flatten(
            concurrency=concurrency,
        )
        .catch(ZeroDivisionError, replace=lambda e: float("inf")),
        itype=itype,
    ) == (
        [
            0.25,
            1 / 3,
            0.5,
            float("inf"),
            1,
            float("inf"),
            -1,
            float("inf"),
            -0.5,
            -1 / 3,
        ]
        if concurrency == 1
        else [
            0.25,
            1,
            1 / 3,
            float("inf"),
            0.5,
            -1,
            float("inf"),
            float("inf"),
            -0.5,
            -1 / 3,
        ]
    )
    # At any concurrency the `flatten` method should continue pulling upstream iterables even if upstream raises an exception.
    assert alist_or_list(
        stream([[4, 3, 2], [], [1, 0]])
        .do(lambda ints: 1 / len(ints))
        .map(sync_to_bi_iterable)
        .map(to_iter)
        .flatten(
            concurrency=concurrency,
        )
        .catch(ZeroDivisionError, replace=lambda e: -1),
        itype=itype,
    ) == ([4, 3, 2, -1, 1, 0] if concurrency == 1 else [4, -1, 3, 1, 2, 0])
    # At any concurrency the `flatten` method should continue pulling upstream iterables even if upstream's __iter__ raises an exception.
    assert alist_or_list(
        stream(
            [
                sync_to_bi_iterable([4, 3, 2]),
                cast(List[int], None),
                sync_to_bi_iterable([1, 0]),
            ]
        )
        .map(to_iter)
        .flatten(
            concurrency=concurrency,
        )
        .catch(AttributeError, replace=lambda e: -1),
        itype=itype,
    ) == ([4, 3, 2, -1, 1, 0] if concurrency == 1 else [4, -1, 3, 1, 2, 0])
    # `flatten` should not yield any element if upstream elements are empty iterables, and be resilient to recursion issue in case of successive empty upstream iterables.
    assert (
        alist_or_list(
            stream([sync_to_bi_iterable(iter([])) for _ in range(2000)])
            .map(to_iter)
            .flatten(
                concurrency=concurrency,
            ),
            itype=itype,
        )
        == []
    )
    # `flatten` should raise if an upstream element is not iterable.
    with pytest.raises((TypeError, AttributeError)):
        anext_or_next(
            aiter_or_iter(
                stream(cast(Union[Iterable, AsyncIterable], ints_src))
                .map(to_iter)
                .flatten(),
                itype=itype,
            ),
            itype=itype,
        )


@pytest.mark.parametrize("itype", [AsyncIterable])
@pytest.mark.asyncio
async def test_flatten_within_async(itype: IterableType) -> None:
    assert await alist(
        stream([stream(ints_src), stream(ints_src).__aiter__()]).flatten()
    ) == list(ints_src) + list(ints_src)


@pytest.mark.parametrize("concurrency", [1, 2])
@pytest.mark.parametrize("itype", ITERABLE_TYPES)
def test_flatten_heterogeneous_sync_async_elements(
    itype: IterableType, concurrency: int
) -> None:
    async def aiterator() -> AsyncIterator[int]:
        yield 0
        yield 1

    def iterator() -> Iterator[int]:
        yield 0
        yield 1

    assert alist_or_list(
        stream(
            cast(
                List[Union[AsyncIterator, Iterator]],
                [aiterator(), iterator(), aiterator(), iterator()],
            )
        ).flatten(concurrency=concurrency),
        itype=itype,
    ) == ([0, 1, 0, 1, 0, 1, 0, 1] if concurrency == 1 else [0, 0, 1, 1, 0, 0, 1, 1])


@pytest.mark.parametrize(
    "itype, slow, to_iter",
    [
        (itype, slow, to_iter)
        for slow, to_iter in (
            (partial(stream.map, into=slow_identity), stream.__iter__),
            (partial(stream.map, into=async_slow_identity), stream.__aiter__),
        )
        for itype in ITERABLE_TYPES
    ],
)
def test_flatten_concurrency(
    itype: IterableType,
    slow: Callable[..., Any],
    to_iter: Callable[..., Any],
) -> None:
    concurrency = 2
    iterable_size = 5
    runtime, res = timestream(
        stream(
            [
                slow(stream(["a"] * iterable_size)),
                slow(stream(["b"] * iterable_size)),
                slow(stream(["c"] * iterable_size)),
            ]
        )
        .map(to_iter)
        .flatten(
            concurrency=concurrency,
        ),
        times=3,
        itype=itype,
    )
    # `flatten` should process 'a's and 'b's concurrently and then 'c's
    assert res == ["a", "b"] * iterable_size + ["c"] * iterable_size

    a_runtime = b_runtime = c_runtime = iterable_size * slow_identity_duration
    expected_runtime = (a_runtime + b_runtime) / concurrency + c_runtime
    # `flatten` should process 'a's and 'b's concurrently and then 'c's without concurrency
    assert runtime == pytest.approx(expected_runtime, rel=0.15)
