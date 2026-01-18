import asyncio
from concurrent.futures import ProcessPoolExecutor
import json
import logging
from pathlib import Path
import time
from datetime import timedelta
from typing import Any, Dict, List, Tuple
import httpx
from httpx import AsyncClient, Response, HTTPStatusError

import pytest
import respx

from streamable._stream import stream


pokemons: stream[str] = (
    stream(range(10))
    .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
    .throttle(5, per=timedelta(seconds=1))
    .map(AsyncClient().get, concurrency=2)
    .do(Response.raise_for_status)
    .catch(HTTPStatusError, do=logging.error)
    .map(lambda poke: poke.json()["name"])
)

POKEMONS = [
    "bulbasaur",
    "ivysaur",
    "venusaur",
    "charmander",
    "charmeleon",
    "charizard",
    "squirtle",
    "wartortle",
    "blastoise",
]


def test_iterate() -> None:
    assert list(pokemons) == POKEMONS


@pytest.mark.asyncio
async def test_aiterate() -> None:
    assert [poke async for poke in pokemons] == POKEMONS


def test_map_example() -> None:
    int_chars: stream[str] = stream(range(10)).map(str)

    assert list(int_chars) == ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]


def test_map_example_thread_concurrency() -> None:
    pokemons: stream[str] = (
        stream(range(1, 4))
        .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
        .map(httpx.Client().get, concurrency=2)
        .map(lambda poke: poke.json()["name"])
    )
    assert list(pokemons) == ["bulbasaur", "ivysaur", "venusaur"]


def test_map_example_process_concurrency() -> None:
    with ProcessPoolExecutor(max_workers=10) as processes:
        state: List[int] = []
        # ints are mapped
        assert (
            list(stream(range(10)).map(state.append, concurrency=processes))
            == [None] * 10
        )
        # but the `state` of the main process is not mutated
        assert state == []


@pytest.mark.asyncio
async def test_map_example_async_concurrency_aiter() -> None:
    pokemons: stream[str] = (
        stream(range(1, 4))
        .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
        .map(httpx.AsyncClient().get, concurrency=2)
        .map(lambda poke: poke.json()["name"])
    )

    assert [name async for name in pokemons] == ["bulbasaur", "ivysaur", "venusaur"]


def test_map_example_async_concurrency_iter() -> None:
    pokemons: stream[str] = (
        stream(range(1, 4))
        .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
        .map(httpx.AsyncClient().get, concurrency=2)
        .map(lambda poke: poke.json()["name"])
    )

    assert list(pokemons) == ["bulbasaur", "ivysaur", "venusaur"]


def test_starmap_example() -> None:
    from streamable import star

    enumerated_pokes: stream[str] = stream(enumerate(pokemons)).map(
        star(lambda index, poke: f"#{index + 1} {poke}")
    )
    assert list(enumerated_pokes) == [
        "#1 bulbasaur",
        "#2 ivysaur",
        "#3 venusaur",
        "#4 charmander",
        "#5 charmeleon",
        "#6 charizard",
        "#7 squirtle",
        "#8 wartortle",
        "#9 blastoise",
    ]


def test_do_example() -> None:
    state: List[int] = []
    store_ints: stream[int] = stream(range(10)).do(state.append)

    assert list(store_ints) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    assert state == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


def test_filter_example() -> None:
    even_ints: stream[int] = stream(range(10)).filter(lambda n: n % 2 == 0)

    assert list(even_ints) == [0, 2, 4, 6, 8]


def test_throttle_example() -> None:
    from datetime import timedelta

    three_ints_per_second: stream[int] = stream(range(10)).throttle(
        3, per=timedelta(seconds=1)
    )

    start = time.perf_counter()
    # takes 3s: ceil(10 ints / 3 per_second) - 1
    assert list(three_ints_per_second) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    assert 2.99 < time.perf_counter() - start < 3.25

    ints_every_100_millis = stream(range(10)).throttle(
        1, per=timedelta(milliseconds=100)
    )

    start = time.perf_counter()
    # takes 900 millis: (10 ints - 1) * 100 millis
    assert list(ints_every_100_millis) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    assert 0.89 < time.perf_counter() - start < 0.95


def test_group_example() -> None:
    int_batches: stream[List[int]] = stream(range(10)).group(5)

    assert list(int_batches) == [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]]

    from datetime import timedelta

    int_1sec_batches: stream[List[int]] = (
        stream(range(10))
        .throttle(2, per=timedelta(seconds=1))
        .group(every=timedelta(seconds=0.99))
    )

    assert list(int_1sec_batches) == [[0, 1, 2], [3, 4], [5, 6], [7, 8], [9]]

    ints_by_parity: stream[Tuple[str, List[int]]] = stream(range(10)).group(
        by=lambda n: "odd" if n % 2 else "even"
    )

    assert list(ints_by_parity) == [("even", [0, 2, 4, 6, 8]), ("odd", [1, 3, 5, 7, 9])]

    from streamable import star

    counts_by_parity: stream[Tuple[str, int]] = ints_by_parity.map(
        star(lambda parity, ints: (parity, len(ints)))
    )

    assert list(counts_by_parity) == [("even", 5), ("odd", 5)]


def test_flatten_example() -> None:
    chars: stream[str] = stream(["hel", "lo!"]).flatten()

    assert list(chars) == ["h", "e", "l", "l", "o", "!"]

    chars = stream(["hel", "lo", "!"]).flatten(concurrency=2)

    assert list(chars) == ["h", "l", "e", "o", "l", "!"]


def test_catch_example() -> None:
    inverses: stream[float] = (
        stream(range(10)).map(lambda n: round(1 / n, 2)).catch(ZeroDivisionError)
    )

    assert list(inverses) == [1.0, 0.5, 0.33, 0.25, 0.2, 0.17, 0.14, 0.12, 0.11]

    with respx.mock:
        respx.get("https://github.com").mock(return_value=httpx.Response(200))
        respx.get("https://foo.bar").mock(
            side_effect=httpx.ConnectError(
                "[Errno 8] nodename nor servname provided, or not known"
            )
        )
        respx.get("https://google.com").mock(return_value=httpx.Response(200))

        domains = ["github.com", "foo.bar", "google.com"]

        resolvable_domains: stream[str] = (
            stream(domains)
            .do(lambda domain: httpx.get(f"https://{domain}"), concurrency=2)
            .catch(httpx.HTTPError, where=lambda e: "not known" in str(e))
        )

        assert list(resolvable_domains) == ["github.com", "google.com"]

    errors: List[Exception] = []
    inverses = (
        stream(range(10))
        .map(lambda n: round(1 / n, 2))
        .catch(ZeroDivisionError, do=errors.append)
    )
    assert list(inverses) == [1.0, 0.5, 0.33, 0.25, 0.2, 0.17, 0.14, 0.12, 0.11]
    assert len(errors) == 1

    inverses = (
        stream(range(10))
        .map(lambda n: round(1 / n, 2))
        .catch(ZeroDivisionError, replace=lambda e: float("inf"))
    )

    assert list(inverses) == [
        float("inf"),
        1.0,
        0.5,
        0.33,
        0.25,
        0.2,
        0.17,
        0.14,
        0.12,
        0.11,
    ]

    inverses = (
        stream(range(10))
        .map(lambda n: round(1 / n, 2))
        .catch(ZeroDivisionError, stop=True)
    )

    assert list(inverses) == []


def test_take_example() -> None:
    first_5_ints: stream[int] = stream(range(10)).take(5)

    assert list(first_5_ints) == [0, 1, 2, 3, 4]

    first_5_ints = stream(range(10)).take(until=lambda n: n == 5)

    assert list(first_5_ints) == [0, 1, 2, 3, 4]


def test_skip_example() -> None:
    ints_after_5: stream[int] = stream(range(10)).skip(5)

    assert list(ints_after_5) == [5, 6, 7, 8, 9]

    ints_after_5 = stream(range(10)).skip(until=lambda n: n >= 5)

    assert list(ints_after_5) == [5, 6, 7, 8, 9]


def test_distinct_example() -> None:
    seen: set[str] = set()

    unique_ints: stream[int] = (
        stream("001000111").filter(lambda char: char not in seen).do(seen.add).map(int)
    )

    assert list(unique_ints) == [0, 1]


def test_buffer_example() -> None:
    pulled: List[int] = []
    buffered_ints = stream(range(10)).do(pulled.append).buffer(5)
    assert next(iter(buffered_ints)) == 0
    assert pulled == [0, 1, 2, 3, 4, 5]


def test_observe_example() -> None:
    observed_ints: stream[int] = stream(range(10)).observe("ints")
    assert list(observed_ints) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


def test_plus_example() -> None:
    concatenated_ints = stream(range(10)) + stream(range(10))
    assert list(concatenated_ints) == [
        0,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        0,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
    ]


def test_cast_example() -> None:
    docs: stream[Any] = stream(['{"foo": "bar"}', '{"foo": "baz"}']).map(json.loads)
    dicts: stream[Dict[str, str]] = docs.cast(Dict[str, str])
    assert dicts is docs


def test_func_source() -> None:
    from queue import Queue, Empty

    ints_queue: Queue[int] = Queue()
    for i in range(10):
        ints_queue.put(i)

    ints: stream[int] = stream(lambda: ints_queue.get(timeout=2)).catch(
        Empty, stop=True
    )
    assert list(ints) == list(range(10))


@pytest.mark.asyncio
async def test_afunc_source() -> None:
    from asyncio import Queue, TimeoutError

    ints_queue: Queue[int] = Queue()
    for i in range(10):
        await ints_queue.put(i)

    async def queue_get() -> int:
        return await asyncio.wait_for(ints_queue.get(), timeout=2)

    ints: stream[int] = stream(queue_get).catch(TimeoutError, stop=True)
    assert [i async for i in ints] == list(range(10))


def test_call_example() -> None:
    state: List[int] = []
    pipeline: stream[int] = stream(range(10)).do(state.append)
    assert pipeline() is pipeline
    assert state == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


@pytest.mark.asyncio
async def test_await_example() -> None:
    state: List[int] = []
    pipeline: stream[int] = stream(range(10)).do(state.append)
    assert pipeline is await pipeline
    assert state == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


def test_pipe(tmp_path: Path) -> None:
    import polars as pl

    pokemons.pipe(pl.DataFrame, schema=["name"]).write_csv(tmp_path / "pokemons.csv")


def test_cast() -> None:
    _dicts: stream[Dict[str, str]] = (
        stream(['{"foo": "bar"}', '{"foo": "baz"}'])
        .map(json.loads)  # stream[Any]
        .cast(Dict[str, str])  # stream[Dict[str, str]]
    )
    assert list(_dicts) == [{"foo": "bar"}, {"foo": "baz"}]
