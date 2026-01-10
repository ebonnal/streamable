import asyncio
from concurrent.futures import ProcessPoolExecutor
import json
import logging
from pathlib import Path
import time
from datetime import timedelta
from typing import Any, Dict, Iterator, List, Tuple, TypeVar
import httpx

import pytest
import respx

from streamable._stream import stream

ints: stream[int] = stream(range(10))


pokemons: stream[str] = (
    stream(range(10))
    .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
    .throttle(5, per=timedelta(seconds=1))
    .map(httpx.AsyncClient().get, concurrency=2)
    .do(httpx.Response.raise_for_status)
    .catch(httpx.HTTPStatusError, do=logging.error)
    .map(lambda poke: poke.json()["name"])
)

three_ints_per_second: stream[int] = ints.throttle(5, per=timedelta(seconds=1))

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

T = TypeVar("T")


def test_iterate() -> None:
    assert list(pokemons) == POKEMONS
    assert [poke for poke in pokemons] == POKEMONS


@pytest.mark.asyncio
async def test_aiterate() -> None:
    assert [poke async for poke in pokemons] == POKEMONS


def test_map_example() -> None:
    str_ints: stream[str] = ints.map(str)

    assert list(str_ints) == ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]


def test_thread_concurrent_map_example() -> None:
    import httpx

    pokemons: stream[str] = (
        stream(range(1, 4))
        .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
        .map(httpx.Client().get, concurrency=2)
        .map(httpx.Response.json)
        .map(lambda poke: poke["name"])
    )
    assert list(pokemons) == ["bulbasaur", "ivysaur", "venusaur"]


def test_process_concurrent_map_example() -> None:
    with ProcessPoolExecutor(10) as processes:
        state: List[int] = []
        # ints are mapped
        assert list(ints.map(state.append, concurrency=processes)) == [None] * 10
        # but the `state` of the main process is not mutated
        assert state == []


@pytest.mark.asyncio
async def test_async_amap_example_aiter() -> None:
    import httpx

    pokemons: stream[str] = (
        stream(range(1, 4))
        .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
        .map(httpx.Client().get, concurrency=2)
        .map(lambda poke: poke.json()["name"])
    )
    # consume as AsyncIterable
    assert [name async for name in pokemons] == ["bulbasaur", "ivysaur", "venusaur"]


def test_async_amap_example_iter() -> None:
    import httpx

    pokemons: stream[str] = (
        stream(range(1, 4))
        .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
        .map(httpx.AsyncClient().get, concurrency=2)
        .map(lambda poke: poke.json()["name"])
    )
    # consume as Iterable (the concurrency will happen via a dedicated event loop)
    assert [name for name in pokemons] == ["bulbasaur", "ivysaur", "venusaur"]


def test_starmap_example() -> None:
    from streamable import star

    indexed_pokemons: stream[str] = stream(enumerate(pokemons)).map(
        star(lambda index, poke: f"#{index + 1} {poke}")
    )
    assert list(indexed_pokemons) == [
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
    ints_into_state: stream[int] = ints.do(state.append)

    assert list(ints_into_state) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    assert state == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


def test_filter_example() -> None:
    even_ints: stream[int] = ints.filter(lambda n: n % 2 == 0)

    assert list(even_ints) == [0, 2, 4, 6, 8]


def test_throttle_example() -> None:
    from datetime import timedelta

    three_ints_per_second: stream[int] = ints.throttle(3, per=timedelta(seconds=1))

    start = time.perf_counter()
    # takes 3s: ceil(10 ints / 3 per_second) - 1
    assert list(three_ints_per_second) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    assert 2.99 < time.perf_counter() - start < 3.25

    ints_every_100_millis = ints.throttle(1, per=timedelta(milliseconds=100))

    start = time.perf_counter()
    # takes 900 millis: (10 ints - 1) * 100 millis
    assert list(ints_every_100_millis) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    assert 0.89 < time.perf_counter() - start < 0.95


def test_group_example() -> None:
    int_batches: stream[List[int]] = ints.group(5)

    assert list(int_batches) == [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]]

    from datetime import timedelta

    int_1sec_batches: stream[List[int]] = ints.throttle(
        2, per=timedelta(seconds=1)
    ).group(every=timedelta(seconds=0.99))

    assert list(int_1sec_batches) == [[0, 1, 2], [3, 4], [5, 6], [7, 8], [9]]

    ints_by_parity: stream[Tuple[str, List[int]]] = ints.group(
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
    inverses: stream[float] = ints.map(lambda n: round(1 / n, 2)).catch(
        ZeroDivisionError
    )

    assert list(inverses) == [1.0, 0.5, 0.33, 0.25, 0.2, 0.17, 0.14, 0.12, 0.11]

    import httpx

    with respx.mock:
        respx.get("https://github.com/foo/bar").mock(return_value=httpx.Response(404))
        respx.get("https://github.com").mock(return_value=httpx.Response(200))
        respx.get("https://foo.bar").mock(
            side_effect=httpx.ConnectError(
                "[Errno 8] nodename nor servname provided, or not known"
            )
        )

        status_codes_ignoring_resolution_errors: stream[int] = (
            stream(
                ["https://github.com", "https://foo.bar", "https://github.com/foo/bar"]
            )
            .map(httpx.get, concurrency=2)
            .catch(httpx.ConnectError, where=lambda exc: "not known" in str(exc))
            .map(lambda response: response.status_code)
        )

        assert list(status_codes_ignoring_resolution_errors) == [200, 404]

    errors: List[Exception] = []
    inverses_: stream[float] = ints.map(lambda n: round(1 / n, 2)).catch(
        ZeroDivisionError, do=errors.append
    )
    assert list(inverses_) == [1.0, 0.5, 0.33, 0.25, 0.2, 0.17, 0.14, 0.12, 0.11]
    assert len(errors) == 1

    inverses = ints.map(lambda n: round(1 / n, 2)).catch(
        ZeroDivisionError, replace=lambda e: float("inf")
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

    inverses = ints.map(lambda n: round(1 / n, 2)).catch(ZeroDivisionError, stop=True)

    assert list(inverses) == []


def test_take_example() -> None:
    first_5_ints: stream[int] = ints.take(5)

    assert list(first_5_ints) == [0, 1, 2, 3, 4]

    first_5_ints = ints.take(until=lambda n: n == 5)

    assert list(first_5_ints) == [0, 1, 2, 3, 4]


def test_skip_example() -> None:
    ints_after_5: stream[int] = ints.skip(5)

    assert list(ints_after_5) == [5, 6, 7, 8, 9]

    ints_after_5 = ints.skip(until=lambda n: n >= 5)

    assert list(ints_after_5) == [5, 6, 7, 8, 9]


def test_distinct_example() -> None:
    seen: set[str] = set()

    unique_ints: stream[int] = (
        stream("001000111").filter(lambda char: char not in seen).do(seen.add).map(int)
    )

    assert list(unique_ints) == [0, 1]


def test_buffer_example() -> None:
    pulled: list[int] = []
    buffered_ints = ints.do(pulled.append).buffer(2)
    assert next(iter(buffered_ints)) == 0
    assert pulled == [0, 1, 2]


def test_observe_example() -> None:
    observed_ints: stream[int] = ints.observe("ints")
    assert list(observed_ints) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


def test_plus_example() -> None:
    assert list(ints + ints) == [
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


def test_zip_example() -> None:
    from streamable import star

    cubes: stream[int] = stream(
        zip(ints, ints, ints)
    ).map(  # stream[tuple[int, int, int]]
        star(lambda a, b, c: a * b * c)
    )  # stream[int]

    assert list(cubes) == [0, 1, 8, 27, 64, 125, 216, 343, 512, 729]


def test_call_example() -> None:
    state: List[int] = []
    pipeline: stream[int] = ints.do(state.append)
    assert pipeline() is pipeline
    assert state == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


@pytest.mark.asyncio
async def test_await_example() -> None:
    state: List[int] = []
    pipeline: stream[int] = ints.do(state.append)
    assert pipeline is await pipeline
    assert state == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


def test_non_stopping_exceptions_example() -> None:
    from contextlib import suppress

    casted_ints: Iterator[int] = iter(stream("0123_56789").map(int).group(3).flatten())
    collected_casted_ints: List[int] = []

    with suppress(ValueError):
        collected_casted_ints.extend(casted_ints)
    assert collected_casted_ints == [0, 1, 2, 3]

    collected_casted_ints.extend(casted_ints)
    assert collected_casted_ints == [0, 1, 2, 3, 5, 6, 7, 8, 9]


def test_pipe(tmp_path: Path) -> None:
    import polars as pl

    pokemons.pipe(pl.DataFrame, schema=["name"]).write_csv(tmp_path / "ints.parquet")


def test_cast() -> None:
    _dicts: stream[Dict[str, str]] = (
        stream(['{"foo": "bar"}', '{"foo": "baz"}'])
        .map(json.loads)  # stream[Any]
        .cast(Dict[str, str])  # stream[Dict[str, str]]
    )
    assert list(_dicts) == [{"foo": "bar"}, {"foo": "baz"}]


def test_etl_example(tmp_path: Path) -> None:  # pragma: no cover
    import csv
    from datetime import timedelta
    from itertools import count
    import httpx
    from streamable import stream

    with open(tmp_path / "quadruped_pokemons.csv", mode="w") as file:
        fields = ["id", "name", "is_legendary", "base_happiness", "capture_rate"]
        writer = csv.DictWriter(file, fields, extrasaction="ignore")
        writer.writeheader()

        pipeline = (
            # Infinite stream[int] of Pokemon ids starting from Pokémon #1: Bulbasaur
            stream(count(1))
            # Limit to 16 requests per second to be friendly to our fellow PokéAPI devs
            .throttle(16, per=timedelta(seconds=1))
            # GET pokemons concurrently using a pool of 8 threads
            .map(lambda poke_id: f"https://pokeapi.co/api/v2/pokemon-species/{poke_id}")
            .map(httpx.Client().get, concurrency=8)
            .do(httpx.Response.raise_for_status)
            .map(httpx.Response.json)
            # Stop when reaching the 1st pokemon of the 4th generation
            .take(until=lambda poke: poke["generation"]["name"] == "generation-iv")
            .observe("pokemons")
            # Keep only quadruped Pokemons
            .filter(lambda poke: poke["shape"]["name"] == "quadruped")
            # Write a batch of pokemons every 5 seconds to the CSV file
            .group(every=timedelta(seconds=5))
            .do(writer.writerows)
            .flatten()
            .observe("written pokemons")
        )

        # Call the stream to consume it (as an Iterable)
        # without collecting its elements
        pipeline()


@pytest.mark.asyncio
async def test_async_etl_example(tmp_path: Path) -> None:  # pragma: no cover
    import csv
    from datetime import timedelta
    from itertools import count
    import httpx
    from streamable import stream

    with open(tmp_path / "quadruped_pokemons.csv", mode="w") as file:
        fields = ["id", "name", "is_legendary", "base_happiness", "capture_rate"]
        writer = csv.DictWriter(file, fields, extrasaction="ignore")
        writer.writeheader()

        pipeline = (
            # Infinite stream[int] of Pokemon ids starting from Pokémon #1: Bulbasaur
            stream(count(1))
            # Limit to 16 requests per second to be friendly to our fellow PokéAPI devs
            .throttle(16, per=timedelta(seconds=1))
            # GET pokemons via 8 concurrent coroutines
            .map(lambda poke_id: f"https://pokeapi.co/api/v2/pokemon-species/{poke_id}")
            .map(httpx.AsyncClient().get, concurrency=8)
            .do(httpx.Response.raise_for_status)
            .map(httpx.Response.json)
            # Stop when reaching the 1st pokemon of the 4th generation
            .take(until=lambda poke: poke["generation"]["name"] == "generation-iv")
            .observe("pokemons")
            # Keep only quadruped Pokemons
            .filter(lambda poke: poke["shape"]["name"] == "quadruped")
            # Write a batch of pokemons every 5 seconds to the CSV file
            .group(every=timedelta(seconds=5))
            .do(writer.writerows)
            .flatten()
            .observe("written pokemons")
        )

        # await the stream to consume it (as an AsyncIterable)
        # without collecting its elements
        await pipeline


# fmt: on
