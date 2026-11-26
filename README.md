# ༄ `streamable`

> sync/async iterable streams for Python

`stream[T]` wraps any `Iterable[T]` or `AsyncIterable[T]` with a lazy fluent interface covering concurrency, batching, buffering, rate limiting, progress observation, and error handling.

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/release/python-3820/)
[![PyPI version](https://img.shields.io/pypi/v/streamable.svg)](https://pypi.org/project/streamable/)
[![Anaconda-Server Badge](https://anaconda.org/conda-forge/streamable/badges/version.svg)](https://anaconda.org/conda-forge/streamable)
[![branch coverage](https://img.shields.io/codecov/c/github/ebonnal/streamable?label=branch%20coverage)](https://codecov.io/gh/ebonnal/streamable)
[![readthedocs](https://app.readthedocs.org/projects/streamable/badge/?version=latest&style=social)](https://streamable.readthedocs.io/)

## 1. install

```
pip install streamable==2.1.0
```
No dependencies. Also available on conda. Make sure to pin/lock a version to prevent supply-chain attacks.

Tested on Linux/Windows/macOS, [CPython](https://github.com/python/cpython) 3.8 to 3.15 and 3.14t ([free-threaded](https://docs.python.org/3/howto/free-threading-python.html)), [PyPy](https://github.com/pypy/pypy), [uvloop](https://github.com/magicstack/uvloop).

## 2. import

```python
from streamable import stream
```

## 3. init

Create a `stream[T]` from an `Iterable[T]` or `AsyncIterable[T]`:

```python
ints: stream[int] = stream(range(10))
```

## 4. operate

Chain lazy operations (accepting sync and async functions):

```python
import logging
from datetime import timedelta
from httpx import HTTPStatusError, Response, get

pokemons: stream[str] = (
    stream(range(10))
    .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
    .throttle(5, per=timedelta(seconds=1))
    .map(get, concurrency=2)
    .do(Response.raise_for_status)
    .catch(HTTPStatusError, do=logging.warning)
    .map(lambda poke: poke.json()["name"])
)
```

Each operation returns a new `stream`, and source elements are processed on-the-fly during iteration.

## 5. iterate

A `stream[T]` is `Iterable[T]` and `AsyncIterable[T]`:

```python
>>> list(pokemons)
['bulbasaur', 'ivysaur', 'venusaur', 'charmander', 'charmeleon', 'charizard', 'squirtle', 'wartortle', 'blastoise']
>>> [pokemon async for pokemon in pokemons]
['bulbasaur', 'ivysaur', 'venusaur', 'charmander', 'charmeleon', 'charizard', 'squirtle', 'wartortle', 'blastoise']
```

---

# core principles

***focused***: A `stream` exposes operations to manipulate its elements, but its source/consumption and retries are not its responsibility. It's meant to be combined with dedicated builtins and libraries: e.g. collect via `list`/`set`, reduce via `functools.reduce`, retry via `tenacity`, read/write via `pyarrow`, `psycopg`, `boto3`, `dlt`: check the [ETL example](#eg-etl-via-dlt).

***sync/async***:

- Operations accept both sync and async functions (recognised if defined via `async def` or marked via `inspect.markcoroutinefunction`).

- A `stream` supports both sync and async iteration (e.g. `for` and `async for`), and operations adapt their behavior.

- A sync iteration over a `stream` involving async functions or an `AsyncIterable` source runs as an async iteration under the hood, using the current event loop if one is set.

***resilient***: An exception raised during a `stream`'s iteration doesn't break it: you can keep iterating (until it is exhausted or [closed](#structured-concurrency)).

***structured concurrency***: The iterators returned by `stream.__iter__`/`stream.__aiter__` expose a [`.close`/`.aclose`](#structured-concurrency) method to eagerly terminate the tasks/threads they may spawn during iteration.

***match builtins***: Iteration over `stream(source).filter(pred).map(into)` introduces zero overhead vs `builtins`-based `map(into, filter(pred, source))`.

---

# operations ([docs](https://streamable.readthedocs.io/))

- mapping
    - [`.map`](#-map)
    - [`.do`](#-do)
- (un)grouping
    - [`.group`](#-group)
    - [`.flatten`](#-flatten)
- filtering
    - [`.filter`](#-filter)
    - [`.take`](#-take)
    - [`.skip`](#-skip)
- control
    - [`.catch`](#-catch)
    - [`.throttle`](#-throttle)
    - [`.buffer`](#-buffer)
    - [`.observe`](#-observe)

## ▼ `.map`

Transform elements:

```python
int_chars: stream[str] = stream(range(10)).map(str)

assert list(int_chars) == ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
```

### `concurrency`

Set the `concurrency` param to apply the transformation concurrently, preserving upstream order unless you set `as_completed=True`.

Only about `concurrency` upstream elements are in-flight for processing: an upstream element is only pulled right before yielding the next result. If the upstream is slow, that can delay the yielding of already available results, unless you place a [`.buffer`](#-buffer) directly before this `.map` to decouple upstream and downstream rates.

Check [`.close`/`.aclose`](#structured-concurrency) for *structured concurrency*.

#### via threads

If `concurrency > 1`, the transformation will be applied via `concurrency` threads:

```python
from httpx import get

pokemons: stream[str] = (
    stream(range(1, 4))
    .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
    .map(get, concurrency=2)
    .map(lambda poke: poke.json()["name"])
)
assert list(pokemons) == ['bulbasaur', 'ivysaur', 'venusaur']
```

#### via `async`

If `concurrency > 1` and the transformation is async, it will be applied via `concurrency` async tasks:

```python
from httpx import AsyncClient

# async context
async with AsyncClient() as async_http:
    pokemons: stream[str] = (
        stream(range(1, 4))
        .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
        .map(async_http.get, concurrency=2)
        .map(lambda poke: poke.json()["name"])
    )
    assert [name async for name in pokemons] == ['bulbasaur', 'ivysaur', 'venusaur']
```

```python
import asyncio
from httpx import AsyncClient

# sync context
with asyncio.Runner() as runner:
    async_http = AsyncClient()
    pokemons: stream[str] = (
        stream(range(1, 4))
        .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
        .map(async_http.get, concurrency=2)
        .map(lambda poke: poke.json()["name"])
    )
    # uses runner's loop
    assert list(pokemons) == ['bulbasaur', 'ivysaur', 'venusaur']
    runner.run(async_http.aclose())
```

#### via processes

`concurrency` can also be a `concurrent.futures.Executor`, pass a `ProcessPoolExecutor` to apply the transformations via processes:

```python
from concurrent.futures import ProcessPoolExecutor

if __name__ == "__main__":
    with ProcessPoolExecutor(max_workers=10) as processes:
        state: list[int] = []
        # ints are mapped
        assert list(
            stream(range(10))
            .map(state.append, concurrency=processes)
        ) == [None] * 10
        # the `state` of the main process is not mutated
        assert state == []
```

## ▼ `.do`

Perform side effects:

```python
state: list[int] = []
storing_ints: stream[int] = stream(range(10)).do(state.append)

assert list(storing_ints) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
assert state == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```

### `concurrency`

Same as [`.map`](#concurrency).

## ▼ `.group`

Group elements into batches...

... `up_to` a given batch size:

```python
int_batches: stream[list[int]] = stream(range(10)).group(5)

assert list(int_batches) == [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]]
```

... `within` a given time interval:

```python
from datetime import timedelta

int_1s_batches: stream[list[int]] = (
    stream(range(10))
    .throttle(2, per=timedelta(seconds=1))
    .group(within=timedelta(seconds=0.99))
)

assert list(int_1s_batches) == [[0, 1], [2, 3], [4, 5], [6, 7], [8, 9]]
```

... `by` a given key, yielding `(key, elements)` pairs:

```python
ints_by_parity: stream[tuple[str, list[int]]] = (
    stream(range(10))
    .group(by=lambda n: "odd" if n % 2 else "even")
)

assert list(ints_by_parity) == [("even", [0, 2, 4, 6, 8]), ("odd", [1, 3, 5, 7, 9])]
```

You can combine these parameters.

## ▼ `.flatten`

Explode upstream elements (`Iterable` or `AsyncIterable`):

```python
chars: stream[str] = stream(["hel", "lo!"]).flatten()

assert list(chars) == ["h", "e", "l", "l", "o", "!"]
```

### `concurrency`

Explode `concurrency` iterables at a time, yielding elements in a round-robin manner, or as they become available if `as_completed=True`.

```python
chars: stream[str] = stream(["hel", "lo", "!"]).flatten(concurrency=2)

assert list(chars) == ["h", "l", "e", "o", "l", "!"]
```

## ▼ `.filter`

Filter elements satisfying a predicate:

```python
even_ints: stream[int] = stream(range(10)).filter(lambda n: n % 2 == 0)

assert list(even_ints) == [0, 2, 4, 6, 8]
```

## ▼ `.take`

Take a given number of elements:

```python
first_5_ints: stream[int] = stream(range(10)).take(5)

assert list(first_5_ints) == [0, 1, 2, 3, 4]
```

... or take `until` a predicate is satisfied:

```python
first_5_ints: stream[int] = stream(range(10)).take(until=lambda n: n == 5)

assert list(first_5_ints) == [0, 1, 2, 3, 4]
```

## ▼ `.skip`

Skip a given number of elements:

```python
ints_after_5: stream[int] = stream(range(10)).skip(5)

assert list(ints_after_5) == [5, 6, 7, 8, 9]
```

... or skip `until` a predicate is satisfied:

```python
ints_after_5: stream[int] = stream(range(10)).skip(until=lambda n: n >= 5)

assert list(ints_after_5) == [5, 6, 7, 8, 9]
```

## ▼ `.catch`

Catch exceptions of a given type:

```python
inverses: stream[float] = (
    stream(range(10))
    .map(lambda n: round(1 / n, 2))
    .catch(ZeroDivisionError)
)

assert list(inverses) == [1.0, 0.5, 0.33, 0.25, 0.2, 0.17, 0.14, 0.12, 0.11]
```

... `where` a predicate is satisfied:

```python
from http import HTTPStatus
from httpx import HTTPStatusError, Response, get

urls = [
    "https://github.com/ebonnal",
    "https://github.com/ebonnal/streamable",
    "https://github.com/ebonnal/foo",
]
responses: stream[Response] = (
    stream(urls)
    .map(get)
    .do(Response.raise_for_status)
    .catch(
        HTTPStatusError,
        where=lambda e: e.response.status_code == HTTPStatus.NOT_FOUND,
    )
)
assert len(list(responses)) == 2
```

... `do` a side effect on catch:

```python
errors: list[Exception] = []
inverses: stream[float] = (
    stream(range(10))
    .map(lambda n: round(1 / n, 2))
    .catch(ZeroDivisionError, do=errors.append)
)
assert list(inverses) == [1.0, 0.5, 0.33, 0.25, 0.2, 0.17, 0.14, 0.12, 0.11]
assert len(errors) == 1
```

... `replace` with a value:

```python
inverses: stream[float] = (
    stream(range(10))
    .map(lambda n: round(1 / n, 2))
    .catch(ZeroDivisionError, replace=lambda e: float("inf"))
)

assert list(inverses) == [float("inf"), 1.0, 0.5, 0.33, 0.25, 0.2, 0.17, 0.14, 0.12, 0.11]
```

... `stop=True` to stop the iteration if an exception is caught:

```python
inverses: stream[float] = (
    stream(range(10))
    .map(lambda n: round(1 / n, 2))
    .catch(ZeroDivisionError, stop=True)
)

assert list(inverses) == []
```

You can combine these parameters.

## ▼ `.throttle`

Limit the number of emissions `per` time interval (sliding window):

```python
from datetime import timedelta

throttled_ints: stream[int] = stream(range(10)).throttle(3, per=timedelta(seconds=1))

# takes 3 seconds
assert list(throttled_ints) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```

## ▼ `.buffer`

Buffer upstream elements into a bounded queue via a background task/thread (decoupling upstream production rate from downstream consumption rate):

```python
from time import sleep

pulled: list[int] = []
buffered_ints = iter(
    stream(range(10))
    .do(pulled.append)
    .buffer(5)
)
assert next(buffered_ints) == 0
sleep(1e-3)
assert pulled == [0, 1, 2, 3, 4, 5]
```

## ▼ `.observe`

Observe the iteration progress:

```python
observed_ints: stream[int] = stream(range(10)).observe("ints")
assert list(observed_ints) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```

logs:

```
2025-12-23T16:43:07Z INFO observed=ints elapsed=0:00:00.000019 errors=0 elements=1
2025-12-23T16:43:07Z INFO observed=ints elapsed=0:00:00.001117 errors=0 elements=2
2025-12-23T16:43:07Z INFO observed=ints elapsed=0:00:00.001147 errors=0 elements=4
2025-12-23T16:43:07Z INFO observed=ints elapsed=0:00:00.001162 errors=0 elements=8
2025-12-23T16:43:07Z INFO observed=ints elapsed=0:00:00.001179 errors=0 elements=10
```

Logs are produced when the counts reach powers of 2 (and at exhaustion/close). Set `every` to produce them periodically:

```python
# observe every 1k elements (or errors)
observed_ints = stream(range(10)).observe("ints", every=1000)
```

```python
# observe every 5 seconds
from datetime import timedelta
observed_ints = stream(range(10)).observe("ints", every=timedelta(seconds=5))
```

Observations are logged via `logging.getLogger("streamable").info`. Set `do` to do something else with the `streamable.Observation`:

```python
observed_ints = stream(range(10)).observe("ints", do=custom_logger.info)
observed_ints = stream(range(10)).observe("ints", do=observations.append)
observed_ints = stream(range(10)).observe("ints", do=print)
```

# ••• other notes

## `+` concatenation

Concatenate a stream with another iterable:

```python
concatenated_ints: stream[int] = stream(range(5)) + range(5, 10)
assert list(concatenated_ints) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```

## `.cast` elements

Provide a type hint for elements:

```python
import json

dicts: stream[dict[str, str]] = (
    stream(['{"foo": "bar"}', '{"foo": "baz"}'])
    .map(json.loads)  # stream[Any]
    .cast(dict[str, str])
)
```

## consumption helpers

### `.__call__` a stream

Iterate as an `Iterable` until exhaustion, without collecting its elements:

```python
state: list[int] = []
pipeline: stream[int] = stream(range(10)).do(state.append)

pipeline()

assert state == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```

### `await` a stream

Iterate as an `AsyncIterable` until exhaustion, without collecting its elements:

```python
state: list[int] = []
pipeline: stream[int] = stream(range(10)).do(state.append)

await pipeline

assert state == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```

### `.pipe`

Apply a callable, passing the stream as first argument, followed by the provided `*args` and `**kwargs`:

```python
import polars as pl

pokemons: stream[str] = ...
pokemons.pipe(pl.DataFrame, schema=["name"]).write_csv("pokemons.csv")
```

## function as source

A `stream` can also be instantiated from a function (sync or async) that will be called sequentially to get the next element, until it raises `StopIteration` (or `StopAsyncIteration` if async).

e.g. stream from a `Queue`:

```python
from queue import Queue
# or from asyncio import Queue

ints_queue: Queue[int] = ...
ints: stream[int] = stream(ints_queue.get)
```

To stop on a sentinel value, either:
- add `.take(until=lambda _: _ is sentinel)`
- or use `stream(iter(ints_queue.get, sentinel))` (sync only)

## starmap

The `star` function decorator transforms a function (sync or async) that takes several positional arguments into a function that takes a tuple.

```python
from streamable import star

pokemons: stream[str] = ...  # see top of the README
enumerated_pokes: stream[str] = (
    stream(enumerate(pokemons))
    .map(star(lambda index, poke: f"#{index + 1} {poke}"))
)
assert list(enumerated_pokes) == ['#1 bulbasaur', '#2 ivysaur', '#3 venusaur', '#4 charmander', '#5 charmeleon', '#6 charizard', '#7 squirtle', '#8 wartortle', '#9 blastoise']
```

## distinct

To collect distinct elements you can `set(a_stream)`.

To deduplicate in the middle of the stream, `.filter` new values and `.do` add them into a `set` (or a fancier cache):

```python
seen: set[str] = set()

unique_ints: stream[int] = (
    stream("001000111")
    .filter(lambda _: _ not in seen)
    .do(seen.add)
    .map(int)
)

assert list(unique_ints) == [0, 1]
```

## structured concurrency

During an iteration, these operations spawn child tasks/threads:

- `.map`/`.do`/`.flatten` with `concurrency > 1`
- `.buffer`
- `.group(..., within=timedelta(...))`
- `.observe(..., every=timedelta(...))`

When the iteration is complete, all the child tasks/threads are done.

When the iterator is destroyed before exhaustion, the pending work is cancelled if possible.

`stream.__iter__`/`stream.__aiter__` returns an `Iterator`/`AsyncIterator` with a retriable `.close`/`.aclose` method to eagerly terminate any pending child tasks/threads.

### `.close`

```python
import time
from contextlib import closing

s = stream(range(10)).do(time.sleep, concurrency=4)
with closing(iter(s)) as it:
    assert next(it) == 0
    assert next(it) == 1
    # 4 worker threads processing elements 2, 3, 4, 5
# work done/cancelled and worker threads terminated
```

equivalent to:

```python
import time
from streamable import stream_iterator

s = stream(range(10)).do(time.sleep, concurrency=4)
it: stream_iterator[int] = iter(s)
try:
    assert next(it) == 0
    assert next(it) == 1
    # 4 worker threads processing elements 2, 3, 4, 5
finally:
    it.close()
    # work done/cancelled and worker threads terminated
```

### `.aclose`

```python
import asyncio
from contextlib import aclosing

s = stream(range(10)).do(asyncio.sleep, concurrency=4)
async with aclosing(aiter(s)) as it:
    assert await anext(it) == 0
    assert await anext(it) == 1
    # 4 pending tasks, for elements 2, 3, 4, 5
# these 4 tasks are done/cancelled
```

equivalent to:

```python
import asyncio
from streamable import stream_aiterator

s = stream(range(10)).do(asyncio.sleep, concurrency=4)
it: stream_aiterator[int] = aiter(s)
try:
    assert await anext(it) == 0
    assert await anext(it) == 1
    # 4 pending tasks, for elements 2, 3, 4, 5
finally:
    await it.aclose()
    # these 4 tasks are done/cancelled
```

## e.g. ETL via [`dlt`](https://github.com/dlt-hub/dlt)

A `stream` is an expressive way to declare a `dlt.resource`:

```python
from datetime import timedelta
from http import HTTPStatus
from itertools import count
import dlt
import httpx
from httpx import Response, HTTPStatusError, TransportError
from dlt.destinations import filesystem
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_random_exponential
from streamable import stream

def not_found(e: HTTPStatusError) -> bool:
    return e.response.status_code == HTTPStatus.NOT_FOUND

retrying = retry(
    retry=retry_if_exception_type(TransportError),
    wait=wait_random_exponential(max=8),
    stop=stop_after_attempt(5),
    reraise=True,
)

@dlt.resource
def pokemons(http_client: httpx.Client, concurrency: int, per_second: int) -> stream[dict]:
    """Ingest Pokémons from the PokéAPI, retry network errors, stop on first 404."""
    return (
        stream(count(1))
        .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
        .throttle(per_second, per=timedelta(seconds=1))
        .map(retrying(http_client.get), concurrency=concurrency)
        .do(Response.raise_for_status)
        .catch(HTTPStatusError, where=not_found, stop=True)
        .map(Response.json)
        .observe("pokemons")
    )

# Write to a partitioned Delta Lake table, chunk by chunk on-the-fly.
with httpx.Client() as http_client:
    dlt.pipeline(
        pipeline_name="ingest_pokeapi",
        destination=filesystem("deltalake"),
        dataset_name="pokeapi",
    ).run(
        pokemons(http_client, concurrency=8, per_second=32),
        table_format="delta",
        write_disposition='merge',
        columns={
            "id": {"primary_key": True},
            "color__name": {"partition": True},
        },
    )
```

# ⭐ links

- [Top 10 Python libraries of 2024, from Tryolabs](https://tryolabs.com/blog/top-python-libraries-2024#top-10---general-use) ([LinkedIn](https://www.linkedin.com/posts/tryolabs_top-python-libraries-2024-activity-7273052840984539137-bcGs?utm_source=share&utm_medium=member_desktop), [Reddit](https://www.reddit.com/r/Python/comments/1hbs4t8/the_handpicked_selection_of_the_best_python/))
- [PyCoder’s weekly](https://pycoders.com/issues/651) x [Real Python](https://realpython.com/)
- [@PythonHub's tweet](https://x.com/PythonHub/status/1842886311369142713)
- [Reddit v1.0.0 showcase](https://www.reddit.com/r/Python/comments/1fp38jd/streamable_streamlike_manipulation_of_iterables/)
