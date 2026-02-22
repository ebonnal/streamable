# ༄ `streamable`

> sync/async iterable streams for Python

`stream[T]` wraps any `Iterable[T]` or `AsyncIterable[T]` with a lazy fluent interface covering concurrency, batching, buffering, rate limiting, progress logging, and error handling.

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/release/python-3820/)
[![PyPI version](https://img.shields.io/pypi/v/streamable.svg)](https://pypi.org/project/streamable/)
[![Anaconda-Server Badge](https://anaconda.org/conda-forge/streamable/badges/version.svg?)](https://anaconda.org/conda-forge/streamable)
[![coverage](https://codecov.io/gh/ebonnal/streamable/graph/badge.svg?token=S62T0JQK9N)](https://codecov.io/gh/ebonnal/streamable)
[![readthedocs](https://app.readthedocs.org/projects/streamable/badge/?version=latest&style=social)](https://streamable.readthedocs.io/en/latest/api.html)


# 1. install

```
pip install streamable
```

# 2. import

```python
from streamable import stream
```

# 3. init

Create a `stream[T]` from an `Iterable[T]` (or `AsyncIterable[T]`):

```python
ints: stream[int] = stream(range(10))
```

# 4. operate

Chain lazy operations:

```python
import logging
from datetime import timedelta
import httpx
from httpx import Response, HTTPStatusError
from streamable import stream

pokemons: stream[str] = (
    stream(range(10))
    .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
    .throttle(5, per=timedelta(seconds=1))
    .map(httpx.get, concurrency=2)
    .do(Response.raise_for_status)
    .catch(HTTPStatusError, do=logging.warning)
    .map(lambda poke: poke.json()["name"])
)
```

Source elements will be processed on-the-fly during iteration.

Operations accept both sync and async functions.

# 5. iterate

A `stream[T]` is `Iterable[T]` (and `AsyncIterable[T]`):

```python
>>> list(pokemons)
['bulbasaur', 'ivysaur', 'venusaur', 'charmander', 'charmeleon', 'charizard', 'squirtle', 'wartortle', 'blastoise']
```

# 📒 Operations ([docs](https://streamable.readthedocs.io/en/latest/api.html))

- function application
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

Operations accept both sync and async functions, they can be mixed within the same `stream`, that can then be consumed as an `Iterable` or `AsyncIterable`. Async functions run in the current loop, one is created if needed.

Operations are implemented so that the iteration can resume after an exception.

A `stream` can be iterated several times if its source allows it.

A `stream` exposes operations to manipulate its elements, but the I/O is not its responsibility. It's meant to be combined with dedicated libraries like `pyarrow`, `psycopg2`, `boto3`, `dlt` ([ETL example](#eg-etl-via-dlt)) ...

## ▼ `.map`

Transform elements:

```python
int_chars: stream[str] = stream(range(10)).map(str)

assert list(int_chars) == ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
```


### `concurrency`

Set the `concurrency` param to apply the transformation concurrently.

Only `concurrency` upstream elements are in-flight for processing.

Preserve upstream order unless you set `as_completed=True`.


#### via threads

If `concurrency > 1`, the transformation will be applied via `concurrency` threads:

```python
pokemons: stream[str] = (
    stream(range(1, 4))
    .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
    .map(httpx.get, concurrency=2)
    .map(lambda poke: poke.json()["name"])
)
assert list(pokemons) == ['bulbasaur', 'ivysaur', 'venusaur']
```

#### via `async`

If `concurrency > 1` and the transformation is async, it will be applied via `concurrency` async tasks:

```python
# async context
async with httpx.AsyncClient() as http_client:
    pokemons: stream[str] = (
        stream(range(1, 4))
        .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
        .map(http_client.get, concurrency=2)
        .map(lambda poke: poke.json()["name"])
    )
    assert [name async for name in pokemons] == ['bulbasaur', 'ivysaur', 'venusaur']
```

```python
# sync context
with asyncio.Runner() as runner:
    http_client = httpx.AsyncClient()
    pokemons: stream[str] = (
        stream(range(1, 4))
        .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
        .map(http_client.get, concurrency=2)
        .map(lambda poke: poke.json()["name"])
    )
    # uses runner's loop
    assert list(pokemons) == ['bulbasaur', 'ivysaur', 'venusaur']
    runner.run(http_client.aclose())
```

#### via processes

`concurrency` can also be a `concurrent.futures.Executor`, pass a `ProcessPoolExecutor` to apply the transformations via processes:


```python
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

Same as `.map`.

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

Explode `concurrency` iterables at a time:

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

urls = [
    "https://github.com/ebonnal",
    "https://github.com/ebonnal/streamable",
    "https://github.com/ebonnal/foo",
]
responses: stream[httpx.Response] = (
    stream(urls)
    .map(httpx.get)
    .do(httpx.Response.raise_for_status)
    .catch(
        httpx.HTTPStatusError,
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

Buffer upstream elements into a bounded queue via a background task (decoupling upstream production rate from downstream consumption rate):

```python
pulled: list[int] = []
buffered_ints = iter(
    stream(range(10))
    .do(pulled.append)
    .buffer(5)
)
assert next(buffered_ints) == 0
time.sleep(1e-3)
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

Logs are produced when the counts reach powers of 2. Set `every` to produce them periodically:
```python
# observe every 1k elements (or errors)
observed_ints = stream(range(10)).observe("ints", every=1000)
# observe every 5 seconds
observed_ints = stream(range(10)).observe("ints", every=timedelta(seconds=5))
```

Observations are logged via `logging.getLogger("streamable").info`. Set `do` to do something else with the `streamable.Observation`:

```python
observed_ints = stream(range(10)).observe("ints", do=custom_logger.info)
observed_ints = stream(range(10)).observe("ints", do=observations.append)
observed_ints = stream(range(10)).observe("ints", do=print)
```


## ▼ `+`

Concatenate a stream with an iterable:
```python
concatenated_ints = stream(range(10)) + range(10)
assert list(concatenated_ints) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```

## ▼ `.cast`

Provide a type hint for elements:

```python
docs: stream[Any] = stream(['{"foo": "bar"}', '{"foo": "baz"}']).map(json.loads)
dicts: stream[dict[str, str]] = docs.cast(dict[str, str])
# the stream remains the same, it's for type checkers only
assert dicts is docs
```

## ▼ `.__call__`

Iterate as an `Iterable` until exhaustion, without collecting its elements:

```python
state: list[int] = []
pipeline: stream[int] = stream(range(10)).do(state.append)

pipeline()

assert state == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```

## ▼ `await`

Iterate as an `AsyncIterable` until exhaustion, without collecting its elements:

```python
state: list[int] = []
pipeline: stream[int] = stream(range(10)).do(state.append)

await pipeline

assert state == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```

## ▼ `.pipe`

Apply a callable, passing the stream as first argument, followed by the provided `*args` and `**kwargs`:

```python
import polars as pl

pokemons: stream[str] = ...
pokemons.pipe(pl.DataFrame, schema=["name"]).write_csv("pokemons.csv")
```

# ••• other notes

## function as source

A `stream` can also be instantiated from a function (sync or async) that will be called sequentially to get the next source element during iteration.

e.g. stream from a `Queue`:

```python
queued_ints: queue.Queue[int] = ...
# or asyncio.Queue[int]
ints: stream[int] = stream(queued_ints.get)
```

## starmap

The `star` function decorator transforms a function (sync or async) that takes several positional arguments into a function that takes a tuple.

```python
from streamable import star

pokemons: stream[str] = ...
enumerated_pokes: stream[str] = (
    stream(enumerate(pokemons))
    .map(star(lambda index, poke: f"#{index + 1} {poke}"))
)
assert list(enumerated_pokes) == ['#1 bulbasaur', '#2 ivysaur', '#3 venusaur', '#4 charmander', '#5 charmeleon', '#6 charizard', '#7 squirtle', '#8 wartortle', '#9 blastoise']
```

## distinct

To collect distinct elements you can `set(a_stream)`.

To deduplicates in the middle of the stream, `.filter` new values and `.do` add them into a `set` (or a fancier cache):

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

## vs `builtins.map/filter`

There is zero overhead during iteration compared to `builtins.map` and `builtins.filter`:

```python
odd_int_chars = stream(range(N)).filter(lambda n: n % 2).map(str)
```

`iter(odd_int_chars)` visits the operations lineage and returns exactly this iterator:

```python
map(str, filter(lambda n: n % 2, range(N)))
```

## e.g. ETL via [`dlt`](https://github.com/dlt-hub/dlt)

A `stream` is an expressive way to declare a `dlt.resource`:

```python
# from datetime import timedelta
# from http import HTTPStatus
# from itertools import count
# import dlt
# import httpx
# from httpx import Response, HTTPStatusError
# from dlt.destinations import filesystem
# from streamable import stream

def not_found(e: HTTPStatusError) -> bool:
    return e.response.status_code == HTTPStatus.NOT_FOUND

@dlt.resource
def pokemons(http_client: httpx.Client, concurrency: int, per_second: int) -> stream[dict]:
    """Ingest Pokémons from the PokéAPI, stop on first 404."""
    return (
        stream(count(1))
        .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
        .throttle(per_second, per=timedelta(seconds=1))
        .map(http_client.get, concurrency=concurrency, as_completed=True)
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
