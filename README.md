# ༄ `streamable`

> ***fluent concurrent sync/async streams***

`stream[T]` wraps any `Iterable[T]` or `AsyncIterable[T]` with a lazy fluent interface covering concurrency, batching, buffering, rate limiting, progress logging, and error handling.

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/release/python-360/)
[![coverage](https://codecov.io/gh/ebonnal/streamable/graph/badge.svg?token=S62T0JQK9N)](https://codecov.io/gh/ebonnal/streamable)
[![PyPI](https://github.com/ebonnal/streamable/actions/workflows/pypi.yml/badge.svg?branch=main)](https://pypi.org/project/streamable)
[![Anaconda-Server Badge](https://anaconda.org/conda-forge/streamable/badges/version.svg?)](https://anaconda.org/conda-forge/streamable)
[![readthedocs](https://app.readthedocs.org/projects/streamable/badge/?version=latest&style=social)](https://streamable.readthedocs.io/en/latest/api.html)


# 1. install

```
pip install streamable
```
no dependencies

# 2. import

```python
from streamable import stream
```

# 3. init

Create a `stream[T]` from an `Iterable[T]` or `AsyncIterable[T]`:

```python
ints: stream[int] = stream(range(10))
```

# 4. operate

Chain lazy operations, accepting both sync and async functions:

```python
import logging
from datetime import timedelta
from httpx import AsyncClient, Response, HTTPStatusError

pokemons: stream[str] = (
    stream(range(10))
    .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
    .throttle(5, per=timedelta(seconds=1))
    .map(AsyncClient().get, concurrency=2)
    .do(Response.raise_for_status)
    .catch(HTTPStatusError, do=logging.error)
    .map(lambda poke: poke.json()["name"])
)
```

Source elements will be processed on-the-fly during iteration.

# 5. iterate

A `stream[T]` is `Iterable[T]`:

```python
>>> list(pokemons)
['bulbasaur', 'ivysaur', 'venusaur', 'charmander', 'charmeleon', 'charizard', 'squirtle', 'wartortle', 'blastoise']

>>> [poke for poke in pokemons]
['bulbasaur', 'ivysaur', 'venusaur', 'charmander', 'charmeleon', 'charizard', 'squirtle', 'wartortle', 'blastoise']
```

... and `AsyncIterable[T]`:
```python
>>> [poke async for poke in pokemons]
['bulbasaur', 'ivysaur', 'venusaur', 'charmander', 'charmeleon', 'charizard', 'squirtle', 'wartortle', 'blastoise']
```


# 📒 Operations

> visit the [docs](https://streamable.readthedocs.io/en/latest/api.html) for more details

  - [`.map`](#-map) elements
  - [`.do`](#-do) side effects on elements
  - [`.group`](#-group) elements into batches
  - [`.flatten`](#-flatten) iterable elements
  - [`.filter`](#-filter) elements
  - [`.take`](#-take) elements until ...
  - [`.skip`](#-skip) elements until ...
  - [`.catch`](#-catch) exceptions
  - [`.throttle`](#-throttle) the rate of iteration
  - [`.buffer`](#-buffer) elements
  - [`.observe`](#-observe) the iteration progress

A `stream` exposes operations to manipulate its elements, but the I/O is not its responsibility. It's meant to be combined with dedicated libraries like `csv`, `json`, `pyarrow`, `psycopg2`, `boto3`, `aiohttp`, `httpx`, `polars`.

Both sync and async functions are accepted by operations, you can freely mix them within the same `stream` and it can then be consumed as an `Iterable` or `AsyncIterable`. When a stream involving async functions is consumed as an `Iterable`, a temporary `asyncio` event loop is attached to it.


## ▼ `.map`

Transform elements:

```python
str_ints: stream[str] = ints.map(str)

assert list(str_ints) == ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
```


### concurrency

Set the `concurrency` parameter to apply the transformation concurrently.

Only `concurrency` upstream elements are pulled for processing; the next upstream element is pulled only when a result is yielded downstream.

It preserves the upstream order by default, but you can set `as_completed=True` to yield results as they become available.

#### Via threads

If `concurrency > 1`, the transformations will be applied via `concurrency` threads:


```python
import httpx

pokemons: stream[str] = (
    stream(range(1, 4))
    .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
    .map(httpx.Client().get, concurrency=2)
    .map(lambda poke: poke.json()["name"])
)
assert list(pokemons) == ['bulbasaur', 'ivysaur', 'venusaur']
```

#### Via `async` coroutines

If `concurrency > 1` and the function is async, the transformations will be applied via `concurrency` async tasks:


```python
import httpx

pokemons: stream[str] = (
    stream(range(1, 4))
    .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
    .map(httpx.AsyncClient().get, concurrency=2)
    .map(lambda poke: poke.json()["name"])
)

assert [name async for name in pokemons] == ['bulbasaur', 'ivysaur', 'venusaur']
assert list(pokemons) == ['bulbasaur', 'ivysaur', 'venusaur']
```

#### Via processes

`concurrency` can also be a `concurrent.futures.Executor`, pass a `ProcessPoolExecutor` to apply the transformations via processes:


```python
if __name__ == "__main__":
    with ProcessPoolExecutor(max_workers=10) as processes:
        state: list[int] = []
        # ints are mapped
        assert list(ints.map(state.append, concurrency=processes)) == 10 * [None]
        # but the `state` of the main process is not mutated
        assert state == []
```

## ▼ `.do`

Apply a side effect:


```python
state: list[int] = []
ints_into_state: stream[int] = ints.do(state.append)

assert list(ints_into_state) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
assert state == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```

### concurrency

Same as `.map`.

## ▼ `.group`

Group elements into batches...

... `up_to` a given size:

```python
ints_by_5: stream[list[int]] = ints.group(5)

assert list(ints_by_5) == [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]]
```

... `every` given time interval:


```python
from datetime import timedelta

ints_within_1_sec: stream[list[int]] = (
    ints
    .throttle(2, per=timedelta(seconds=1))
    .group(every=timedelta(seconds=0.99))
)

assert list(ints_within_1_sec) == [[0, 1, 2], [3, 4], [5, 6], [7, 8], [9]]
```

... `by` a given key, yielding `(key, group)` pairs:


```python
ints_by_parity: stream[tuple[str, list[int]]] = (
    ints
    .group(by=lambda n: "odd" if n % 2 else "even")
)

assert list(ints_by_parity) == [("even", [0, 2, 4, 6, 8]), ("odd", [1, 3, 5, 7, 9])]
```

You can mix these parameters.

## ▼ `.flatten`

Explode upstream `Iterable` elements (or `AsyncIterable`):

```python
chars: stream[str] = stream(["hel", "lo!"]).flatten()

assert list(chars) == ["h", "e", "l", "l", "o", "!"]
```

### concurrency

Flattens `concurrency` iterables concurrently (via threads for `Iterable` elements and via coroutines for `AsyncIterable` elements):


```python
chars: stream[str] = stream(["hel", "lo", "!"]).flatten(concurrency=2)

assert list(chars) == ["h", "l", "e", "o", "l", "!"]
```

## ▼ `.filter`

Filter elements satisfying a predicate:

```python
even_ints: stream[int] = ints.filter(lambda n: n % 2 == 0)

assert list(even_ints) == [0, 2, 4, 6, 8]
```

## ▼ `.take`

Take a certain number of elements:

```python
five_first_ints: stream[int] = ints.take(5)

assert list(five_first_ints) == [0, 1, 2, 3, 4]
```

... or `until` a predicate is satisfied:


```python
five_first_ints: stream[int] = ints.take(until=lambda n: n == 5)

assert list(five_first_ints) == [0, 1, 2, 3, 4]
```

## ▼ `.skip`

Skip a certain number of elements:

```python
ints_after_five: stream[int] = ints.skip(5)

assert list(ints_after_five) == [5, 6, 7, 8, 9]
```

... or `until` a predicate is satisfied:


```python
ints_after_five: stream[int] = ints.skip(until=lambda n: n >= 5)

assert list(ints_after_five) == [5, 6, 7, 8, 9]
```

## ▼ `.catch`

Catch exceptions of a given type:

```python
inverses: stream[float] = ints.map(lambda n: round(1 / n, 2)).catch(ZeroDivisionError)

assert list(inverses) == [1.0, 0.5, 0.33, 0.25, 0.2, 0.17, 0.14, 0.12, 0.11]
```

... `where` a predicate is satisfied:

```python
import httpx

status_codes_ignoring_resolution_errors: stream[int] = (
    stream(["https://github.com", "https://foo.bar", "https://github.com/foo/bar"])
    .map(httpx.get, concurrency=2)
    .catch(httpx.ConnectError, where=lambda exc: "not known" in str(exc))
    .map(lambda response: response.status_code)
)

assert list(status_codes_ignoring_resolution_errors) == [200, 404]
```

... `do` a side effect on catch:

```python
errors: list[Exception] = []
inverses: stream[float] = (
    ints
    .map(lambda n: round(1 / n, 2))
    .catch(ZeroDivisionError, do=errors.append)
)
assert list(inverses) == [1.0, 0.5, 0.33, 0.25, 0.2, 0.17, 0.14, 0.12, 0.11]
assert len(errors) == 1
```

... `replace` with a value:

```python
inverses: stream[float] = (
    ints
    .map(lambda n: round(1 / n, 2))
    .catch(ZeroDivisionError, replace=lambda exc: float("inf"))
)

assert list(inverses) == [float("inf"), 1.0, 0.5, 0.33, 0.25, 0.2, 0.17, 0.14, 0.12, 0.11]
```

... `stop=True` to stop the iteration if an exception is caught.

```python
inverses: stream[float] = (
    ints
    .map(lambda n: round(1 / n, 2))
    .catch(ZeroDivisionError, stop=True)
)

assert list(inverses) == []
```

You can mix these parameters.

## ▼ `.throttle`

Limit the number of emissions `per` time interval:

```python
from datetime import timedelta

three_ints_per_second: stream[int] = ints.throttle(3, per=timedelta(seconds=1))

# collects the 10 ints in 3 seconds
assert list(three_ints_per_second) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```

## ▼ `.buffer`

Buffer upstream elements via a background task to decouple upstream and downstream rates:

```python
pulled: list[int] = []
buffered_ints = ints.do(pulled.append).buffer(2)
assert next(iter(buffered_ints)) == 0
assert pulled == [0, 1, 2]
```

## ▼ `.observe`

Log the progress of iteration:

```python
observed_ints: stream[int] = ints.observe("ints")
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

By default, logs are produced when the counts reach powers of 2, but they can instead be produced periodically `every` fixed *n* elements or time interval:
```python
# observe every 1k elements (or errors)
observed_ints = ints.observe("ints", every=1000)
# observe every 5 seconds
observed_ints = ints.observe("ints", every=timedelta(seconds=5))
```

By default, observations are logged via `logging.getLogger("streamable").info`, but they can instead be passed to a function taking a `stream.Observation`:

```python
observed_ints = ints.observe("ints", do=other_logger.info)
observed_ints = ints.observe("ints", do=logs.append)
observed_ints = ints.observe("ints", do=print)
```


## ▼ `+`

Concatenate streams:

```python
assert list(ints + ints) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```

## ▼ `.cast`

Cast elements:

```python
docs: stream[Any] = stream(['{"foo": "bar"}', '{"foo": "baz"}']).map(json.loads)
dicts: stream[dict[str, str]] = docs.cast(dict[str, str])
# the stream remains the same, it's for type checkers only
assert dicts is docs
```

## ▼ `.__call__`

Iterate over the stream as an `Iterable` until exhaustion (without collecting its elements):

```python
state: list[int] = []
pipeline: stream[int] = ints.do(state.append)

pipeline()

assert state == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```

## ▼ `await`

Iterate over the stream as an `AsyncIterable` until exhaustion (without collecting its elements):

```python
state: list[int] = []
pipeline: stream[int] = ints.do(state.append)

await pipeline

assert state == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```

## ▼ `.pipe`

Apply a callable, passing the stream as first argument, followed by the provided `*args` and `**kwargs`:

```python
import polars as pl

pokemons.pipe(pl.DataFrame, schema=["name"]).write_csv("pokemons.csv")
```

# ••• other notes

## function as source

A `stream` can also be instantiated from a (sync or async) function that will be called sequentially to get the next source element during iteration.

Stream from a `Queue`:

```python
queued_ints: queue.Queue[int] = ...
# or asyncio.Queue[int]
ints: stream[int] = stream(queued_ints.get)
```

## starmap

The `star` function decorator transforms a function (sync or async) that takes several positional arguments into a function that takes a tuple.

```python
from streamable import star

indexed_pokemons: stream[str] = (
    stream(enumerate(pokemons))
    .map(star(lambda index, poke: f"#{index + 1} {poke}"))
)
assert list(indexed_pokemons) == ['#1 bulbasaur', '#2 ivysaur', '#3 venusaur', '#4 charmander', '#5 charmeleon', '#6 charizard', '#7 squirtle', '#8 wartortle', '#9 blastoise']
```

## zip

Use the builtins' `zip` function:

```python
from streamable import star

cubes: stream[int] = (
    stream(zip(ints, ints, ints))  # stream[tuple[int, int, int]]
    .map(star(lambda a, b, c: a * b * c))  # stream[int]
)

assert list(cubes) == [0, 1, 8, 27, 64, 125, 216, 343, 512, 729]
```

##  distinct

To collect distinct elements you can `set(a_stream)`.

To deduplicates in the middle of the stream, `.filter` new values and `.do` add them into a `set` (or a fancier external cache):

```python
seen: set[str] = set()

unique_ints: stream[int] = (
    stream("001000111")
    .filter(lambda char: char not in seen)
    .do(seen.add)
    .map(int)
)

assert list(unique_ints) == [0, 1]
```

## iteration can be resumed after an exception

If at one point during the iteration an exception is raised and caught, the iteration can resume from there.

## performances

All the operations process elements on-the-fly. At any point in time, concurrent operations only keep `concurrency` elements in memory for processing.

There is zero overhead during iteration compared to builtins:

```python
odd_int_strings = stream(range(1_000_000)).filter(lambda n: n % 2).map(str)
```

`iter(odd_int_strings)` visits the operations lineage and returns exactly this iterator:

```python
map(str, filter(lambda n: n % 2, range(1_000_000)))
```

Operations have been [implemented](https://github.com/ebonnal/streamable/blob/main/streamable/_iterators.py) with speed in mind, issues and PRs are very welcome!


## ⭐ highlights from the community
- [Tryolabs' Top 10 Python libraries of 2024](https://tryolabs.com/blog/top-python-libraries-2024#top-10---general-use) ([LinkedIn](https://www.linkedin.com/posts/tryolabs_top-python-libraries-2024-activity-7273052840984539137-bcGs?utm_source=share&utm_medium=member_desktop), [Reddit](https://www.reddit.com/r/Python/comments/1hbs4t8/the_handpicked_selection_of_the_best_python/))
- [PyCoder’s Weekly](https://pycoders.com/issues/651) x [Real Python](https://realpython.com/)
- [@PythonHub's tweet](https://x.com/PythonHub/status/1842886311369142713)
- [Upvoters on our showcase Reddit post](https://www.reddit.com/r/Python/comments/1fp38jd/streamable_streamlike_manipulation_of_iterables/)
