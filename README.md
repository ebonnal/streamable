# ༄ `streamable`

> ***fluent concurrent sync/async streams***

`stream[T]` enriches any `Iterable[T]` or `AsyncIterable[T]` with a small set of chainable lazy operations for elegant data manipulation, including thread/coroutine concurrency, batching, rate limiting, and error handling.

A `stream[T]` is both an `Iterable[T]` and an `AsyncIterable[T]`: a convenient bridge between the sync and async worlds.

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

Create a `stream[T]` decorating an `Iterable[T]` or `AsyncIterable[T]` source:

```python
ints: stream[int] = stream(range(10))
```

# 4. operate

Chain ***lazy*** operations (only evaluated during iteration), each returning a new `stream`:

```python
import httpx

pokemons: stream[str] = (
    ints
    .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
    .map(httpx.Client().get, concurrency=2)
    .do(httpx.Response.raise_for_status)
    .map(lambda poke: poke.json()["name"])
    .catch(httpx.HTTPStatusError)
)
```


***... or the `async` way!***

All ***operations also accept async functions***: you can pass `httpx.AsyncClient().get` to `.map` and the concurrency will happen via the event loop instead of threads.

[visit the documentation](https://streamable.readthedocs.io/en/latest/api.html)

# 5. iterate

A `stream` is ***both*** `Iterable` and `AsyncIterable`:

```python
>>> list(pokemons)
['bulbasaur', 'ivysaur', 'venusaur', 'charmander', 'charmeleon', 'charizard', 'squirtle', 'wartortle', 'blastoise']
>>> [poke for poke in pokemons]
['bulbasaur', 'ivysaur', 'venusaur', 'charmander', 'charmeleon', 'charizard', 'squirtle', 'wartortle', 'blastoise']
>>> [poke async for poke in pokemons]
['bulbasaur', 'ivysaur', 'venusaur', 'charmander', 'charmeleon', 'charizard', 'squirtle', 'wartortle', 'blastoise']
```

Elements are processed ***on-the-fly*** as the iteration advances.


# 📒 Operations ([API Reference](https://streamable.readthedocs.io/en/latest/api.html))


  - [`.map`](#-map) elements
  - [`.do`](#-do) a side effect
  - [`.group`](#-group) elements
  - [`.flatten`](#-flatten) iterable elements
  - [`.filter`](#-filter) elements
  - [`.take`](#-take) first elements until ...
  - [`.skip`](#-skip) elements until ...
  - [`.catch`](#-catch) exceptions
  - [`.throttle`](#-throttle) the rate of iteration
  - [`.observe`](#-observe) the iteration progress

All the ***operations that take a function accept both sync and async functions***, you can freely mix them within the same `stream`. It can then be consumed either as an `Iterable` or as an `AsyncIterable`. When a stream involving async functions is consumed as an `Iterable`, a temporary `asyncio` event loop is attached to it.

A `stream` exposes a minimalist yet expressive set of operations to manipulate its elements, but creating its source or consuming it is not its responsibility, it's meant to be combined with standard and specialized libraries (`csv`, `json`, `pyarrow`, `psycopg2`, `boto3`, `requests`, `httpx`, ...).

## ▼ `.map`

Applies a transformation on elements:

```python
str_ints: stream[str] = ints.map(str)

assert list(str_ints) == ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
```


### concurrency

Set the `concurrency` parameter to apply the transformation concurrently.

> ***Memory-efficient***: Only `concurrency` upstream elements are pulled for processing; the next upstream element is pulled only when a result is yielded downstream.

> ***Ordering***: it yields results in the upstream order (FIFO), set `ordered=False` to yield results as they become available (*First Done, First Out*).

#### via threads

If you set a `concurrency > 1`, then the transformation will be applied via `concurrency` threads.


```python
import httpx

pokemons: stream[str] = (
    stream(range(1, 4))
    .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
    .map(httpx.Client().get, concurrency=3)
    .map(lambda poke: poke.json()["name"])
)
assert list(pokemons) == ['bulbasaur', 'ivysaur', 'venusaur']
```

#### via `async` coroutines

If you set a `concurrency > 1` and you provided an coroutine function, elements will be transformed concurrently via the event loop.


```python
import httpx

pokemons: stream[str] = (
    stream(range(1, 4))
    .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
    .map(httpx.AsyncClient().get, concurrency=3)
    .map(lambda poke: poke.json()["name"])
)
# within async context: consume as AsyncIterable
assert [name async for name in pokemons] == ['bulbasaur', 'ivysaur', 'venusaur']
# within sync context: consume as Iterable (concurrency will happen in dedicated event loop)
assert [name for name in pokemons] == ['bulbasaur', 'ivysaur', 'venusaur']
```

#### via processes

It is also possible to pass any `concurrent.futures.Executor` as `concurrency`, so you can pass a `ProcessPoolExecutor` to transform your elements via processes:


```python
if __name__ == "__main__":
    with ProcessPoolExecutor(10) as processes:
        state: list[int] = []
        # ints are mapped
        assert list(ints.map(state.append, concurrency=processes)) == 10 * [None]
        # but the `state` of the main process is not mutated
        assert state == []
```

## ▼ `.do`

Applies a side effect on elements:


```python
state: list[int] = []
ints_into_state: stream[int] = ints.do(state.append)

assert list(ints_into_state) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
assert state == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```

### concurrency

Same as `.map`.

## ▼ `.group`

Groups elements into batches...

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

You can mix the `up_to`/`every`/`by` parameters.

## ▼ `.flatten`

Ungroups elements assuming that they are `Iterable` or `AsyncIterable`:

```python
flattened_grouped_ints: stream[int] = ints.group(2).flatten()

assert list(flattened_grouped_ints) == list(ints)
```

### concurrency

Flattens `concurrency` iterables concurrently (via threads for `Iterable` elements and via coroutines for `AsyncIterable` elements):


```python
round_robined_ints: stream[int] = (
    stream([[0, 0], [1, 1, 1, 1], [2, 2]])
    .flatten(concurrency=2)
)
assert list(round_robined_ints) == [0, 1, 0, 1, 1, 2, 1, 2]
```

## ▼ `.filter`

Keeps only the elements that satisfy a condition:

```python
even_ints: stream[int] = ints.filter(lambda n: n % 2 == 0)

assert list(even_ints) == [0, 2, 4, 6, 8]
```

## ▼ `.take`

Takes the first specified number of elements:

```python
five_first_ints: stream[int] = ints.take(5)

assert list(five_first_ints) == [0, 1, 2, 3, 4]
```

... or takes elements `until` a condition become satisfied:


```python
five_first_ints: stream[int] = ints.take(until=lambda n: n == 5)

assert list(five_first_ints) == [0, 1, 2, 3, 4]
```

## ▼ `.skip`

Skips the first specified number of elements:

```python
ints_after_five: stream[int] = ints.skip(5)

assert list(ints_after_five) == [5, 6, 7, 8, 9]
```

... or skips elements `until` a predicate is satisfied:


```python
ints_after_five: stream[int] = ints.skip(until=lambda n: n >= 5)

assert list(ints_after_five) == [5, 6, 7, 8, 9]
```

## ▼ `.catch`

Catches a given type of exception, and optionally `replace` it:

```python
inverses: stream[float] = (
    ints
    .map(lambda n: round(1 / n, 2))
    .catch(ZeroDivisionError, replace=lambda exc: float("inf"))
)

assert list(inverses) == [float("inf"), 1.0, 0.5, 0.33, 0.25, 0.2, 0.17, 0.14, 0.12, 0.11]
```

Only catch the exception if it satisfies a `where` condition:

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

`do` a side effect on catch:


```python
errors: List[Exception] = []
inverses: stream[float] = (
    ints
    .map(lambda n: round(1 / n, 2))
    .catch(ZeroDivisionError, do=errors.append)
)
assert list(inverses) == [1.0, 0.5, 0.33, 0.25, 0.2, 0.17, 0.14, 0.12, 0.11]
assert len(errors) == 1
```

You can mix these parameters.

Set `stop=True` to stop the iteration if an exception is caught.

## ▼ `.throttle`

Limits the number of emissions `per` time interval:

```python
from datetime import timedelta

three_ints_per_second: stream[int] = ints.throttle(3, per=timedelta(seconds=1))

# collects the 10 ints in 3 seconds
assert list(three_ints_per_second) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```


## ▼ `.observe`

Logs the progress of iteration:

```python
observed_ints: stream[int] = (
    ints
    .throttle(2, per=timedelta(seconds=1))
    .observe("ints")
)
assert list(observed_ints) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```

logs:

```
{"@timestamp":...,"log.level":"INFO","elapsed":"0:00:00.000012","label":"ints","errors":0,"emissions":1}
{"@timestamp":...,"log.level":"INFO","elapsed":"0:00:00.000063","label":"ints","errors":0,"emissions":2}
{"@timestamp":...,"log.level":"INFO","elapsed":"0:00:01.005173","label":"ints","errors":0,"emissions":4}
{"@timestamp":...,"log.level":"INFO","elapsed":"0:00:03.002394","label":"ints","errors":0,"emissions":8}
{"@timestamp":...,"log.level":"INFO","elapsed":"0:00:04.005194","label":"ints","errors":0,"emissions":10}
```

It follows [ECS format](https://www.elastic.co/docs/reference/ecs) but you can specify your own via the `format` parameter.

A new log is emitted when the number of yielded elements (or errors) ***reaches powers of 2***.

To emit a log ***every `n` elements***, set `every=n`.

To emit a log ***every `n` seconds***, set `every=timedelta(seconds=n)`.

Logs are emitted by `logging.getLogger("streamable")`.


## ▼`+` (concat)

Concatenates streams:

```python
assert list(ints + ints) == [0, 1, 2, 3 ,4, 5, 6, 7, 8, 9, 0, 1, 2, 3 ,4, 5, 6, 7, 8, 9]
```

## ▼ `__call__` / `await`

*Calling* the stream iterates over it until exhaustion ***without collecting its elements***, and returns it:

```python
state: list[int] = []
pipeline: stream[int] = ints.do(state.append)

pipeline()

assert state == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```

*Awaiting* the stream iterates over it as an `AsyncIterable` until exhaustion ***without collecting its elements***, and returns it:

```python
state: list[int] = []
pipeline: stream[int] = ints.do(state.append)

await pipeline

assert state == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```

## ▼ `.pipe`

Calls a function, passing the stream as first argument, followed by `*args/**kwargs` if any:

```python
import polars as pl

pokemons.pipe(pl.DataFrame, schema=["name"]).write_csv("pokemons.csv")
```

# ••• other notes

## function as source

A `stream` ***can also be instantiated from a function*** (sync or async), it will be called sequentially to get the next source element.

e.g. stream from a `Queue` source:

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

All the operations process elements on-the-fly. At any point in time, ***concurrent operations only keep `concurrency` elements in memory for processing***.

There is ***zero overhead during iteration compared to builtins***:

```python
odd_int_strings = stream(range(1_000_000)).filter(lambda n: n % 2).map(str)
```

`iter(odd_int_strings)` visits the operations lineage and returns exactly this iterator:

```python
map(str, filter(lambda n: n % 2, range(1_000_000)))
```

Operations have been [implemented](https://github.com/ebonnal/streamable/blob/main/streamable/iterators.py) with speed in mind. If you have any ideas for improvement, whether performance-related or not, an issue, PR, or discussion would be very much appreciated! ([CONTRIBUTING.md](CONTRIBUTING.md))


## ⭐ highlights from the community
- [Tryolabs' Top 10 Python libraries of 2024](https://tryolabs.com/blog/top-python-libraries-2024#top-10---general-use) ([LinkedIn](https://www.linkedin.com/posts/tryolabs_top-python-libraries-2024-activity-7273052840984539137-bcGs?utm_source=share&utm_medium=member_desktop), [Reddit](https://www.reddit.com/r/Python/comments/1hbs4t8/the_handpicked_selection_of_the_best_python/))
- [PyCoder’s Weekly](https://pycoders.com/issues/651) x [Real Python](https://realpython.com/)
- [@PythonHub's tweet](https://x.com/PythonHub/status/1842886311369142713)
- [Upvoters on our showcase Reddit post](https://www.reddit.com/r/Python/comments/1fp38jd/streamable_streamlike_manipulation_of_iterables/)
