# ༄ `streamable`

> *fluent concurrent sync/async streams*

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

Chain ***lazy*** operations (only evaluated during iteration), each returning a new ***immutable*** `stream`:

```python
from json import JSONDecodeError
import httpx

pokemons: stream[str] = (
    ints
    .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
    .map(httpx.Client().get, concurrency=2)
    .map(lambda poke: poke.json()["name"])
    .catch(JSONDecodeError)
)
```

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

# ↔ showcase: ETL

Let's walk through the `stream`'s features with an Extract-Transform-Load script:

This toy script gets Pokémons concurrently from [PokéAPI](https://pokeapi.co/), and writes the quadrupeds from the first three generations into a CSV file, in 5-seconds batches:

```python
import csv
from datetime import timedelta
from itertools import count
import httpx
from streamable import stream

with open("./quadruped_pokemons.csv", mode="w") as file:
    fields = ["id", "name", "is_legendary", "base_happiness", "capture_rate"]
    writer = csv.DictWriter(file, fields, extrasaction='ignore')
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
        # Stop the iteration when reaching the 1st pokemon of the 4th generation
        .truncate(when=lambda poke: poke["generation"]["name"] == "generation-iv")
        .observe("pokemons")
        # Keep only quadruped Pokemons
        .filter(lambda poke: poke["shape"]["name"] == "quadruped")
        # Write a batch of pokemons every 5 seconds to the CSV file
        .group(over=timedelta(seconds=5))
        .do(writer.writerows)
        .flatten()
        .observe("written pokemons")
        # Catch exceptions and raises the 1st one at the end of the iteration
        .catch(Exception, finally_raise=True)
    )

    # Call the stream to consume it (as an Iterable)
    # without collecting its elements
    pipeline()
```

## ... or the `async` way!

All operations also accept coroutine/async functions: you can pass `httpx.AsyncClient().get` to `.map` and the concurrency will happen via the event loop instead of threads.

If you are within an async context, you can replace `pipeline()` by `await pipeline` to consume the full stream as an `AsyncIterable` without collecting its elements.

If you are within a sync context, you can keep `pipeline()` and the `.map` async concurrency will happen within a dedicated event loop.

<details><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
import csv
from datetime import timedelta
from itertools import count
import httpx
from streamable import stream

with open("./quadruped_pokemons.csv", mode="w") as file:
    fields = ["id", "name", "is_legendary", "base_happiness", "capture_rate"]
    writer = csv.DictWriter(file, fields, extrasaction='ignore')
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
        # Stop the iteration when reaching the 1st pokemon of the 4th generation
        .truncate(when=lambda poke: poke["generation"]["name"] == "generation-iv")
        .observe("pokemons")
        # Keep only quadruped Pokemons
        .filter(lambda poke: poke["shape"]["name"] == "quadruped")
        # Write a batch of pokemons every 5 seconds to the CSV file
        .group(over=timedelta(seconds=5))
        .do(writer.writerows)
        .flatten()
        .observe("written pokemons")
        # Catch exceptions and raises the 1st one at the end of the iteration
        .catch(Exception, finally_raise=True)
    )

    # await the stream to consume it (as an AsyncIterable)
    # without collecting its elements
    await pipeline
```

</details>

# 📒 ***Operations***

> [!IMPORTANT]
> A `stream` exposes a minimalist yet expressive set of operations to manipulate its elements, but creating its source or consuming it is not its responsability, it's meant to be combined with standard and specialized libraries (`csv`, `json`, `pyarrow`, `psycopg2`, `boto3`, `requests`, `httpx`, ...).


Let's do a quick tour of the operations (check the [***docs***](https://streamable.readthedocs.io/en/latest/api.html) for more details).

|||
|--|--|
[`.map`](#-map)|transform elements|
[`.do`](#-do)|apply a side effect on elements|
[`.group`](#-group) / [`.groupby`](#-groupby)|batch elements up to a size, by a key, over a time interval|
[`.flatten`](#-flatten)|explode iterable elements|
[`.filter`](#-filter)|remove elements|
[`.truncate`](#-truncate)|cut the stream|
[`.skip`](#-skip)|ignore head elements|
[`.catch`](#-catch)|handle exceptions|
[`.throttle`](#-throttle)|rate-limit the iteration|
[`.observe`](#-observe)|logs iteration progress|


## sync/async compatibility

All the operations that take a function accept both sync and async functions, you can freely mix them within the same `stream`. It can then be consumed either as an `Iterable` or as an `AsyncIterable`. When a stream involving async functions is consumed as an `Iterable`, a temporary `asyncio` event loop is attached to it.

## 🟡 `.map`

Applies a transformation on elements:

<details><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
str_ints: stream[str] = ints.map(str)

assert list(str_ints) == ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
```
</details>

### concurrency

Set the `concurrency` parameter to apply the transformation concurrently.

> [!NOTE]
> **Memory-efficient**: Only `concurrency` upstream elements are pulled for processing; the next upstream element is pulled only when a result is yielded downstream.

> [!NOTE]
> **Ordering**: it yields results in the upstream order (FIFO), set `ordered=False` to yield results as they become available (*First Done, First Out*).

#### via threads

If you set a `concurrency > 1`, then the transformation will be applied via `concurrency` threads.

<details><summary style="text-indent: 40px;">👀 show snippet</summary></br>

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
</details>

#### via coroutines

If you set a `concurrency > 1` and you provided an coroutine function, elements will be transformed concurrently via the event loop.

<details><summary style="text-indent: 40px;">👀 show snippet</summary></br>

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
</details>

#### via processes

It is also possible to pass any `concurrent.futures.Executor` as `concurrency`, so you can pass a `ProcessPoolExecutor` to transform your elements via processes:

<details><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
if __name__ == "__main__":
    with ProcessPoolExecutor(max_workers=10) as processes:
        state: list[int] = []
        # ints are mapped
        assert list(ints.map(state.append, concurrency=processes)) == [None] * 10
        # but the `state` of the main process is not mutated
        assert state == []
```
</details>

## 🟡 `.do`

Applies a side effect on elements:

<details><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
state: list[int] = []
appending_ints: stream[int] = ints.do(state.append)

assert list(appending_ints) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
assert state == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```
</details>

### concurrency

Same as `.map`.

## 🟡 `.group`

Groups into `list`s ...

... `up_to` a given size:
<details><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
ints_by_5: stream[list[int]] = ints.group(5)

assert list(ints_by_5) == [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]]
```
</details>

... `by` a given key:

<details><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
ints_by_parity: stream[list[int]] = ints.group(by=lambda n: n % 2)

assert list(ints_by_parity) == [[0, 2, 4, 6, 8], [1, 3, 5, 7, 9]]
```
</details>

... `over` a given time interval:

<details><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
from datetime import timedelta

ints_within_1_sec: stream[list[int]] = (
    ints
    .throttle(2, per=timedelta(seconds=1))
    .group(over=timedelta(seconds=0.99))
)

assert list(ints_within_1_sec) == [[0, 1, 2], [3, 4], [5, 6], [7, 8], [9]]
```
</details>

Combine the `up_to`/`by`/`over` parameters:

<details><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
ints_by_parity_by_2: stream[list[int]] = (
    ints
    .group(by=lambda n: n % 2, up_to=2)
)

assert list(ints_by_parity_by_2) == [[0, 2], [1, 3], [4, 6], [5, 7], [8], [9]]
```
</details>

## 🟡 `.groupby`

Like `.group`, but groups into `(key, elements)` tuples:
<details><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
ints_by_parity: stream[tuple[str, list[int]]] = (
    ints
    .groupby(lambda n: "odd" if n % 2 else "even")
)

assert list(ints_by_parity) == [("even", [0, 2, 4, 6, 8]), ("odd", [1, 3, 5, 7, 9])]
```
</details>

## 🟡 `.flatten`

Ungroups elements assuming that they are `Iterable` or `AsyncIterable`:

<details><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
even_then_odd_ints: stream[int] = ints_by_parity.flatten()

assert list(even_then_odd_ints) == [0, 2, 4, 6, 8, 1, 3, 5, 7, 9]
```
</details>

### concurrency

Flattens `concurrency` iterables concurrently (via threads for `Iterable` elements and via coroutines for `AsyncIterable` elements):

<details><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
round_robined_ints: stream[int] = (
    stream([[0, 0], [1, 1, 1, 1], [2, 2]])
    .flatten(concurrency=2)
)
assert list(round_robined_ints) == [0, 1, 0, 1, 1, 2, 1, 2]
```
</details>

## 🟡 `.filter`

Keeps only the elements that satisfy a condition:

<details><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
even_ints: stream[int] = ints.filter(lambda n: n % 2 == 0)

assert list(even_ints) == [0, 2, 4, 6, 8]
```
</details>

## 🟡 `.truncate`

Ends iteration when a given number of elements have been yielded:

<details><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
five_first_ints: stream[int] = ints.truncate(5)

assert list(five_first_ints) == [0, 1, 2, 3, 4]
```
</details>

... or `when` a condition is satisfied:

<details><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
five_first_ints: stream[int] = ints.truncate(when=lambda n: n == 5)

assert list(five_first_ints) == [0, 1, 2, 3, 4]
```
</details>

If both `count` and `when` are set, truncation occurs as soon as either condition is met.

## 🟡 `.skip`

Skips the first specified number of elements:

<details><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
ints_after_five: stream[int] = ints.skip(5)

assert list(ints_after_five) == [5, 6, 7, 8, 9]
```
</details>

or skips elements `until` a predicate is satisfied:

<details><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
ints_after_five: stream[int] = ints.skip(until=lambda n: n >= 5)

assert list(ints_after_five) == [5, 6, 7, 8, 9]
```
</details>

If both `count` and `until` are set, skipping stops as soon as either condition is met.

## 🟡 `.catch`

Catches a given type of exception, and optionally `replace` it:

<details><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
inverses: stream[float] = (
    ints
    .map(lambda n: round(1 / n, 2))
    .catch(ZeroDivisionError, replace=lambda e: float("inf"))
)

assert list(inverses) == [float("inf"), 1.0, 0.5, 0.33, 0.25, 0.2, 0.17, 0.14, 0.12, 0.11]
```
</details>

You can specify an additional `when` condition for the catch:
<details><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
import httpx

status_codes_ignoring_resolution_errors: stream[int] = (
    stream(["https://github.com", "https://foo.bar", "https://github.com/foo/bar"])
    .map(httpx.get, concurrency=2)
    .catch(httpx.ConnectError, when=lambda exception: "not known" in str(exception))
    .map(lambda response: response.status_code)
)

assert list(status_codes_ignoring_resolution_errors) == [200, 404]
```
</details>

You can `do` a side effect on catch:

<details><summary style="text-indent: 40px;">👀 show snippet</summary></br>

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
</details>

Set `finally_raise=True` parameter to raise the first exception caught (if any) when the iteration stops.

Set `terminate=True` to stop the iteration if an exception is caught.

## 🟡 `.throttle`

Limits the number of yields `per` time interval:

<details><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
from datetime import timedelta

three_ints_per_second: stream[int] = ints.throttle(3, per=timedelta(seconds=1))

# takes 3s: ceil(10 ints / 3 per_second) - 1
assert list(three_ints_per_second) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```
</details>


## 🟡 `.observe`

Logs the progress of iterations:
<details><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
>>> assert list(ints.throttle(2, per=timedelta(seconds=1)).observe("ints")) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```

```
INFO: [duration=0:00:00.001793 errors=0] 1 ints yielded
INFO: [duration=0:00:00.004388 errors=0] 2 ints yielded
INFO: [duration=0:00:01.003655 errors=0] 4 ints yielded
INFO: [duration=0:00:03.003196 errors=0] 8 ints yielded
INFO: [duration=0:00:04.003852 errors=0] 10 ints yielded
```
</details>

A new log is emitted when the number of yielded elements (or errors) reaches powers of 2.

To emit a log every *n* elements, set `every=n`.

To emit a log every *n* seconds, set `every=timedelta(seconds=n)`.

Logs are emitted by `logging.getLogger("streamable")`.


## 🟡`+` (concat)

Concatenates streams:

<details><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
assert list(ints + ints) == [0, 1, 2, 3 ,4, 5, 6, 7, 8, 9, 0, 1, 2, 3 ,4, 5, 6, 7, 8, 9]
```
</details>

## 🟡 `__call__` / `await`

*Calling* the stream iterates over it until exhaustion without collecting its elements, and returns it:
<details><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
state: list[int] = []
appending_ints: stream[int] = ints.do(state.append)
assert appending_ints() is appending_ints
assert state == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```
</details>

*Awaiting* the stream iterates over it as an `AsyncIterable` until exhaustion without collecting its elements, and returns it:

<details><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
state: list[int] = []
appending_ints: stream[int] = ints.do(state.append)
assert appending_ints is await appending_ints
assert state == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```
</details>

## 🟡 `.pipe`

Calls a function, passing the stream as first argument, followed by `*args/**kwargs` if any:

<details><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
import pandas as pd

(
    ints
    .observe("ints")
    .pipe(pd.DataFrame, columns=["integer"])
    .to_csv("ints.csv", index=False)
)
```
</details>

# ••• other notes

## function as source

A stream can also be instantiated from a (sync or async) function source, it will be called sequentially to get the next source element, e.g. stream from a `Queue` source:

<details><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
from queue import Queue, Empty

ints_queue: Queue[int] = ...

ints: stream[int] = (
    stream(lambda: ints_queue.get(timeout=2))
    .catch(Empty, terminate=True)
)
```
</details>


## starmap

The `star` function decorator transforms a function that takes several positional arguments into a function that takes a tuple. `astar` is the equivalent for async functions.

<details><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
from streamable import star

indexed_pokemons: stream[str] = (
    stream(enumerate(pokemons))
    .map(star(lambda index, poke: f"#{index + 1} {poke}"))
)
assert list(indexed_pokemons) == ['#1 bulbasaur', '#2 ivysaur', '#3 venusaur', '#4 charmander', '#5 charmeleon', '#6 charizard', '#7 squirtle', '#8 wartortle', '#9 blastoise']
```

</details>


## zip

Use the builtins' `zip` function:

<details><summary style="text-indent: 40px;">👀 show snippet</summary></br>

```python
from streamable import star

cubes: stream[int] = (
    stream(zip(ints, ints, ints))  # stream[tuple[int, int, int]]
    .map(star(lambda a, b, c: a * b * c))  # stream[int]
)

assert list(cubes) == [0, 1, 8, 27, 64, 125, 216, 343, 512, 729]
```
</details>


##  distinct

To collect distinct elements you can `set(a_stream)`.

To deduplicates in the middle of the stream, `.filter` new values and `.do` add them into a `set` (or a fancier external cache):

<details><summary style="text-indent: 40px;">👀 show snippet</summary></br>

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
</details>

## iteration can be resumed after an exception

If at one point during the iteration an exception is raised and caught, the iteration can resume from there.

## performances

All the operations process elements on-the-fly. At any point in time, concurrent operations only keep `concurrency` elements in memory for processing.

There is *zero overhead during iteration compared to builtins*:

```python
odd_int_strings = stream(range(1_000_000)).filter(lambda n: n % 2).map(str)
```

`iter(odd_int_strings)` visits the operations lineage and returns exactly this iterator:

```python
map(str, filter(lambda n: n % 2, range(1_000_000)))
```

Operations have been [implemented](https://github.com/ebonnal/streamable/blob/main/streamable/iterators.py) with speed in mind. If you have any ideas for improvement, whether performance-related or not, an issue, PR, or discussion would be very much appreciated! ([CONTRIBUTING.md](CONTRIBUTING.md))

</details>

## highlights from the community
- [Tryolabs' Top 10 Python libraries of 2024](https://tryolabs.com/blog/top-python-libraries-2024#top-10---general-use) ([LinkedIn](https://www.linkedin.com/posts/tryolabs_top-python-libraries-2024-activity-7273052840984539137-bcGs?utm_source=share&utm_medium=member_desktop), [Reddit](https://www.reddit.com/r/Python/comments/1hbs4t8/the_handpicked_selection_of_the_best_python/))
- [PyCoder’s Weekly](https://pycoders.com/issues/651) x [Real Python](https://realpython.com/)
- [@PythonHub's tweet](https://x.com/PythonHub/status/1842886311369142713)
- [Upvoters on our showcase Reddit post](https://www.reddit.com/r/Python/comments/1fp38jd/streamable_streamlike_manipulation_of_iterables/)
