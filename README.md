# ༄ `streamable`: *fluent iteration*
[![Actions Status](https://github.com/ebonnal/streamable/workflows/unittest/badge.svg)](https://github.com/ebonnal/streamable/actions)
[![codecov](https://codecov.io/gh/ebonnal/streamable/graph/badge.svg?token=S62T0JQK9N)](https://codecov.io/gh/ebonnal/streamable)
[![Actions Status](https://github.com/ebonnal/streamable/workflows/typing/badge.svg)](https://github.com/ebonnal/streamable/actions)
[![Actions Status](https://github.com/ebonnal/streamable/workflows/lint/badge.svg)](https://github.com/ebonnal/streamable/actions)
[![Actions Status](https://github.com/ebonnal/streamable/workflows/PyPI/badge.svg)](https://pypi.org/project/streamable)

---

## TL;DR:
|||
|--|--|
|🇹 typed|The `Stream[T]` class extends `Iterable[T]`|
|🪶 light|`pip install streamable` with no additional dependencies|
|🛡️ robust|Extensively unittested with 100% coverage|
|💤 lazy|Operations are only evaluated during iteration|
|🔄 concurrent|Threads-based or `asyncio`-based concurrency for I/O bound tasks|

---


## 1. install

```bash
pip install streamable
```

## 2. import
```python
from streamable import Stream
```

## 3. init
Instantiate a `Stream[T]` from an `Iterable[T]`.

```python
integers: Stream[int] = Stream(range(10))
```


## 4. operate
- `Stream`s are ***immutable***: applying an operation returns a new stream.

- Operations are ***lazy***: only evaluated at iteration time.

```python
odd_integer_strings: Stream[str] = (
    integers
    .filter(lambda n: n % 2)
    .map(str)
)
```


## 5. iterate
- Iterate over a `Stream[T]` as you would over any other `Iterable[T]`.
- Source elements are ***processed on-the-fly***.

### collect it
```python
>>> list(odd_integer_strings)
['1', '3', '5', '7', '9']
>>> set(odd_integer_strings)
{'9', '1', '5', '3', '7'}
```

### reduce it
```python
>>> sum(integers)
45
>>> from functools import reduce
>>> reduce(str.__add__, odd_integer_strings)
'13579'
```

### loop it
```python
for odd_integer_string in odd_integer_strings:
    ...
```

---

# 📒 ***Operations***

## `.map`
Applies a transformation on elements:
```python
negative_integer_strings: Stream[str] = integers.map(lambda n: -n).map(str)

assert list(integer_strings) == ['0', '-1', '-2', '-3', '-4', '-5', '-6', '-7', '-8', '-9']
```

It has an optional `concurrency: int` parameter to execute the function concurrently (threads-based) while preserving the order.

It has a sibling operation called `.amap` to apply an async function concurrently (see section ***`asyncio` support***).

## `.foreach`
Applies a side effect on elements:

```python
self_printing_integers: Stream[int] = integers.foreach(print)

assert list(self_printing_integers) == list(integers)  # will trigger the printing of the integers
```

It has an optional `concurrency: int` parameter to execute the function concurrently (threads-based) while preserving the order.

It has a sibling operation called `.aforeach` to apply an async function concurrently (see section ***`asyncio` support***).

## `.filter`
Keeps only the elements that satisfy a condition:

```python
pair_integers: Stream[int] = integers.filter(lambda n: n % 2 == 0)

assert list(pair_integers) == [0, 2, 4, 6, 8]
```

## `.slow`

Limits the rate at which elements are yielded up to a maximum number of elements per second:

```python
slow_integers: Stream[int] = integers.slow(frequency=5)

assert list(slow_integers) == list(integers)  # takes 10 / 5 = 2 seconds
```

## `.group`

Groups elements into `List`s:

```python
integers_5_by_5: Stream[List[int]] = integers.group(size=5)

assert list(integers_5_by_5) == [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]]
```
```python
integers_by_parity: Stream[List[int]] = integers.group(by=lambda n: n % 2)

assert list(integers_by_parity) == [[0, 2, 4, 6, 8], [1, 3, 5, 7, 9]]
```
```python
integers_within_1s: Stream[List[int]] = integers.slow(frequency=2).group(seconds=1)

assert list(integers_within_1s) == [[0, 1], [2, 3], [4, 5], [6, 7], [8, 9]]
```

Combine the `size`/`by`/`seconds` parameters:
```python
integers_2_by_2_by_parity: Stream[List[int]] = integers.group(by=lambda n: n % 2, size=2)

assert list(integers_2_by_2_by_parity) == [[0, 2], [1, 3], [4, 6], [5, 7], [8], [9]]
```

## `.flatten`

Ungroups elements assuming that they are `Iterable`s.

```python
pair_then_odd_integers: Stream[int] = integers_by_parity.flatten()

assert pair_then_odd_integers == [0, 2, 4, 6, 8, 1, 3, 5, 7, 9]
```

It has an optional `concurrency: int` parameter to flatten several iterables concurrently (threads).


## `.catch`

Catches a given type of exceptions:

```python
safe_inverse_floats: Stream[float] = (
    integers
    .map(lambda n: round(1 / n, 2))
    .catch(ZeroDivisionError)
)

assert list(safe_inverse_floats) == [1.0, 0.5, 0.33, 0.25, 0.2, 0.17, 0.14, 0.12, 0.11]
```

It has an optional `finally_raise: bool` parameter to raise the first catched exception when upstream's iteration ends.

## `.truncate`
Stops the iteration:
- after a given number of yielded elements:
```python
five_first_integers: Stream[int] = integers.truncate(5)

assert list(five_first_integers) == [0, 1, 2, 3, 4]
```

- as soon as a condition is satisfied:
```python
five_first_integers: Stream[int] = integers.truncate(when=lambda n: n == 5)

assert list(five_first_integers) == [0, 1, 2, 3, 4]
```

## `.observe`

Logs the progress of iterations over this stream:

If you iterate on
```python
observed_slow_integers: Stream[int] = slow_integers.observe("integers")
```
you will get these logs:
```
INFO: [duration=0:00:00.502155 errors=0] 1 integers yielded
INFO: [duration=0:00:01.006336 errors=0] 2 integers yielded
INFO: [duration=0:00:02.011921 errors=0] 4 integers yielded
INFO: [duration=0:00:04.029666 errors=0] 8 integers yielded
INFO: [duration=0:00:05.039571 errors=0] 10 integers yielded
```

The amount of logs will never be overwhelming because they are produced logarithmically e.g. the 11th log will be produced when the iteration reaches the 1024th element.

---

# 📦 ***Notes Box***

## typing
This is a **fully typed library** (you can [`mypy`](https://github.com/python/mypy) it).

## supported Python versions
Compatible with **Python `3.7+`** (unittested for: `3.7.17`, `3.8.18`, `3.9.18`, `3.10.13`, `3.11.7`, `3.12.1`).

## support for `asyncio`
As an alternative to the threads-based concurrency available for `.map` and `.foreach` operations (via their `concurrency` parameter), one can use `.amap` and `.aforeach` operations to **apply `async` functions** concurrently on a stream:

```python
import asyncio
import time

async def slow_async_square(n: int) -> int:
    await asyncio.sleep(3)
    return n ** 2

def slow_str(n: int) -> str:
    time.sleep(3)
    return str(n)

print(
    ", ".join(
        integers
        # coroutines-based concurrency
        .amap(slow_async_square, concurrency=8)
        # threads-based concurrency
        .map(slow_str, concurrency=8)
        .truncate(5)
    )
)
```
this prints (in 6s):
```bash
0, 1, 4, 9, 16
```

## CPU-bound tasks
For CPU-bound tasks, consider using the [`PyPy`](https://github.com/pypy/pypy) interpreter whose *Just In Time* (JIT) compilation should drastically improve performances, e.g. this snippet is run **50 times faster** by [`PyPy`](https://github.com/pypy/pypy) compared to standard *CPython* interpreter:
```python
# cpu_bound_script.py
from streamable import Stream
print(
    sum(
        Stream(range(1, 1_000_000_000))
        .map(lambda n: 1/n)
    )
)
```

[Few rough runtime orders of magnitude: CPython vs PyPy vs Java vs C vs Rust.](https://github.com/ebonnal/streamable/issues/10)

## Extract-Transform-Load tasks

One can leverage this library to write elegant ETL scripts, check the [**README dedicated to ETL**](README_ETL.md).

## as functions
The `Stream`'s methods are also exposed as functions:
```python
from streamable.functions import slow

iterator: Iterator[int] = ...
slow_iterator: Iterator[int] = slow(iterator)
```

## visitor pattern
The `Stream` class exposes an `.accept` method and you can implement a [***visitor***](https://en.wikipedia.org/wiki/Visitor_pattern) by extending the `streamable.visitor.Visitor` class:

```python
from streamable.visitor import Visitor

class DepthVisitor(Visitor[int]):
    def visit_stream(self, stream: Stream) -> int:
        if not stream.upstream:
            return 1
        return 1 + stream.upstream.accept(self)

def stream_depth(stream: Stream) -> int:
    return stream.accept(DepthVisitor())
```
```python
>>> stream_depth(odd_integer_strings)
3
```

## go to line
Style tip: Enclose operations in parentheses to keep lines short without needing trailing backslashes `\`.

```python
stream: Stream[str] = (
    Stream(range(10))
    .map(str)
    .foreach(print)
    .flatten()
    .truncate(10)
)
```

## explain
```python
print(stream.explanation())
```
```
└─•TruncateStream(count=10, when=None)
  └─•FlattenStream(concurrency=1)
    └─•ForeachStream(effect=print, concurrency=1)
      └─•MapStream(transformation=str, concurrency=1)
        └─•Stream(source=range(...))
```

## change logging level
```python
import logging

logging.getLogger("streamable").setLevel(logging.WARNING)
```
