# `streamable`: *fluent iteration*

[![Actions Status](https://github.com/ebonnal/streamable/workflows/unittest/badge.svg)](https://github.com/ebonnal/streamable/actions)
[![codecov](https://codecov.io/gh/ebonnal/streamable/graph/badge.svg?token=S62T0JQK9N)](https://codecov.io/gh/ebonnal/streamable)
[![Actions Status](https://github.com/ebonnal/streamable/workflows/typing/badge.svg)](https://github.com/ebonnal/streamable/actions)
[![Actions Status](https://github.com/ebonnal/streamable/workflows/lint/badge.svg)](https://github.com/ebonnal/streamable/actions)
[![Actions Status](https://github.com/ebonnal/streamable/workflows/PyPI/badge.svg)](https://github.com/ebonnal/streamable/actions)

## 1. install

```bash
pip install streamable
```

## 2. import
```python
from streamable import Stream
```

## 3. init

```python
integers: Stream[int] = Stream(lambda: range(N))
```

Instantiate a `Stream[T]` by providing a function that returns a fresh `Iterable[T]` (the data source).

## 4. operate

```python
odd_square_strings: Stream[str] = (
    integers
    .map(lambda n: n ** 2)
    .filter(lambda n: n % 2)
    .map(str)
)
```

`Stream` instances are ***immutable***: operations return a new stream.

Operations are ***lazy***: they do not iterate over the source.

## 5. iterate
`Stream[T]` extends `Iterable[T]` allowing:
```python
set(odd_square_strings)
```
```python
sum(odd_square_strings)
```
```python
for i in odd_square_strings:
    ...
```

---

# 📒 ***Operations***

## `.map`
Applies a function on elements.
```python
integer_strings: Stream[str] = integers.map(str)
```

It has an optional `concurrency` parameter to execute the function concurrently while preserving the order (threads).

## `.foreach`
Applies a function on elements like `.map` but yields the elements instead of the results.

```python
printed_integers: Stream[int] = integers.foreach(print)
```

It has an optional `concurrency` parameter to execute the function concurrently while preserving the order (threads).

## `.filter`
Filters elements based on a predicate function.

```python
pair_integers: Stream[int] = integers.filter(lambda n: n % 2 == 0)
```

## `.batch`

Groups elements into batches.

```python
integer_batches: Stream[List[int]] = integers.batch(size=100, seconds=60)
```

A batch is a list of `size` elements but it may contain fewer elements in these cases:
- upstream is exhausted
- an exception occurred upstream
- more than `seconds` have elapsed since the last batch.

## `.flatten`

Ungroups elements assuming that they are `Iterable`s.

```python
integers: Stream[int] = integer_batches.flatten()
```

It has an optional `concurrency` parameter to flatten several iterables concurrently (threads).

## `.slow`

Limits the rate at which elements are yielded up to a maximum `frequency` (elements per second).

```python
slow_integers: Stream[int] = integers.slow(frequency=2)
```

## `.catch`

Catches exceptions that satisfy a predicate function.

```python
safe_inverse_floats: Stream[float] = (
    integers
    .map(lambda n: 1 / n)
    .catch(lambda ex: isinstance(ex, ZeroDivisionError))
)
```

It has an optional `raise_at_exhaustion` parameter to raise the first catched exception when iteration ends.

## `.observe`

Logs the progress of iterations over this stream.

With
```python
observed_slow_integers: Stream[int] = slow_integers.observe(what="integers from 0 to 9")
```

you should get:

```
INFO: iteration over 'integers from 0 to 9' will be observed.
INFO: after 0:00:00.000283, 0 error and 1 `integers from 0 to 9` yielded.
INFO: after 0:00:00.501373, 0 error and 2 `integers from 0 to 9` yielded.
INFO: after 0:00:01.501346, 0 error and 4 `integers from 0 to 9` yielded.
INFO: after 0:00:03.500864, 0 error and 8 `integers from 0 to 9` yielded.
INFO: after 0:00:04.500547, 0 error and 10 `integers from 0 to 9` yielded.
```

The amount of logs will never be overwhelming because they are produced logarithmically e.g. the 11th log will be produced when the iteration reaches the 1024th element.

## `.limit`
Limits the number of elements yielded.

```python
five_first_integers: Stream[int] = integers.limit(count=5)
```


---

# 📦 ***Notes Box***

## typing
This is a **typed module**, you can [`mypy`](https://github.com/python/mypy) it.

## supported Python versions
Compatible with **Python `3.7` or newer** (unittested for: `3.7.17`, `3.8.18`, `3.9.18`, `3.10.13`, `3.11.7`, `3.12.1`).

## go to line
Tip: enclose operations in parentheses to avoid trailing backslashes `\`.

```python
stream: Stream[str] = (
    Stream(lambda: range(10))
    .map(str)
    .batch(2)
    .foreach(print)
    .flatten()
    .filter()
    .catch()
)
```

## functions
The `Stream`'s methods are also exposed as functions:
```python
from streamable.functions import slow

iterator: Iterator[int] = ...
slow_iterator: Iterator[int] = slow(iterator)
```

## set logging level
```python
import logging

logging.getLogger("streamable").setLevel(logging.WARNING)
```

## visitor pattern
The `Stream` class exposes an `.accept` method and you can implement a [***visitor***](https://en.wikipedia.org/wiki/Visitor_pattern) by extending the `streamable.visitor.Visitor` class.
