# `streamable`: *fluent iteration*

[![Actions Status](https://github.com/ebonnal/streamable/workflows/unittest/badge.svg)](https://github.com/ebonnal/streamable/actions)
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
integers: Stream[int] = Stream(lambda: range(10))
```

Instantiate a `Stream` by providing a function that returns a fresh `Iterable` (the data source).

## 4. operate

Applying an operation:
- is ***lazy*** i.e. it does not iterate over the source
- returns a ***child*** stream without modifying the parent

```python
odd_square_strings: Stream[str] = (
    integers
    .map(lambda x: x ** 2)
    .filter(lambda x: x % 2)
    .map(str)
)
```

## 5. iterate
`Stream[T]` extends `Iterable[T]` allowing:
```python
set(odd_squares)
```
```python
sum(odd_squares)
```
```python
for i in odd_squares:
    ...
```

---

# 📒 ***Operations***

## `.map`
Defines the application of a function on parent elements.
```python
integer_strings: Stream[str] = integers.map(str)
```

It has an optional `concurrency` parameter to execute the function concurrently while preserving the order (threads).

## `.foreach`
Defines the application of a function on parent elements like `.map`, but the parent elements are forwarded instead of the result of the function.

```python
printed_integers: Stream[int] = integers.foreach(print)
```

It has an optional `concurrency` parameter to execute the function concurrently while preserving the order (threads).

## `.filter`
Defines the filtering of parent elements based on a predicate function.

```python
pair_integers: Stream[int] = integers.filter(lambda x: x % 2 == 0)
```

## `.batch`

Defines the grouping of parent elements into batches.

```python
integer_batches: Stream[List[int]] = integers.batch(size=100, seconds=60)
```

Here a batch is a list of 100 elements but it may contain less elements in these cases:
- upstream is exhausted
- an exception occurred upstream
- more than `seconds` have elapsed since the last batch.

## `.flatten`

Defines the ungrouping of parent elements assuming that they are `Iterable`s.

```python
integers: Stream[int] = integer_batches.flatten()
```

It has an optional `concurrency` parameter to flatten several parent iterables concurrently (threads).

## `.slow`

Defines a maximum rate at which parent elements are yielded.

```python
slowed_integers: Stream[int] = integers.slow(frequency=2)
```

The `frequency` is expressed in elements per second.

## `.catch`

Defines the catching of exceptions satisfying a predicate function.

```python
inverse_floats: Stream[float] = integers.map(lambda x: 1 / x)
safe_inverse_floats: Stream[float] = inverse_floats.catch(lambda ex: isinstance(ex, ZeroDivisionError))
```

It has optional parameters:
- `raise_at_exhaustion`: to raise the first catched exception at upstream's exhaustion.

## `.observe`

Defines the logging of the progress of any iteration over this stream.

With
```python
observed_slowed_integers: Stream[int] = slowed_integers.observe(what="integers from 0 to 9")
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
Defines a limitation on the number of parent elements yielded.

```python
ten_first_integers: Stream[int] = integers.limit(count=10)
```


---

# 📦 ***Notes Box***

## typing
This module is **typed**, you can [`mypy`](https://github.com/python/mypy) it !

## supported Python versions
This module is **compatible with Python `3.7` or newer**.

It is unittested for: `3.7.17`, `3.8.18`, `3.9.18`, `3.10.13`, `3.11.7`, `3.12.1`

## multi lines
You may find it convenient to enclose your operations in parentheses instead of using trailing backslashes `\`.

```python
(
    Stream(lambda: range(10))
    .map(lambda x: 1 / x)
    .catch(ZeroDivisionError)
    .exhaust()
)
```

## functions
`Stream`'s methods are also exposed as functions:
```python
from streamable.functions import slow
iterator: Iterator[int] = ...
slowed_iterator: Iterator[int] = slow(iterator)
```

## set logging level
```python
import logging
logging.getLogger("streamable").setLevel(logging.WARNING)
```

## visitor pattern
A `Stream` exposes an `.accept` method and you can implement your custom [***visitor***](https://en.wikipedia.org/wiki/Visitor_pattern) by extending the `streamable.visit.Visitor` class.
