# `streamable`: *Ease iteration*

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

Instantiate a `Stream` by providing a function that returns an `Iterable` (the data source).

## 4. apply operations

- Applying an operation is ***lazy***: it does not compute the elements.
- A `Stream` is ***immutable***: applying an operation returns a child stream independent from the parent.

```python
odd_square_strings: Stream[str] = (
    integers
    .map(lambda x: x ** 2)
    .filter(lambda x: x % 2 == 1)
    .map(str)
)
```

## 5. iterate

Once your stream's declaration is done you can iterate over it. A `Stream[T]` is an `Iterable[T]` and you can iterate over it as you wish, e.g.:
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

# 📒 ***Tour of Operations***

Let's dive into the operations exposed by a `Stream`.

Let's keep:
```python
integers = Stream(lambda: range(10))
```

## `.map`
Defines the application of a function on parent elements.
```python
integer_strings: Stream[str] = integers.map(str)
```

It has an optional `concurrency` parameter if you need to apply the function concurrently using multiple threads.

## `.do`
Defines the application of a function on parent elements like `.map`, but the parent elements are forwarded instead of the result of the function.

```python
printed_integers: Stream[int] = integers.do(print)
```

It also has an optional `concurrency` parameter.

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

In this example a batch is a list of 100 elements.

It may contain less elements in the following cases:
- the stream is exhausted
- an exception occurred
- more than 60 `seconds` have elapsed since the last batch has been yielded.

## `.flatten`

Defines the ungrouping of parent elements assuming that the parent elements are `Iterable`s.

```python
integers: Stream[int] = integer_batches.flatten()
```

It also has an optional `concurrency` parameter to flatten concurrently several parent iterables.

## `.chain`

Defines the concatenation of the parent stream with other streams. The resulting stream yields the elements of one stream until it is exhausted and then moves to the next one. It starts with the stream on which `.chain` is called.

```python
one_to_ten_integers: Stream[int] = Stream(lambda: range(1, 11))
eleven_to_twenty_integers: Stream[int] = Stream(lambda: range(11, 21))
twenty_one_to_thirty_integers: Stream[int] = Stream(lambda: range(21, 31))

one_to_thirty_integers: Stream[int] = one_to_ten_integers.chain(
    eleven_to_twenty_integers,
    twenty_one_to_thirty_integers,
)
```

## `.slow`

Defines a maximum rate at which parent elements are yielded.

```python
slowed_integers: Stream[int] = integers.slow(frequency=2)
```

The rate is expressed in elements per second, here a maximum of 2 elements per second are yielded when iterating on the stream.

## `.observe`

If you define:

```python
observed_slowed_integers: Stream[int] = slowed_integers.observe(what="integers from 0 to 9")
```

then when iterating over the stream, you should get these logs:

```
INFO: iteration over 'integers from 0 to 9' will be observed.
INFO: after 0:00:00.000283, 0 error and 1 `integers from 0 to 9` yielded.
INFO: after 0:00:00.501373, 0 error and 2 `integers from 0 to 9` yielded.
INFO: after 0:00:01.501346, 0 error and 4 `integers from 0 to 9` yielded.
INFO: after 0:00:03.500864, 0 error and 8 `integers from 0 to 9` yielded.
INFO: after 0:00:04.500547, 0 error and 10 `integers from 0 to 9` yielded.
```

As you can notice the logs can never be overwhelming because they are produced logarithmically: the $i^{th}$ log is produced when $2^{i-1}$ elements have been iterated.


## `.catch`

Defines that the provided type of exception will be catched.

```python
inverse_floats: Stream[float] = integers.map(lambda x: 1 / x)
safe_inverse_floats: Stream[float] = inverse_floats.catch(ZeroDivisionError)
```

It has optional parameters:
- `when`: a function that takes the parent element as input and decides whether or not to catch the exception.
- `raise_at_exhaustion`: a boolean that you can set to `True` if you want the first catched exception to be raised when the stream is exhausted (i.e. at the end of the iteration).

---

# 📦 ***Notes Box***

## typing
**This module is typed**, you can `mypy` it !

## supported Python versions
The module is at least **compatible with Python 3.7+**, it is unittested for the following versions: `3.7.17`, `3.8.18`, `3.9.18`, `3.10.13`, `3.11.7`, `3.12.1`

## multi lines
You may use parenthesis instead of trailing backslash `\` to go to line between operation declarations.
```python
(
    Stream(lambda: range(10))
    .map(lambda x: 1 / x)
    .catch(ZeroDivisionError)
    .exhaust()
)
```

## `streamable`'s functions
The functionalities exposed by the `Stream` class are also available as functions:
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
