# dev commands
## make
`make all`, or a specific target:
```bash
make venv
make test
make type-check
make format
make lint
```

## run a specific test
```bash
pytest -s -k test_etl_example
```

## changelog
```bash
git log --oneline -- version.py
```

# design principles and patterns

## decorator
A `Stream[T]` *decorates* an iterable with a fluent interface: it is instanciated from an `Iterable[T]`/`AsyncIterable[T]` and is an `Iterable[T]`/`AsyncIterable[T]`.

## single responsability

A `Stream` exposes a minimalist interface to manipulate elements, creating its source or consuming it is not its responsability, it should be used in combination with specialized libraries like `functools`, `csv`, `json`, `pyarrow`, `psycopg2`, `boto3`, `requests`, ...

## fluent interface
The lib is built around a single entry point, the `Stream` class. This is the only import required. This lib must be trivial to use, all the behaviors it exposes should be carefully named and designed to make them as self-explanatory as possible. A stream declaration should resemble natural language:
```python
events = Stream(events).do(print).truncate(when=lambda event: event["year"] > "2023").catch(KeyError)
```
is relatively close to
> **Stream** the **events** and **for each** one, **print** it, **truncate** the stream **when** an **event**'s **year** is after **2023**, and **catch `KeyError`s** along the way.

## composite
Applying an operation simply performs argument validation and returns a new instance of a `Stream` sub-class corresponding to the operation. Defining a stream is basically constructing a composite structure where each node is a `Stream` instance: The above `events` variable holds a `CatchStream[int]` instance, whose `.upstream` attribute points to a `TruncateStream[int]` instance, whose `.upstream` attribute points to a `DoStream[int]` instance, whose `.upstream` points to a `Stream[int]` instance. Each node's `.source` attribute points to the same `range(10)`.

## visitor
Each node in this composite structure exposes an `.accept` method enabling traversal by a visitor. `.__iter__`/`.__aiter__`/`.__repr__`/`.__str__`/`.__eq__` rely on visitor classes defined in the `streamable.visitors` package.
