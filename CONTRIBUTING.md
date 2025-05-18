# Make

`make all` or a specific target:
```bash
make venv
make test
make type-check
make format
```

Run a specific test:
```bash
python -m unittest tests.test_stream.TestStream.test_distinct
```

# Design principles

## Fluent Interface Pattern
The lib is built around a single entry point, the `Stream` class. This is the only import required. This lib must be trivial to use, all the behaviors it exposes should be carefully named and designed to make them as self-explanatory as possible. A stream declaration should resemble natural language:
```python
events = Stream(events).foreach(print).truncate(when=lambda event: event["year"] > "2023").catch(KeyError)
```
is relatively close to
> **Stream** the **events** and **for each** one, **print** it, **truncate** the stream **when** an **event**'s **date** is after **2023**, and **catch `KeyError`s** along the way.

## Composite Pattern
Applying an operation simply performs argument validation and returns a new instance of a `Stream` sub-class corresponding to the operation. Defining a stream is basically constructing a composite structure where each node is a `Stream` instance: The above `events` variable holds a `CatchStream[int]` instance, whose `.upstream` attribute points to a `TruncateStream[int]` instance, whose `.upstream` attribute points to a `ForeachStream[int]` instance, whose `.upstream` points to a `Stream[int]` instance. Each node's `.source` attribute points to the same `range(10)`.

## Visitor Pattern
Each node in this composite structure exposes an `.accept` method enabling traversal by a visitor. `.__iter__`/`.__aiter__`/`.__repr__`/`.__str__`/`.__eq__` rely on visitor classes defined in the `streamable.visitors` package.

## Decorator Pattern
A `Stream[T]` both inherits from `Iterable[T]` and holds an `Iterable[T]` as its `.source`: when you instantiate a stream from an iterable you decorate it with a fluent interface.
