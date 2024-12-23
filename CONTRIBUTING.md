# Design Overview

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
Each node in this composite structure exposes an `.accept` method enabling traversal by a visitor. Both `Stream.__iter__` and `Stream.__repr__` rely on visitor classes defined in the `streamable.visitors` package.

## Decorator Pattern
A `Stream[T]` both inherits from `Iterable[T]` and holds an `Iterable[T]` as its `.source`: when you instantiate a stream from an iterable you decorate it with a fluent interface.

# Cheat Sheet: commands for contributors

```bash
git clone git@github.com:ebonnal/streamable
cd streamable
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
```

## run unittest and check coverage
```bash
python -m coverage run -m unittest && coverage report
```

## check typing
```bash
python -m mypy streamable tests
```

## lint
```bash
python -m autoflake --in-place --remove-all-unused-imports --remove-unused-variables --ignore-init-module -r streamable tests \
&& python -m isort streamable tests \
&& python -m black .
```
