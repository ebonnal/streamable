# Changelog

## [2.0.0]

### 🚨 Breaking Changes

#### Class Name
- **BREAKING**: Renamed `Stream` → `stream`
  ```python
  # v1.6.6
  from streamable import Stream
  ints = Stream(range(10))
  
  # v2.0.0
  from streamable import stream
  ints = stream(range(10))
  ```

#### Unified Sync/Async Methods
- **BREAKING**: All `a*` methods merged into their sync counterparts. All operations now accept both sync and async functions automatically.
  - `acatch()` → merged into `catch()` 
  - `afilter()` → merged into `filter()`
  - `aflatten()` → merged into `flatten()`
  - `aforeach()` → merged into `do()` (see below)
  - `agroup()` → merged into `group()`
  - `agroupby()` → merged into `group()` with `by` parameter (see below)
  - `amap()` → merged into `map()`
  - `askip()` → merged into `skip()`
  - `atruncate()` → merged into `take()` (see below)

#### Method Renames
- **BREAKING**: `foreach()` / `aforeach()` → `do()`

- **BREAKING**: `truncate()` / `atruncate()` → `take()`

- **BREAKING**: `groupby()` / `agroupby()` → merged into `group()`
  ```python
  # v1.6.6
  ints.groupby(key=lambda x: x % 2)
  
  # v2.0.0
  ints.group(by=lambda x: x % 2)
  ```

#### Parameter Renames
- **BREAKING**: `catch()` parameter changes:
  - Parameter renames: `when` → `where`, `replacement` → `replace` (now a callable instead of a value)
  - Parameter removed: `finally_raise` (use the new `do` parameter to save and manually raise errors after iteration if needed)
  
  ```python
  # v1.6.6
  ints.catch(
      errors=ValueError,
      when=lambda e: "error" in str(e),
      replacement=42,
  )
  
  # v2.0.0
  ints.catch(
      errors=ValueError,
      where=lambda e: "error" in str(e),  # when → where
      replace=lambda e: 42,  # replacement → replace, now a callable
  )
  ```

- **BREAKING**: `filter()` positional parameter rename: `when` → `where`

- **BREAKING**: `skip()` API changes: merge `count` and `until` params

- **BREAKING**: `group()` parameter renames:
  - `size` → `up_to` (positional)
  - `interval` → `every` (keyword-only)

- **BREAKING**: `throttle()` parameter changes:
  - `count` → `up_to`, now required
  - `per` is now required

- **BREAKING**: `map()` parameter rename:
  - `transformation` → `into` (positional)
  - Removed `via` parameter, for process-based concurrency you can now pass a `ProcessPoolExecutor` as `concurrency`
  
  ```python
  # v1.6.6
  ints.map(fn, concurrency=2, via="thread")
  # v2.0.0
  ints.map(fn, concurrency=2)
  
  # v1.6.6
  ints.map(fn, concurrency=2, via="process")
  # v2.0.0
  with ProcessPoolExecutor(max_workers=2) as processes:
    ints.map(fn, concurrency=processes)
  ```

#### Removed Methods
- **BREAKING**: `display()` removed
- **BREAKING**: `count()` / `acount()` removed
- **BREAKING**: `distinct()` / `adistinct()` removed

```python
# v1.6.6
stream(...).distinct()

# v2.0.0
seen: set[...] = set()

stream(...).filter(lambda _: _ not in seen).do(seen.add)
```

#### Redesigned Methods
- **BREAKING**: `observe()`: positional parameter rename: `what` → `subject`

### ✨ New Features

- Unified sync/async operations: All methods now automatically handle both sync and async functions without needing separate `a*` methods
- `__iadd__` support: In-place addition operator (`+=`) support for streams
- Enhanced `observe()`: New flexible observation with configurable `every` intervals and custom `how` to process observation message.
- Improved `catch()`: Now supports `do` side effects in addition to `replace`, and new `stop` parameter to halt iteration on caught exception.
