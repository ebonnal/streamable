# Changelog

`streamable` follows *MAJOR.MINOR.PATCH* semantic versioning, expect breaking changes only in *MAJOR* version bumps.

## [2.1.0b0]

- `.buffer`: `up_to` now defaults to `None` for unbounded buffer.
  ```python
  nolimit_buffered_stream = stream(...).buffer()
  ```

- `.__aiter__` now returns a `ClosableAsyncIterator` exposing an `.aclose()` method:
  - During an async iteration, these operations spawn child tasks:
    - `.map`/`.do`/`.flatten` with `concurrency > 1`
    - `.buffer`
    - `.group(..., within=timedelta(...))`
    - `.observe(..., every=timedelta(...))`
  - When the iteration is complete, all the child tasks are done.
  - When the iterator is destroyed before it is exhausted, the pending child tasks are cancelled at a subsequent cycle of the event loop.
  - Use the `.aclose` method to eagerly cancel any pending child tasks:

  ```python
  from contextlib import aclosing

  s = stream(range(10)).do(asyncio.sleep, concurrency=4)
  async with aclosing(aiter(s)) as it:
      assert await anext(it) == 0
      assert await anext(it) == 1
      # 4 pending tasks, for elements 2, 3, 4, 5
  # these 4 tasks are cancelled

  # `aclosing` appears in Python 3.10. For prior versions, use `.aclose` in a `finally` block:
  s = stream(range(10)).do(asyncio.sleep, concurrency=4)
  it = aiter(s)
  try:
      assert await anext(it) == 0
      assert await anext(it) == 1
      # 4 pending tasks, for elements 2, 3, 4, 5
  finally:
      await it.aclose()
      # these 4 tasks are cancelled
  ```

## [2.0.0]

`stream` lowercase is now preferred over `Stream`.

All `a*` methods have been merged into their sync counterparts. All operations now accept both sync and async functions.

Operations changes:

- **[methods merged]** `.map`/`.amap` → `.map`
  - **[kwarg renamed]** `ordered` → `as_completed` (inverted semantics)
  - **[kwarg removed]**: `via`
  - *[kwarg extended]* `concurrency` can now be an `Executor`
  - *[pos arg renamed]* `transformation` → `into`
- **[methods merged + renamed]** `.foreach`/`.aforeach` → `.do`
  - same changes as `.map`
- **[methods merged]** `.filter`/`.afilter` → `.filter`
  - *[pos arg renamed]* `predicate` → `where`
- **[methods merged]** `.flatten`/`.aflatten` → `.flatten`
- **[methods merged]** `.group`/`.agroup`/`.groupby`/`.agroupby` → `.group`
  - **[output change]** now `.group(by=...)` yields `(key, elements)` tuples.
  - **[kwarg renamed]** `interval` → `within`
  - **[kwarg renamed]** `size` → `up_to`
- **[methods merged]** `.skip`/`.askip` → `.skip`
  - **[kwargs merged]** `count` and `until` params merged into one `until: int | Callable`
- **[methods merged + renamed]** `.truncate`/`.atruncate` → `.take`
  - **[kwargs merged]** `count` and `when` params merged into one `until: int | Callable`
- **[methods merged]** `.catch`/`.acatch` → `.catch`
  - **[kwarg removed]** `finally_raise`
  - **[kwarg renamed + re-typed]** `replacement: T` → `replace: Callable[[Exception], U]`
  - **[kwarg renamed]** `when` → `where`
  - *[new kwarg]* add `do` for side effect on catch
- **[methods merged]** `.throttle`/`.athrottle` → `.throttle`
  - **[pos arg renamed + required]** `count` → `up_to`, now required
  - **[kwarg required]** `per`, now required
- `.observe`
  - **[pos arg renamed]** `what` → `subject`
  - *[new kwarg]* add optional `every: int | timedelta` param for periodic observation
  - *[new kwarg]* add `do` for custom observation

- **[methods removed]** `.distinct`/`.adistinct`
- **[methods removed]** `.count` / `.acount`
- **[methods removed]** `.display`
