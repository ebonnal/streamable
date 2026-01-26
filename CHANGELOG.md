# Changelog

Note: This library follows *MAJOR.MINOR.PATCH* semantic versioning, breaking changes require a *MAJOR* version bump.

## [2.0.0]

Multiple breaking changes in this new major version, let's do a tour of them:

`Stream` renamed lowercase `stream`
```python
# v1.6.6
from streamable import Stream
ints = Stream(range(10))

# v2.0.0
from streamable import stream
ints = stream(range(10))
```
All `a*` methods have been merged into their sync counterparts. All operations now accept both sync and async functions.

Operations changes:

- **[methods merged]** `.map`/`.amap` → `.map`
  - **[kwarg renamed]** `ordered` → `as_completed` (opposite value)
  - **[kwarg removed]**: `via`
  - *[kwarg extended]* `concurrency` can now be an `Executor`
  - *[pos arg renamed]* `transformation` → `into`
- **[methods merged + renamed]** `.foreach`/`.aforeach` → `.do`
  - + same changes as `.map`
- **[methods merged]** `.filter`/`.afilter` → `.filter`
  - *[pos arg renamed]* `predicate` → `where`
- **[methods merged]** `.flatten`/`.aflatten` → `.flatten`
- **[methods merged]** `.group`/`.agroup`/`.groupby`/`.agroupby` → `.group`
  - **[output change]** now `.group(by=...)` yields `(key, elements)` tuples.
  - **[kwarg renamed]** `interval` → `within`
  - **[kwarg renamed]** `size` → `up_to`
- **[methods merged]** `.skip`/`.askip` → `.skip`
  - **[kwargs merged]** `count` and `until` params merged into one `until: int | Callable`
- **[methods merged+ renamed]** `.truncate`/`.atruncate` → `.take`
  - **[kwargs merged]** `count` and `when` params merged into one `until: int | Callable`
- **[methods merged]** `.catch`/`.acatch` → `.catch`
  - **[kwarg removed]** `finally_raise`
  - **[kwarg renamed + retyped]** `replacement: T` → `replace: Callable[[Exception], U]` 
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
