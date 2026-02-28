from collections.abc import Iterable
from datetime import timedelta
import timeit
from streamable import stream

N = 10_000_000
ints = stream(range(N))


def consume(s: Iterable):
    for _ in s:
        pass


baseline = timeit.timeit(lambda: consume(map(lambda _: _, ints)), number=50) / 50
baseline = timeit.timeit(lambda: consume(map(lambda _: _, ints)), number=50) / 50

for times, s in (
    (50, ints.map(lambda _: _)),
    (50, ints.filter(lambda _: _)),
    (50, ints.do(lambda _: _)),
    (50, ints.skip(N)),
    (50, ints.catch(ValueError)),
    (50, ints.group(5)),
    (50, ints.take(N)),
    (10, ints.observe("ints", do=bool)),
    (10, ints.observe("ints", do=bool, every=N)),
    (10, ints.observe("ints", do=bool, every=timedelta(seconds=1))),
    (10, ints.throttle(N, per=timedelta(seconds=1))),
    (10, ints.group(5, by=bool)),
    (1, stream((i,) for i in range(N)).flatten()),
    (1, ints.buffer(N)),
    (1, ints.map(lambda _: _, concurrency=2)),
    (1, ints.map(lambda _: _, concurrency=2, as_completed=True)),
    (1, ints.group(within=timedelta(seconds=1))),
    (1, stream((i,) for i in range(N)).flatten(concurrency=2)),
):
    duration = timeit.timeit(lambda: consume(s), number=times) / times
    print(s)
    print(f"is {duration / baseline:.2f}x slower than builtins.map")
