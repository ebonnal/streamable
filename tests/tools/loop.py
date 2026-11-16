import asyncio
import time
from collections.abc import Awaitable

TEST_LOOP = asyncio.new_event_loop()


def assert_only_current_task_is_alive() -> None:
    assert asyncio.all_tasks() == {asyncio.current_task()}


async def advance_cycles(cycles: int) -> None:
    for _ in range(cycles):
        await asyncio.sleep(0)


async def max_event_loop_stall(coro: Awaitable) -> float:
    """Awaits `coro` and returns the longest time the event loop went without running."""
    task = asyncio.ensure_future(coro)
    stall = 0.0
    while not task.done():
        start = time.perf_counter()
        await asyncio.sleep(0)
        stall = max(stall, time.perf_counter() - start)
    await task
    return stall
