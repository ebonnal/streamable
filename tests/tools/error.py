import asyncio
import sys
from contextlib import contextmanager
from typing import Any, Dict, Generator, List


class TestError(Exception):
    __test__ = False


class TestBaseError(BaseException):
    __test__ = False


@contextmanager
def collecting_unraisables() -> Generator[List["sys.UnraisableHookArgs"], None, None]:
    collected: List["sys.UnraisableHookArgs"] = []
    hook, sys.unraisablehook = sys.unraisablehook, collected.append
    try:
        yield collected
    finally:
        sys.unraisablehook = hook


@contextmanager
def collecting_unretrievables() -> Generator[List[Dict[str, Any]], None, None]:
    loop = asyncio.get_running_loop()
    unhandled: List[Dict[str, Any]] = []
    default_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _, context: unhandled.append(context))
    try:
        yield unhandled
    finally:
        loop.set_exception_handler(default_handler)
