import sys
from contextlib import contextmanager
from typing import Generator, List


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
