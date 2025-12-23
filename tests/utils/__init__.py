"""
Test utilities for streamable tests.

This package provides helpers for testing both sync and async iterables.
"""

# Re-export commonly used items for convenience
from tests.utils.types import IterableType, ITERABLE_TYPES
from tests.utils.data import N, TestError, ints_src, even_src
from tests.utils.conversion import (
    to_list,
    aiterable_to_list,
    bi_iterable_to_iter,
    anext_or_next,
    alist_or_list,
    stopiteration_type,
)
from tests.utils.timing import timestream, timecoro
from tests.utils.functions import (
    identity,
    async_identity,
    square,
    async_square,
    throw,
    throw_func,
    async_throw_func,
    throw_for_odd_func,
    async_throw_for_odd_func,
    slow_identity,
    async_slow_identity,
    slow_identity_duration,
    randomly_slowed,
    async_randomly_slowed,
    identity_sleep,
    async_identity_sleep,
    range_raising_at_exhaustion,
    src_raising_at_exhaustion,
    sync_to_bi_iterable,
)

__all__ = [
    # Types
    "IterableType",
    "ITERABLE_TYPES",
    # Data
    "N",
    "TestError",
    "ints_src",
    "even_src",
    # Conversion
    "to_list",
    "aiterable_to_list",
    "bi_iterable_to_iter",
    "anext_or_next",
    "alist_or_list",
    "stopiteration_type",
    # Timing
    "timestream",
    "timecoro",
    # Functions
    "identity",
    "async_identity",
    "square",
    "async_square",
    "throw",
    "throw_func",
    "async_throw_func",
    "throw_for_odd_func",
    "async_throw_for_odd_func",
    "slow_identity",
    "async_slow_identity",
    "slow_identity_duration",
    "randomly_slowed",
    "async_randomly_slowed",
    "identity_sleep",
    "async_identity_sleep",
    "range_raising_at_exhaustion",
    "src_raising_at_exhaustion",
    "sync_to_bi_iterable",
]
