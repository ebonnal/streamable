import os
from enum import Enum
from typing import List

import pytest


class SkippedTest(Enum):
    REFCOUNTING_DEPENDENT = "refcounting-dependent"
    TIMING_SENSITIVE = "timing-sensitive"

    @classmethod
    def list(cls) -> List["SkippedTest"]:
        return [
            cls(flag.strip())
            for flag in os.environ.get("STREAMABLE_SKIPPED_TESTS", "").split(",")
            if flag.strip()
        ]


mark_refcounting_dependent = pytest.mark.skipif(
    SkippedTest.REFCOUNTING_DEPENDENT in SkippedTest.list(),
    reason="refcounting-dependent test skipped",
)

mark_timing_sensitive = pytest.mark.skipif(
    SkippedTest.TIMING_SENSITIVE in SkippedTest.list(),
    reason="timing-sensitive test skipped",
)
