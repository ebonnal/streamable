import unittest

from streamable.futuretools import FIFOThreadFutureResultCollection


class TestFutureTools(unittest.TestCase):
    def test_iter(self) -> None:
        iter(FIFOThreadFutureResultCollection())  # type: ignore
