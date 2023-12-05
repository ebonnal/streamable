import time
import timeit
import unittest
from typing import TypeVar

from kioss import Pipe

T = TypeVar("T")


def timepipe(pipe: Pipe):
    def iterate():
        for _ in pipe:
            pass

    return timeit.timeit(iterate, number=1)


# simulates an I/0 bound function
def ten_ms_identity(x: T) -> T:
    time.sleep(0.01)
    return x


# size of the test collections
N = 64


class TestPipe(unittest.TestCase):
    def test(self):
        pass
