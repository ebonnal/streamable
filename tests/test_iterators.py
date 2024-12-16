import unittest

from streamable.iterators import _OSConcurrentMapIterable


class TestIterators(unittest.TestCase):
    def test_validation(self):
        with self.assertRaisesRegex(
            ValueError,
            "`buffersize` should be greater or equal to 1, but got 0.",
            msg="`_OSConcurrentMapIterable` constructor should raise for non-positive buffersize",
        ):
            _OSConcurrentMapIterable(
                iterator=iter([]),
                transformation=str,
                concurrency=1,
                buffersize=0,
                ordered=True,
                via="thread",
            )
