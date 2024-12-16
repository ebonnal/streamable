import unittest

from streamable.iterators import _OSConcurrentMapIterable


class TestIterators(unittest.TestCase):
    def test_validation(self):
        with self.assertRaisesRegex(
            ValueError,
            "`buffer_size` should be greater or equal to 1, but got 0.",
            msg="`_OSConcurrentMapIterable` constructor should raise for non-positive buffer_size",
        ):
            _OSConcurrentMapIterable(
                iterator=iter([]),
                transformation=str,
                concurrency=1,
                buffer_size=0,
                ordered=True,
                via="thread",
            )
