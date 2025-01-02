import unittest

from streamable.iterators import ObserveIterator, _OSConcurrentMapIterable


class TestIterators(unittest.TestCase):
    def test_validation(self):
        with self.assertRaisesRegex(
            ValueError,
            "`buffersize` must be >= 1 but got 0",
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

        with self.assertRaisesRegex(
            ValueError,
            "`base` must be > 0 but got 0",
            msg="",
        ):
            ObserveIterator(
                iterator=iter([]),
                what="",
                base=0,
            )
