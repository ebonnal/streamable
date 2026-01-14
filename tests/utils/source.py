"""Test data constants."""

# Test data size
from streamable import stream


N = 256

# Test data sources
INTEGERS = range(N)
EVEN_INTEGERS = range(0, N, 2)
ints = stream(INTEGERS)
