"""Test data constants."""

# Test data size
N = 256

# Test data sources
ints_src = range(N)
even_src = range(0, N, 2)


class TestError(Exception):
    """Test exception for testing error handling."""

    pass
