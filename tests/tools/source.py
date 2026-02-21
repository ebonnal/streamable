from streamable import stream


N = 256

INTEGERS = range(N)
EVEN_INTEGERS = range(0, N, 2)
ints = stream(INTEGERS)
