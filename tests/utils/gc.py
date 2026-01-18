from contextlib import contextmanager
import gc


get_referees = gc.get_referents


@contextmanager
def disabled_gc():
    gc.disable()
    try:
        yield
    finally:
        gc.enable()
