from contextlib import contextmanager


@contextmanager
def noop_context_manager():
    yield
