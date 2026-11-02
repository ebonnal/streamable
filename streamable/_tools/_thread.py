import threading

_streamable_thread_local = threading.local()


def init_thread_local() -> None:
    _streamable_thread_local.is_owned_thread = True


def is_owned_thread() -> bool:
    return getattr(_streamable_thread_local, "is_owned_thread", False)
