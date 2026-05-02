from threading import Semaphore


class NoopSemaphore(Semaphore):
    __slots__ = ()

    def __init__(self) -> None:
        pass

    def acquire(self, blocking=True, timeout=None) -> bool:
        return True

    def release(self, n=1) -> None:
        return

    def locked(self) -> bool:
        return False
