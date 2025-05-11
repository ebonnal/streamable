class WrappedError(Exception):
    def __init__(self, error: Exception):
        super().__init__(repr(error))
        self.error = error
