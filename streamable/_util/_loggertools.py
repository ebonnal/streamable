import logging
from typing import Optional

_logger: Optional[logging.Logger] = None


def get_logger() -> logging.Logger:
    global _logger
    if not _logger:
        _logger = logging.getLogger("streamable")
        _logger.propagate = False
        _handler = logging.StreamHandler()
        _formatter = logging.Formatter("%(asctime)s: %(levelname)s: %(message)s")
        _handler.setFormatter(_formatter)
        _logger.addHandler(_handler)
        _logger.setLevel(logging.INFO)
    return _logger
