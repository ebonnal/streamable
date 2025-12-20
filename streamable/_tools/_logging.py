import logging
import time
from typing import Optional

_logger: Optional[logging.Logger] = None


def logfmt_str_escape(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def get_logger() -> logging.Logger:
    global _logger
    if not _logger:
        _logger = logging.getLogger("streamable")
        _logger.propagate = False
        if not _logger.handlers:
            _handler = logging.StreamHandler()
            _formatter = logging.Formatter(
                "%(asctime)s %(levelname)s %(message)s", "%Y-%m-%dT%H:%M:%SZ"
            )
            _formatter.converter = time.gmtime
            _handler.setFormatter(_formatter)
            _logger.addHandler(_handler)
            _logger.setLevel(logging.INFO)
    return _logger
