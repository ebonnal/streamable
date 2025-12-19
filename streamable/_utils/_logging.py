import logging
import time
from typing import Optional

_logger: Optional[logging.Logger] = None


class SubjectEscapingFormatter(logging.Formatter):
    @staticmethod
    def _escape(value: str) -> str:
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'

    def format(self, record: logging.LogRecord) -> str:
        record.subject = self._escape(getattr(record, "subject"))
        return super().format(record)


def get_logger() -> logging.Logger:
    global _logger
    if not _logger:
        _logger = logging.getLogger("streamable")
        _logger.propagate = False
        _handler = logging.StreamHandler()
        _formatter = SubjectEscapingFormatter(
            "%(asctime)s %(levelname)s stream=%(subject)s elapsed=%(elapsed)s errors=%(errors)s emissions=%(emissions)s",
            "%Y-%m-%dT%H:%M:%SZ",
        )
        _formatter.converter = time.gmtime
        _handler.setFormatter(_formatter)
        _logger.addHandler(_handler)
        _logger.setLevel(logging.INFO)
    return _logger
