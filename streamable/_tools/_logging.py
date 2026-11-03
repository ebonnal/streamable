import logging
import time


def logfmt_str_escape(value: str) -> str:
    escaped = (
        " ".join(value.splitlines())
        .replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\t", " ")
    )
    return (
        f'"{escaped}"'
        if not escaped or escaped != value or " " in escaped or "=" in escaped
        else escaped
    )


def setup_logger() -> None:
    logger = logging.getLogger("streamable")
    logger.propagate = False
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s %(message)s", "%Y-%m-%dT%H:%M:%SZ"
        )
        formatter.converter = time.gmtime
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
