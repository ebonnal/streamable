import datetime
from typing import NamedTuple
from streamable._tools._logging import logfmt_str_escape


class Observation(NamedTuple):
    """
    Representation of the progress of iteration over a stream.

    Args:
        subject (``str``): Human-readable description of stream elements (e.g., "cats", "dogs", "requests").

        elapsed (``timedelta``): Time elapsed since iteration started.

        errors (``int``): Number of errors encountered during iteration so far.

        elements (``int``): Number of elements emitted so far.
    """

    subject: str
    elapsed: datetime.timedelta
    errors: int
    elements: int

    def __str__(self) -> str:
        """
        Return a logfmt-formatted string representation.

        Returns:
            ``str``: Logfmt string with subject, elapsed, errors, and elements.
        """
        subject = logfmt_str_escape(self.subject)
        elapsed = logfmt_str_escape(str(self.elapsed))
        return f"observed={subject} elapsed={elapsed} errors={self.errors} elements={self.elements}"
