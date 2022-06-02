import os
import sys
import logging
import contextvars

error_stream = contextvars.ContextVar('error_stream', default=sys.stderr)


class _ColorStreamHandler(logging.StreamHandler):
    """On Windows, wrap stream with Colorama for ANSI style support."""

    def __init__(self) -> None:
        try:
            import colorama
        except ImportError:
            stream = None
        else:
            stream = colorama.AnsiToWin32(sys.stderr)

        super().__init__(stream)


class ErrorStreamHandler(logging.StreamHandler):
    def __init__(self, stream):
        super().__init__()
        self._stream = stream

    @property
    def stream(self):
        return self._stream.get()

    @stream.setter
    def stream(self, value):
        # ignore setting the stream
        return


default_handler = ErrorStreamHandler(error_stream)
default_handler.setFormatter(
    logging.Formatter("[%(asctime)s] %(levelname)s in %(module)s: %(message)s")
)


# Implementation borrowed from flask's logging implementation
def has_level_handler(logger: logging.Logger) -> bool:
    """Check if there is a handler in the logging chain that will handle the
    given logger's :meth:`effective level <~logging.Logger.getEffectiveLevel>`.
    """
    level = logger.getEffectiveLevel()
    current = logger

    while current:
        if any(handler.level <= level for handler in current.handlers):
            return True

        if not current.propagate:
            break

        current = current.parent  # type: ignore

    return False


def create_logger(app) -> logging.Logger:
    """Get the Swill logger and configure it"""

    logger = logging.getLogger(app.name)

    if app.debug and not logger.level:
        logger.setLevel(logging.DEBUG)

    if not has_level_handler(logger):
        logger.addHandler(default_handler)

    return logger


_logger = logging.getLogger("swill")
_logger.setLevel(logging.getLevelName(os.getenv('SWILL_LOG_LEVEL', 'INFO')))
_internal_handler = _ColorStreamHandler()
_internal_handler.setFormatter(
    logging.Formatter("[%(asctime)s] %(levelname)s in %(module)s: %(message)s")
)

if not has_level_handler(_logger):
    _logger.addHandler(_internal_handler)
