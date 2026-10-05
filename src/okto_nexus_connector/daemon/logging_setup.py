"""Bounded, redacted operational logs for foreground and detached daemons."""
import logging
from logging.handlers import RotatingFileHandler

from ..redaction import redact_text


class SafeFormatter(logging.Formatter):
    def format(self, record):
        return redact_text(super().format(record))


def configure_logging(root):
    directory = root / 'logs'
    directory.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger('okto_nexus_connector')
    path = directory / 'daemon.log'
    for handler in logger.handlers:
        if getattr(handler, 'baseFilename', None) == str(path.resolve()):
            return handler
    handler = RotatingFileHandler(path, maxBytes=2 * 1024 * 1024,
                                  backupCount=3, encoding='utf-8')
    handler.setFormatter(SafeFormatter('%(asctime)s %(levelname)s %(name)s %(message)s'))
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    return handler
