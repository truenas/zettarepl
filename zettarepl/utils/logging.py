# -*- coding=utf-8 -*-
import logging
import os

logger = logging.getLogger(__name__)

__all__ = ["LoggingLevelContext", "LongStringsFilter"]


class LoggingLevelContext:
    def __init__(self, level):
        self.level = level
        self.prev_level = None

    def __enter__(self):
        self.prev_level = logging.getLogger().level

        if self.level != logging.NOTSET:
            logger.debug("Setting new logging level: %r", self.level)
            logging.getLogger().setLevel(self.level)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.prev_level is not None:
            logging.getLogger().setLevel(self.prev_level)


class LongStringsFilter(logging.Filter):
    def __init__(self, name=""):
        super().__init__(name)

        self.max_string_length = int(os.environ.get("LOGGING_MAX_STRING_LENGTH", "64"))

    def filter(self, record):
        record.args = self._process(record.args)
        return True

    def _process(self, value):
        if isinstance(value, dict):
            return {k: self._process(v) for k, v in value.items()}

        if isinstance(value, list):
            return list(map(self._process, value))

        if isinstance(value, tuple):
            return tuple(map(self._process, value))

        if self.max_string_length:
            if isinstance(value, bytes):
                placeholder = b"...."
            elif isinstance(value, str):
                placeholder = "...."
            else:
                return value

            if len(value) <= self.max_string_length:
                return value

            return (
                value[:int((self.max_string_length - 4) / 2)] +
                placeholder +
                value[-int((self.max_string_length - 4) / 2):]
            )

        return value
