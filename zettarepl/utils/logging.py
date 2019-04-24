# -*- coding=utf-8 -*-
import logging
import os
import re

logger = logging.getLogger(__name__)

__all__ = ["LongStringsFilter", "ReplicationTaskLoggingLevelFilter"]


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


class ReplicationTaskLoggingLevelFilter(logging.Filter):
    levels = {}

    def filter(self, record: logging.LogRecord):
        m1 = re.match("replication_task__([^.]+)", record.threadName)
        m2 = re.match("zettarepl\.paramiko\.replication_task__([^.]+)", record.name)
        if m1 or m2:
            if m1:
                task_id = m1.group(1)
            else:
                task_id = m2.group(1)

            if task_id in self.levels:
                return record.levelno >= self.levels[task_id]
            else:
                logger.debug("I don't have logging level for task %r", task_id)

        return True
