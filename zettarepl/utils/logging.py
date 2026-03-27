# -*- coding=utf-8 -*-
import logging
import os
import re
from typing import Any, MutableMapping

logger = logging.getLogger(__name__)

__all__ = ["LongStringsFilter", "ReplicationTaskLoggingLevelFilter", "logging_record_replication_task",
           "PrefixLoggerAdapter"]


class LongStringsFilter(logging.Filter):
    def __init__(self, name: str = "") -> None:
        super().__init__(name)

        self.max_string_length: int = int(os.environ.get("LOGGING_MAX_STRING_LENGTH", "512"))

    def filter(self, record: logging.LogRecord) -> bool:
        record.args = self._process(record.args)
        return True

    def _process(self, value: Any) -> Any:
        if isinstance(value, dict):
            return {k: self._process(v) for k, v in value.items()}

        if isinstance(value, list):
            return list(map(self._process, value))

        if isinstance(value, tuple):
            return tuple(map(self._process, value))

        if self.max_string_length:
            placeholder: bytes | str
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
    levels: dict[str, int] = {}

    def __init__(self, default_level: int = logging.NOTSET) -> None:
        self.default_level = default_level
        super().__init__()

    def filter(self, record: logging.LogRecord) -> bool:
        task_id = logging_record_replication_task(record)
        if task_id is not None:
            if task_id in self.levels:
                if self.levels[task_id] != logging.NOTSET:
                    return record.levelno >= self.levels[task_id]
            else:
                logger.debug("I don't have logging level for task %r", task_id)

        return record.levelno >= self.default_level


def logging_record_replication_task(record: logging.LogRecord) -> str | None:
    m1 = re.match(r"replication_task__([^.]+)", record.threadName or "")
    m2 = re.match(r"zettarepl\.paramiko\.replication_task__([^.]+)", record.name)
    if m1:
        return m1.group(1)
    elif m2:
        return m2.group(1)

    return None


class PrefixLoggerAdapter[T: logging.Logger | logging.LoggerAdapter[Any]](logging.LoggerAdapter[T]):
    def __init__(self, logger: T, prefix: str) -> None:
        super().__init__(logger, {"prefix": prefix})

    def process(self, msg: str, kwargs: MutableMapping[str, Any]) -> tuple[str, MutableMapping[str, str]]:
        return f"[{self.extra['prefix']}] {msg}", kwargs  # type: ignore[index]
