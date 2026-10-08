# -*- coding=utf-8 -*-
from __future__ import annotations

import logging

from zettarepl.observer import ReplicationTaskLog
from zettarepl.utils.logging import logging_record_replication_task

from .client import MiddlewareClient

__all__ = ["ReplicationTaskLogHandler"]


class ReplicationTaskLogHandler(logging.Handler):
    def __init__(self, client: MiddlewareClient) -> None:
        self.client = client
        super().__init__()

    def emit(self, record: logging.LogRecord) -> None:
        replication_task_id = logging_record_replication_task(record)
        if replication_task_id is not None:
            self.client.notify(ReplicationTaskLog(replication_task_id, self.format(record)))
