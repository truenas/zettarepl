# -*- coding=utf-8 -*-
import logging
import time
from unittest.mock import PropertyMock

logger = logging.getLogger(__name__)

__all__ = ["mock_name", "wait_replication_tasks_to_complete"]


def mock_name(mock, name):
    type(mock).name = PropertyMock(return_value=name)
    return mock


def wait_replication_tasks_to_complete(zettarepl, timeout=300):
    for i in range(timeout):
        if not zettarepl.running_tasks and not zettarepl.pending_tasks:
            return

        time.sleep(1)

    raise TimeoutError()
