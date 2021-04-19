# -*- coding=utf-8 -*-
import itertools
import logging
import time

from zettarepl.transport.interface import ExecException

from .error import StuckReplicationError

logger = logging.getLogger(__name__)

__all__ = ["retry_stuck_replication"]


def retry_stuck_replication(func, recoverable_error):
    for i in itertools.count(1):
        try:
            return func()
        except ExecException as e:
            if (
                isinstance(recoverable_error, StuckReplicationError) and
                "contains partially-complete state" in e.stdout
            ):
                logger.warning(
                    "After killing stuck replication destination contains partially-complete state, allowing ZFS "
                    "to catch up"
                )
                if i >= 60:
                    raise

                time.sleep(60)
            else:
                raise
