# -*- coding=utf-8 -*-
import itertools
import logging
import time

from .error import ContainsPartiallyCompleteState

logger = logging.getLogger(__name__)

__all__ = ["retry_contains_partially_complete_state"]


def retry_contains_partially_complete_state(func):
    for i in itertools.count(1):
        try:
            return func()
        except ContainsPartiallyCompleteState:
            logger.warning(
                "Specified receive_resume_token, but received an error: contains partially-complete state. Allowing "
                "ZFS to catch up"
            )
            if i >= 60:
                raise

            time.sleep(60)
