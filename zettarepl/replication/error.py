# -*- coding=utf-8 -*-
import logging

logger = logging.getLogger(__name__)

__all__ = ["ReplicationError", "ReplicationConfigurationError", "RecoverableReplicationError",
           "NoIncrementalBaseReplicationError", "StuckReplicationError", "ContainsPartiallyCompleteState"]


class ReplicationError(Exception):
    pass


class ReplicationConfigurationError(ReplicationError):
    pass


class RecoverableReplicationError(ReplicationError):
    pass


class NoIncrementalBaseReplicationError(ReplicationError):
    pass


class StuckReplicationError(RecoverableReplicationError):
    pass


class ContainsPartiallyCompleteState(ReplicationError):
    pass
