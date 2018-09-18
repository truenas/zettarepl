# -*- coding=utf-8 -*-
import logging

logger = logging.getLogger(__name__)

__all__ = ["ReplicationError", "ReplicationConfigurationError", "RecoverableReplicationError", "StuckReplicationError"]


class ReplicationError(Exception):
    pass


class ReplicationConfigurationError(ReplicationError):
    pass


class RecoverableReplicationError(ReplicationError):
    pass


class StuckReplicationError(RecoverableReplicationError):
    pass
