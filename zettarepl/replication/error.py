# -*- coding=utf-8 -*-
import logging

logger = logging.getLogger(__name__)

__all__ = ["ReplicationError", "RecoverableReplicationError", "StuckReplicationError"]


class ReplicationError(Exception):
    pass


class RecoverableReplicationError(ReplicationError):
    pass


class StuckReplicationError(RecoverableReplicationError):
    pass
