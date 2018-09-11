# -*- coding=utf-8 -*-
import enum
import logging

logger = logging.getLogger(__name__)

__all__ = ["ReplicationDirection"]


class ReplicationDirection(enum.Enum):
    PUSH = "push"
    PULL = "pull"
