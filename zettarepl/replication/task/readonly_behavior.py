# -*- coding=utf-8 -*-
import enum
import logging

logger = logging.getLogger(__name__)

__all__ = ["ReadOnlyBehavior"]


class ReadOnlyBehavior(enum.Enum):
    IGNORE = "ignore"
    SET = "set"
    REQUIRE = "require"
