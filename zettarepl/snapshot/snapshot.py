# -*- coding=utf-8 -*-
from collections import namedtuple
import logging

logger = logging.getLogger(__name__)

__all__ = ["Snapshot"]


class Snapshot(namedtuple("Snapshot", ["dataset", "name"])):
    def __str__(self) -> str:
        return f"{self.dataset}@{self.name}"
