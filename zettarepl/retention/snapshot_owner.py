# -*- coding=utf-8 -*-
import logging

from zettarepl.snapshot.name import ParsedSnapshotName

logger = logging.getLogger(__name__)

__all__ = ["SnapshotOwner"]


class SnapshotOwner:
    def owns(self, parsed_snapshot_name: ParsedSnapshotName):
        raise NotImplementedError

    def should_retain(self, parsed_snapshot_name: ParsedSnapshotName):
        raise NotImplementedError
