# -*- coding=utf-8 -*-
import logging

from zettarepl.snapshot.name import ParsedSnapshotName

logger = logging.getLogger(__name__)

__all__ = ["SnapshotOwner"]


class SnapshotOwner:
    def get_naming_schemas(self) -> [str]:
        raise NotImplementedError

    def owns_dataset(self, dataset: str):
        raise NotImplementedError

    def owns_snapshot(self, parsed_snapshot_name: ParsedSnapshotName):
        raise NotImplementedError

    def wants_to_delete(self):
        raise NotImplementedError()

    def should_retain(self, dataset: str, parsed_snapshot_name: ParsedSnapshotName):
        raise NotImplementedError
