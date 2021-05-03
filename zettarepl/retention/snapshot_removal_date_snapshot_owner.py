# -*- coding=utf-8 -*-
from datetime import datetime
import logging

from zettarepl.snapshot.name import ParsedSnapshotName

from .snapshot_owner import SnapshotOwner

logger = logging.getLogger(__name__)

__all__ = ["SnapshotRemovalDateSnapshotOwner"]


class SnapshotRemovalDateSnapshotOwner(SnapshotOwner):
    def __init__(self, now: datetime, removal_dates):
        self.now = now
        self.removal_dates = removal_dates
        self.datasets = {snapshot.split("@", 1)[0] for snapshot in self.removal_dates.keys()}

    def get_naming_schemas(self) -> [str]:
        return [None]

    def owns_dataset(self, dataset: str):
        return dataset in self.datasets

    def owns_snapshot(self, dataset: str, parsed_snapshot_name: ParsedSnapshotName):
        return f"{dataset}@{parsed_snapshot_name.name}" in self.removal_dates

    def wants_to_delete(self):
        return True

    def should_retain(self, dataset: str, parsed_snapshot_name: ParsedSnapshotName):
        return self.removal_dates[f"{dataset}@{parsed_snapshot_name.name}"].replace(tzinfo=None) > self.now

    def __repr__(self):
        return f"<{self.__class__.__name__} {len(self.removal_dates)}>"
