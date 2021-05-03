# -*- coding=utf-8 -*-
from datetime import datetime
import logging

from zettarepl.dataset.relationship import belongs_to_tree
from zettarepl.retention.snapshot_owner import SnapshotOwner
from zettarepl.snapshot.name import *
from zettarepl.utils.datetime import idealized_datetime

from .task import PeriodicSnapshotTask

logger = logging.getLogger(__name__)

__all__ = ["PeriodicSnapshotTaskSnapshotOwner"]


class PeriodicSnapshotTaskSnapshotOwner(SnapshotOwner):
    def __init__(self, now: datetime, periodic_snapshot_task: PeriodicSnapshotTask):
        self.idealized_now = idealized_datetime(now)
        self.periodic_snapshot_task = periodic_snapshot_task

    def get_naming_schemas(self):
        return [self.periodic_snapshot_task.naming_schema]

    def owns_dataset(self, dataset: str):
        return belongs_to_tree(dataset, self.periodic_snapshot_task.dataset, self.periodic_snapshot_task.recursive,
                               self.periodic_snapshot_task.exclude)

    def owns_snapshot(self, dataset: str, parsed_snapshot_name: ParsedSnapshotName):
        return self.periodic_snapshot_task.schedule.should_run(parsed_snapshot_name.datetime)

    def wants_to_delete(self):
        return True

    def should_retain(self, dataset: str, parsed_snapshot_name: ParsedSnapshotName):
        delete_before = self.idealized_now - self.periodic_snapshot_task.lifetime
        return idealized_datetime(parsed_snapshot_name.datetime) >= delete_before

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.periodic_snapshot_task.id!r}>"
