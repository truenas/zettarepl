# -*- coding=utf-8 -*-
from collections import defaultdict
from datetime import datetime
import logging

from zettarepl.retention.calculate import calculate_snapshots_to_remove
from zettarepl.retention.snapshot_owner import SnapshotOwner
from zettarepl.utils.datetime import idealized_datetime

from .name import *
from .snapshot import Snapshot
from .task.task import PeriodicSnapshotTask

logger = logging.getLogger(__name__)

__all__ = ["calculate_periodic_snapshot_tasks_retention"]


class PeriodicSnapshotTaskSnapshotOwner(SnapshotOwner):
    def __init__(self, idealized_now: datetime, periodic_snapshot_task: PeriodicSnapshotTask):
        self.idealized_now = idealized_now
        self.periodic_snapshot_task = periodic_snapshot_task

    def owns(self, parsed_snapshot_name: ParsedSnapshotName):
        return self.periodic_snapshot_task.schedule.should_run(parsed_snapshot_name.datetime)

    def should_retain(self, parsed_snapshot_name: ParsedSnapshotName):
        delete_before = self.idealized_now - self.periodic_snapshot_task.lifetime
        return idealized_datetime(parsed_snapshot_name.datetime) >= delete_before


def calculate_periodic_snapshot_tasks_retention(now: datetime, tasks: [PeriodicSnapshotTask], snapshots: [Snapshot]):
    idealized_now = idealized_datetime(now)

    dataset__naming_schema__tasks = defaultdict(lambda: defaultdict(list))
    for task in tasks:
        dataset__naming_schema__tasks[task.dataset][task.naming_schema].append(task)

    result = []
    for dataset, naming_schema__tasks in dataset__naming_schema__tasks.items():
        dataset_snapshots_names = [snapshot.name for snapshot in snapshots if snapshot.dataset == dataset]
        for naming_schema, tasks in naming_schema__tasks.items():
            owners = [PeriodicSnapshotTaskSnapshotOwner(idealized_now, task) for task in tasks]
            result.extend([
                Snapshot(dataset, snapshot_name)
                for snapshot_name in calculate_snapshots_to_remove(naming_schema, owners, dataset_snapshots_names)
            ])

    return result
