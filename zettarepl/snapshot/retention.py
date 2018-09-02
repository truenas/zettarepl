# -*- coding=utf-8 -*-
from collections import defaultdict
from datetime import datetime
import logging

from zettarepl.utils.datetime import idealized_datetime

from . import Snapshot
from .name import parse_snapshots_names
from .task import PeriodicSnapshotTask

logger = logging.getLogger(__name__)

__all__ = ["calculate_periodic_snapshot_tasks_retention"]


def calculate_periodic_snapshot_tasks_retention(now: datetime, tasks: [PeriodicSnapshotTask], snapshots: [Snapshot]):
    idealized_now = idealized_datetime(now)

    dataset__naming_schema__tasks = defaultdict(lambda: defaultdict(list))
    for task in tasks:
        dataset__naming_schema__tasks[task.dataset][task.naming_schema].append(task)

    result = []
    for dataset, naming_schema__tasks in dataset__naming_schema__tasks.items():
        dataset_snapshots_names = [snapshot.name for snapshot in snapshots if snapshot.dataset == dataset]
        for naming_schema, tasks in naming_schema__tasks.items():
            for parsed_snapshot_name in parse_snapshots_names(dataset_snapshots_names, naming_schema):
                owners = [
                    task
                    for task in tasks
                    if task.schedule.should_run(parsed_snapshot_name.datetime)
                ]
                if owners and all(idealized_datetime(parsed_snapshot_name.datetime) < idealized_now - owner.lifetime
                                  for owner in owners):
                    result.append(Snapshot(dataset, parsed_snapshot_name.name))

    return result
