# -*- coding=utf-8 -*-
from collections import defaultdict
import logging

from zettarepl.dataset.exclude import should_exclude
from zettarepl.dataset.list import list_datasets
from zettarepl.dataset.relationship import is_child
from zettarepl.snapshot.task.task import PeriodicSnapshotTask
from zettarepl.snapshot.snapshot import Snapshot
from zettarepl.transport.interface import ExecException, Shell

logger = logging.getLogger(__name__)

__all__ = ["get_empty_snapshots_for_deletion"]


def get_empty_snapshots_for_deletion(shell: Shell, tasks_with_snapshot_names: [(PeriodicSnapshotTask, str)]):
    datasets = list_datasets(shell)

    snapshots = defaultdict(list)
    for task, snapshot_name in tasks_with_snapshot_names:
        for snapshot in get_task_snapshots(datasets, task, snapshot_name):
            snapshots[snapshot].append(task.allow_empty)

    empty_snapshots = []
    for snapshot in [snapshot for snapshot, allow_empty in snapshots.items() if not any(allow_empty)]:
        try:
            if is_empty_snapshot(shell, snapshot):
                empty_snapshots.append(snapshot)
        except ExecException as e:
            logger.warning("Failed to check if snapshot %r is empty, assuming it is not. Error: %r", snapshot, e)

    return empty_snapshots


def get_task_snapshots(datasets: [str], task: PeriodicSnapshotTask, snapshot_name: str):
    if task.recursive:
        return [
            Snapshot(dataset, snapshot_name)
            for dataset in datasets
            if is_child(dataset, task.dataset) and not should_exclude(dataset, task.exclude)
        ]
    else:
        return [Snapshot(task.dataset, snapshot_name)]


def is_empty_snapshot(shell: Shell, snapshot: Snapshot):
    return shell.exec(["zfs", "get", "-H", "-o", "value", "written", str(snapshot)]).strip() == "0"
