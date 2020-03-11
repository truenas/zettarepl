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

    datasets__allow_empty = defaultdict(list)
    datasets__snapshots = defaultdict(list)
    for task, snapshot_name in tasks_with_snapshot_names:
        for snapshot in get_task_snapshots(datasets, task, snapshot_name):
            datasets__allow_empty[snapshot.dataset].append(task.allow_empty)
            datasets__snapshots[snapshot.dataset].append(snapshot)

    empty_snapshots = []
    for dataset in [dataset for dataset, allow_empty in datasets__allow_empty.items() if not any(allow_empty)]:
        try:
            if all(is_empty_snapshot(shell, snapshot) for snapshot in datasets__snapshots[dataset]):
                empty_snapshots.extend(datasets__snapshots[dataset])
        except ExecException as e:
            logger.warning("Failed to check if snapshots for dataset %r are empty, assuming they are is not. Error: %r",
                           dataset, e)

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
