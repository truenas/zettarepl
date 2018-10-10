# -*- coding=utf-8 -*-
import logging

from zettarepl.dataset.relationship import belongs_to_tree
from zettarepl.snapshot.name import ParsedSnapshotName

from .dataset import get_target_dataset
from .task import ReplicationTask

logger = logging.getLogger(__name__)

__all__ = ["replication_task_should_replicate_dataset", "replication_task_replicates_target_dataset",
           "replication_task_should_replicate_parsed_snapshot"]


def replication_task_should_replicate_dataset(replication_task: ReplicationTask, dataset: str):
    return any(
        belongs_to_tree(dataset, source_dataset, replication_task.recursive,
                        replication_task.exclude)
        for source_dataset in replication_task.source_datasets
    )


def replication_task_replicates_target_dataset(replication_task: ReplicationTask, dataset: str):
    return belongs_to_tree(dataset, replication_task.target_dataset, replication_task.recursive,
                           [get_target_dataset(replication_task, exclude) for exclude in replication_task.exclude])


def replication_task_should_replicate_parsed_snapshot(replication_task: ReplicationTask,
                                                      parsed_snapshot: ParsedSnapshotName):
    return (
        (
            replication_task.restrict_schedule is None or
            replication_task.restrict_schedule.should_run(parsed_snapshot.datetime)
        ) and
        (
            not replication_task.only_matching_schedule or
            replication_task.schedule.should_run(parsed_snapshot.datetime)
        )
    )
