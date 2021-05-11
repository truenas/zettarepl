# -*- coding=utf-8 -*-
import logging

from .dataset import get_target_dataset
from .task import ReplicationTask

logger = logging.getLogger(__name__)

__all__ = ["replication_tasks_source_datasets_queries", "replication_tasks_target_datasets_queries"]


def replication_tasks_source_datasets_queries(replication_tasks: [ReplicationTask]):
    return sum([
        [
            (source_dataset, replication_task.recursive)
            for source_dataset in replication_task.source_datasets
        ]
        for replication_task in replication_tasks
    ], [])


def replication_tasks_target_datasets_queries(replication_tasks: [ReplicationTask]):
    return sum(
        [
            [
                (get_target_dataset(replication_task, dataset), replication_task.recursive)
                for dataset in replication_task.source_datasets
            ]
            for replication_task in replication_tasks
        ],
        [],
    )
