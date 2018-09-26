# -*- coding=utf-8 -*-
import logging

from .task import ReplicationTask

logger = logging.getLogger(__name__)

__all__ = ["replication_task_naming_schemas"]


def replication_task_naming_schemas(replication_task: ReplicationTask):
    return (
        set(periodic_snapshot_task.naming_schema
            for periodic_snapshot_task in replication_task.periodic_snapshot_tasks) |
        set(replication_task.also_include_naming_schema)
    )
