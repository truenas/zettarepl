# -*- coding=utf-8 -*-
from datetime import datetime
import logging

from zettarepl.snapshot.name import parse_snapshots_names_with_multiple_schemas, parsed_snapshot_sort_key
from zettarepl.snapshot.list import list_snapshots

from .task.naming_schema import replication_task_naming_schemas

logger = logging.getLogger(__name__)

__all__ = ["get_most_recent_snapshot"]


def get_most_recent_snapshot(src_snapshots, replication_task, src_shell, src_dataset):
    if replication_task.name_pattern:
        return get_most_recent_snapshot_with_name_pattern(
            replication_task, src_shell, src_dataset,
        )
    else:
        return get_most_recent_snapshot_with_naming_schemas(
            src_snapshots, replication_task,
        )


def get_most_recent_snapshot_with_name_pattern(replication_task, src_shell, src_dataset):
    for snapshot in reversed(list_snapshots(src_shell, src_dataset, False, "createtxg")):
        if replication_task.name_pattern.match(snapshot.name):
            return snapshot.name


def get_most_recent_snapshot_with_naming_schemas(src_snapshots, replication_task):
    naming_schemas = replication_task_naming_schemas(replication_task)

    parsed_src_snapshots = parse_snapshots_names_with_multiple_schemas(src_snapshots, naming_schemas)
    if not parsed_src_snapshots:
        return

    return sorted(parsed_src_snapshots, key=parsed_snapshot_sort_key)[-1].name
