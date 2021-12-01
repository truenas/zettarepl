# -*- coding=utf-8 -*-
from collections import namedtuple
from datetime import datetime
import logging

from zettarepl.snapshot.name import parse_snapshots_names_with_multiple_schemas, parsed_snapshot_sort_key
from zettarepl.snapshot.list import list_snapshots

from .task.naming_schema import replication_task_naming_schemas
from .task.should_replicate import replication_task_should_replicate_parsed_snapshot

logger = logging.getLogger(__name__)

__all__ = ["get_snapshots_to_send"]

SnapshotsToSend = namedtuple("SnapshotsToSend", ["incremental_base", "snapshots", "include_intermediate",
                                                 "empty_is_successful"])


def get_snapshots_to_send(src_snapshots, dst_snapshots, replication_task, src_shell, src_dataset):
    if replication_task.name_pattern:
        return get_snapshots_to_send_with_name_pattern(
            src_snapshots, dst_snapshots, replication_task, src_shell, src_dataset,
        )
    else:
        return get_snapshots_to_send_with_naming_schemas(
            src_snapshots, dst_snapshots, replication_task,
        )


def get_snapshots_to_send_with_name_pattern(src_snapshots, dst_snapshots, replication_task, src_shell, src_dataset):
    src_snapshots = [
        snapshot.name
        for snapshot in list_snapshots(src_shell, src_dataset, False, "createtxg")
    ]

    incremental_base = None
    snapshots_to_send = src_snapshots
    # Find the newest common snapshot and send the rest
    for i, snapshot in enumerate(src_snapshots):
        if snapshot in dst_snapshots:
            incremental_base = snapshot
            snapshots_to_send = src_snapshots[i + 1:]

    filtered_snapshots_to_send = list(filter(replication_task.name_pattern.match, snapshots_to_send))

    include_intermediate = False
    if snapshots_to_send == filtered_snapshots_to_send:
        if len(filtered_snapshots_to_send) > 1:
            if incremental_base is None:
                filtered_snapshots_to_send = [filtered_snapshots_to_send[0], filtered_snapshots_to_send[-1]]
            else:
                filtered_snapshots_to_send = [filtered_snapshots_to_send[-1]]

            include_intermediate = True

    return SnapshotsToSend(incremental_base, filtered_snapshots_to_send, include_intermediate, False)


def get_snapshots_to_send_with_naming_schemas(src_snapshots, dst_snapshots, replication_task):
    naming_schemas = replication_task_naming_schemas(replication_task)

    parsed_src_snapshots = parse_snapshots_names_with_multiple_schemas(src_snapshots, naming_schemas)
    parsed_dst_snapshots = parse_snapshots_names_with_multiple_schemas(dst_snapshots, naming_schemas)

    try:
        parsed_incremental_base = sorted(
            set(parsed_src_snapshots) & set(parsed_dst_snapshots),
            key=parsed_snapshot_sort_key,
        )[-1]
        incremental_base = parsed_incremental_base.name
    except IndexError:
        parsed_incremental_base = None
        incremental_base = None

    snapshots_to_send = [
        parsed_snapshot
        for parsed_snapshot in sorted(parsed_src_snapshots, key=parsed_snapshot_sort_key)
        if (
            (
                parsed_incremental_base is None or
                # is newer than incremental base
                parsed_snapshot != parsed_incremental_base and sorted(
                    [parsed_snapshot, parsed_incremental_base],
                    key=parsed_snapshot_sort_key
                )[0] == parsed_incremental_base
            ) and
            replication_task_should_replicate_parsed_snapshot(replication_task, parsed_snapshot)
        )
    ]

    # Do not send something that will immediately be removed by retention policy
    will_be_removed = replication_task.retention_policy.calculate_delete_snapshots(
        # We don't know what time it is, our best guess is newest snapshot datetime
        max([parsed_src_snapshot.datetime for parsed_src_snapshot in parsed_src_snapshots] or [datetime.max]),
        snapshots_to_send, snapshots_to_send)
    snapshots_to_send = [parsed_snapshot.name
                         for parsed_snapshot in snapshots_to_send
                         if parsed_snapshot not in will_be_removed]

    return SnapshotsToSend(incremental_base, snapshots_to_send, False, False)
