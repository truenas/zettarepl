# -*- coding=utf-8 -*-
import logging

from zettarepl.dataset.mountpoint import dataset_mountpoints
from zettarepl.dataset.mtab import Mtab
from zettarepl.snapshot.list import list_snapshots
from zettarepl.snapshot.name import parse_snapshots_names
from zettarepl.transport.interface import Transport
from zettarepl.transport.shell.local import LocalShell

from .task import ReplicationTask

logger = logging.getLogger(__name__)

__all__ = []


def run_push_replication_tasks(transport: Transport, replication_tasks: [ReplicationTask]):
    replication_tasks = sorted(replication_tasks, key=lambda replication_task: (
        replication_task.source_dataset,
        not replication_task.recursive,
    ))

    local_shell = LocalShell()
    shell = transport.create_shell()
    mtab = Mtab(shell)
    for replication_task in replication_tasks:
        src_mountpoint = dataset_mountpoints(
            local_shell, replication_task.source_dataset, False, [], mtab)[replication_task.source_dataset]
        dst_mountpoint = dataset_mountpoints(
            shell, replication_task.target_dataset, False, [], mtab)[replication_task.target_dataset]

        src_snapshots = list_snapshots(local_shell, src_mountpoint)
        dst_snapshots = list_snapshots(local_shell, dst_mountpoint)

        snapshot = get_snapshot_to_send(replication_task, replication_task.source_dataset, src_snapshots)
        if snapshot is None:
            continue

        common_snapshots = set(src_snapshots) & set(dst_snapshots)
        incremental_base = sorted(common_snapshots, reverse=True)[0] if common_snapshots else None
        if incremental_base is None and not replication_task.allow_from_scratch:
            logger.warning("No incremental base for replication task %r and replication from scratch is not allowed",
                           replication_task.id)
            continue

        receive_resume_token = shell.exec(
            ["zfs", "get", "-H", "receive_resume_token", replication_task.target_dataset]).split("\t")[2]
        receive_resume_token = None if receive_resume_token == "-" else receive_resume_token

        transport.push_snapshot(shell, replication_task.source_dataset, replication_task.target_dataset,
                                snapshot, incremental_base, receive_resume_token)


def get_snapshot_to_send(replication_task, source_dataset, snapshots):
    if replication_task.restrict_schedule:
        for parsed_snapshot in sorted(parse_snapshots_names(snapshots, replication_task.naming_schema),
                                      key=lambda parsed_snapshot: parsed_snapshot.datetime,
                                      reverse=True):
            if replication_task.restrict_schedule.should_run(parsed_snapshot.datetime):
                return parsed_snapshot.name

        logger.warning("Could not find snapshot for dataset %r that matches naming schema %r and restrict schedule %r",
                       source_dataset, replication_task.naming_schema, replication_task.restrict_schedule)
        return None
    else:
        try:
            return sorted(snapshots, reverse=True)[0]
        except IndexError:
            logger.warning("Dataset %r does not have snapshots", source_dataset)
            return None
