# -*- coding=utf-8 -*-
import logging

from zettarepl.dataset.mountpoint import dataset_mountpoints
from zettarepl.dataset.mtab import Mtab
from zettarepl.snapshot.list import list_snapshots
from zettarepl.snapshot.name import parse_snapshots_names_with_multiple_schemas
from zettarepl.transport.interface import Transport
from zettarepl.transport.local import LocalShell

from .task.task import ReplicationTask

logger = logging.getLogger(__name__)

__all__ = ["run_push_replication_tasks"]


class NoIncrementalBaseException(Exception):
    pass


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
        dst_snapshots = list_snapshots(shell, dst_mountpoint)

        try:
            incremental_base, snapshots = get_snapshots_to_send(src_snapshots, dst_snapshots, replication_task)
        except NoIncrementalBaseException:
            logger.warning("No incremental base for replication task %r and replication from scratch is not allowed",
                           replication_task.id)
            continue

        if not snapshots:
            continue

        receive_resume_token = shell.exec(
            ["zfs", "get", "-H", "receive_resume_token", replication_task.target_dataset]).split("\t")[2]
        receive_resume_token = None if receive_resume_token == "-" else receive_resume_token

        for snapshot in snapshots:
            transport.push_snapshot(shell, replication_task.source_dataset, replication_task.target_dataset,
                                    snapshot, replication_task.recursive, incremental_base, receive_resume_token)
            incremental_base = snapshot
            receive_resume_token = None


def get_snapshots_to_send(src_snapshots, dst_snapshots, replication_task):
    naming_schemas = (set(periodic_snapshot_task.naming_schema
                          for periodic_snapshot_task in replication_task.periodic_snapshot_tasks) |
                      set(replication_task.also_include_naming_schema))

    parsed_src_snapshots = parse_snapshots_names_with_multiple_schemas(src_snapshots, naming_schemas)
    parsed_dst_snapshots = parse_snapshots_names_with_multiple_schemas(dst_snapshots, naming_schemas)

    try:
        parsed_incremental_base = sorted(
            set(parsed_src_snapshots) & set(parsed_dst_snapshots),
            key=lambda parsed_snapshot: (parsed_snapshot.datetime, parsed_snapshot.name)
        )[-1]
        incremental_base = parsed_incremental_base.name
    except IndexError:
        parsed_incremental_base = None
        incremental_base = None

    if parsed_incremental_base is None:
        if dst_snapshots and not replication_task.allow_from_scratch:
            raise NoIncrementalBaseException()

    snapshots_to_send = [
        parsed_snapshot.name
        for parsed_snapshot in sorted(
            parsed_src_snapshots,
            key=lambda parsed_snapshot: (parsed_snapshot.datetime, parsed_snapshot.name)
        )
        if (
            (
                parsed_incremental_base is None or
                # is newer than incremental base
                (
                    parsed_snapshot.datetime == parsed_incremental_base.datetime and
                    parsed_snapshot.name > parsed_incremental_base.name
                ) or
                (
                    parsed_snapshot.datetime > parsed_incremental_base.datetime
                )
            ) and
            (
                replication_task.restrict_schedule is None or
                replication_task.restrict_schedule.should_run(parsed_snapshot.datetime)
            ) and
            (
                not replication_task.only_matching_schedule or
                replication_task.schedule.should_run(parsed_snapshot.datetime)
            )
        )
    ]

    return incremental_base, snapshots_to_send
