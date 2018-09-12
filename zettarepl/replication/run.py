# -*- coding=utf-8 -*-
from collections import namedtuple
import logging
import os

from zettarepl.dataset.mountpoint import dataset_mountpoints
from zettarepl.dataset.mtab import Mtab
from zettarepl.replication.task.direction import ReplicationDirection
from zettarepl.snapshot.list import list_snapshots
from zettarepl.snapshot.name import parse_snapshots_names_with_multiple_schemas
from zettarepl.transport.interface import Transport
from zettarepl.transport.local import LocalShell
from zettarepl.transport.zfscli import get_receive_resume_token

from .monitor import ReplicationMonitor
from .process_runner import ReplicationProcessRunner
from .task.task import ReplicationTask

logger = logging.getLogger(__name__)

__all__ = ["run_replication_tasks"]

ReplicationContext = namedtuple("ReplicationContext", ["transport", "shell", "mtab"])
ReplicationOptions = namedtuple("ReplicationOptions", ["speed_limit"])


class NoIncrementalBaseException(Exception):
    pass


class ReplicationStepTemplate:
    def __init__(self, local_context: ReplicationContext, remote_context: ReplicationContext,
                 direction: ReplicationDirection, src_dataset: str, dst_dataset: str, recursive: bool,
                 options: ReplicationOptions):
        self.local_context = local_context
        self.remote_context = remote_context
        self.direction = direction

        if self.direction == ReplicationDirection.PUSH:
            self.src_context = self.local_context
            self.dst_context = remote_context
        elif self.direction == ReplicationDirection.PULL:
            self.src_context = remote_context
            self.dst_context = local_context
        else:
            raise ValueError(f"Invalid replication direction: {self.direction.direction!r}")

        self.src_dataset = src_dataset
        self.dst_dataset = dst_dataset
        self.recursive = recursive

        self.options = options

    def instantiate(self, **kwargs):
        return ReplicationStep(self.local_context, self.remote_context,
                               self.direction, self.src_dataset, self.dst_dataset, self.recursive,
                               self.options, **kwargs)


class ReplicationStep(ReplicationStepTemplate):
    def __init__(self, *args, snapshot=None, incremental_base=None, receive_resume_token=None):
        super().__init__(*args)

        self.snapshot = snapshot
        self.incremental_base = incremental_base
        self.receive_resume_token = receive_resume_token
        if self.receive_resume_token is None:
            assert self.snapshot is not None
        else:
            assert self.snapshot is None
            assert self.incremental_base is None


def run_replication_tasks(local_shell: LocalShell, transport: Transport, replication_tasks: [ReplicationTask]):
    replication_tasks = sorted(replication_tasks, key=lambda replication_task: (
        replication_task.source_dataset,
        not replication_task.recursive,
    ))

    local_mtab = Mtab(local_shell)
    remote_shell = transport.shell(transport)
    remote_mtab = Mtab(remote_shell)
    for replication_task in replication_tasks:
        run_replication_task(replication_task,
                             ReplicationContext(None, local_shell, local_mtab),
                             ReplicationContext(transport, remote_shell, remote_mtab))


def run_replication_task(replication_task: ReplicationTask, local_context: ReplicationContext,
                         remote_context: ReplicationContext):
    if replication_task.direction == ReplicationDirection.PUSH:
        src_context = local_context
        dst_context = remote_context
    elif replication_task.direction == ReplicationDirection.PULL:
        src_context = remote_context
        dst_context = local_context
    else:
        raise ValueError(f"Invalid replication direction: {replication_task.direction!r}")

    list_datasets_recursive = (
        # Will have to send individual datasets non-recursively so we need a list of them
        replication_task.recursive and replication_task.exclude
    )

    src_mountpoints = dataset_mountpoints(
        src_context.shell, replication_task.source_dataset,
        list_datasets_recursive, replication_task.exclude,
        src_context.mtab)
    dst_mountpoints = dataset_mountpoints(
        dst_context.shell, replication_task.target_dataset,
        list_datasets_recursive, [],
        dst_context.mtab)

    replicate = [(replication_task.source_dataset, replication_task.target_dataset, replication_task.recursive)]
    if replication_task.recursive and replication_task.exclude:
        replicate = [(src_dataset, get_target_dataset(replication_task, src_dataset), False)
                     for src_dataset in src_mountpoints.keys()]

    options = ReplicationOptions(replication_task.speed_limit)

    for src_dataset, dst_dataset, recursive in replicate:
        if dst_dataset in dst_mountpoints:
            receive_resume_token = get_receive_resume_token(dst_context.shell, dst_dataset)

            if receive_resume_token is not None:
                logger.info("Resuming replication for dst_dataset %r", dst_dataset)
                run_replication_step(
                    ReplicationStep(local_context, remote_context, replication_task.direction, src_dataset, dst_dataset,
                                    recursive, options, receive_resume_token=receive_resume_token))

    for src_dataset, dst_dataset, recursive in replicate:
        src_mountpoint = src_mountpoints[src_dataset]
        src_snapshots = list_snapshots(src_context.shell, src_mountpoint)

        dst_snapshots = []
        if dst_dataset in dst_mountpoints:
            dst_snapshots = list_snapshots(dst_context.shell, dst_mountpoints[dst_dataset])

        try:
            incremental_base, snapshots = get_snapshots_to_send(src_snapshots, dst_snapshots, replication_task)
        except NoIncrementalBaseException:
            logger.warning("No incremental base for replication task %r on dataset %r and replication from scratch "
                           "is not allowed", replication_task.id, src_dataset)
            continue

        if not snapshots:
            logger.info("No snapshots to send for replication task %r on dataset %r", replication_task.id, src_dataset)
            continue

        step_template = ReplicationStepTemplate(
            local_context, remote_context, replication_task.direction, src_dataset, dst_dataset, recursive, options)
        replicate_snapshots(step_template, incremental_base, snapshots)


def replicate_snapshots(step_template: ReplicationStepTemplate, incremental_base, snapshots):
    for snapshot in snapshots:
        run_replication_step(step_template.instantiate(incremental_base=incremental_base, snapshot=snapshot))
        incremental_base = snapshot


def run_replication_step(step: ReplicationStep):
    process = step.dst_context.transport.replication_process(
        step.local_context.shell, step.remote_context.shell, step.direction, step.src_dataset, step.dst_dataset,
        step.snapshot, step.recursive, step.incremental_base, step.receive_resume_token, step.options.speed_limit)
    process.run()

    monitor = ReplicationMonitor(step.remote_context.shell, step.dst_dataset)
    ReplicationProcessRunner(process, monitor).run()


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


def get_target_dataset(replication_task, src_dataset):
    return os.path.normpath(
        os.path.join(replication_task.target_dataset, os.path.relpath(src_dataset, replication_task.source_dataset)))
