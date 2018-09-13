# -*- coding=utf-8 -*-
from collections import namedtuple
import logging
import os

from zettarepl.dataset.mountpoint import dataset_mountpoints
from zettarepl.dataset.mtab import Mtab
from zettarepl.snapshot.destroy import destroy_snapshots
from zettarepl.snapshot.list import list_snapshots
from zettarepl.snapshot.name import parse_snapshots_names_with_multiple_schemas
from zettarepl.transport.interface import Transport
from zettarepl.transport.local import LocalShell
from zettarepl.transport.zfscli import get_receive_resume_token

from .monitor import ReplicationMonitor
from .process_runner import ReplicationProcessRunner
from .task.direction import ReplicationDirection
from .task.task import ReplicationTask

logger = logging.getLogger(__name__)

__all__ = ["run_replication_tasks"]

ReplicationContext = namedtuple("ReplicationContext", ["transport", "shell", "mtab"])


class ReplicationStepTemplate:
    def __init__(self, replication_task: ReplicationTask,
                 local_context: ReplicationContext, remote_context: ReplicationContext,
                 src_dataset: str, dst_dataset: str, recursive: bool):
        self.replication_task = replication_task
        self.local_context = local_context
        self.remote_context = remote_context

        if self.replication_task.direction == ReplicationDirection.PUSH:
            self.src_context = self.local_context
            self.dst_context = remote_context
        elif self.replication_task.direction == ReplicationDirection.PULL:
            self.src_context = remote_context
            self.dst_context = local_context
        else:
            raise ValueError(f"Invalid replication direction: {self.replication_task.direction.direction!r}")

        self.src_dataset = src_dataset
        self.dst_dataset = dst_dataset
        self.recursive = recursive

    def instantiate(self, **kwargs):
        return ReplicationStep(self.replication_task,
                               self.local_context, self.remote_context,
                               self.src_dataset, self.dst_dataset, self.recursive, **kwargs)


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
        # Recursive replication tasks go first
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
    src_mountpoints, dst_mountpoints = get_src_dst_mountpoints(replication_task, local_context, remote_context)

    src_datasets = src_mountpoints.keys()  # Order is right because `dataset_mountpoints` returns `OrderedDict`
    step_templates = calculate_replication_step_templates(replication_task, local_context, remote_context, src_datasets)

    resumed = resume_replications(step_templates, dst_mountpoints.keys())
    if resumed:
        _, dst_mountpoints = get_src_dst_mountpoints(replication_task, local_context, remote_context,
                                                     src_mountpoints=False)

    run_replication_steps(step_templates, src_mountpoints, dst_mountpoints)


def get_src_dst_context(replication_task: ReplicationTask, local_context: ReplicationContext,
                        remote_context: ReplicationContext):
    if replication_task.direction == ReplicationDirection.PUSH:
        src_context = local_context
        dst_context = remote_context
    elif replication_task.direction == ReplicationDirection.PULL:
        src_context = remote_context
        dst_context = local_context
    else:
        raise ValueError(f"Invalid replication direction: {replication_task.direction!r}")

    return src_context, dst_context


def get_src_dst_mountpoints(replication_task: ReplicationTask, local_context: ReplicationContext,
                            remote_context: ReplicationContext, src_mountpoints=True, dst_mountpoints=True):
    src_context, dst_context = get_src_dst_context(replication_task, local_context, remote_context)

    list_datasets_recursive = (
        # Will have to send individual datasets non-recursively so we need a list of them
        replication_task.recursive and replication_task.exclude
    )

    if src_mountpoints:
        src_mountpoints = dataset_mountpoints(
            src_context.shell, replication_task.source_dataset,
            list_datasets_recursive, replication_task.exclude,
            src_context.mtab)
    else:
        src_mountpoints = None

    if dst_mountpoints:
        dst_mountpoints = dataset_mountpoints(
            dst_context.shell, replication_task.target_dataset,
            list_datasets_recursive, [],
            dst_context.mtab)
    else:
        dst_mountpoints = None

    return src_mountpoints, dst_mountpoints


def calculate_replication_step_templates(replication_task: ReplicationTask, local_context: ReplicationContext,
                                         remote_context: ReplicationContext, src_datasets):
    replicate = [(replication_task.source_dataset, replication_task.target_dataset, replication_task.recursive)]
    if replication_task.recursive and replication_task.exclude:
        replicate = [(src_dataset, get_target_dataset(replication_task, src_dataset), False)
                     for src_dataset in src_datasets]

    return [ReplicationStepTemplate(replication_task, local_context, remote_context,
                                    src_dataset, dst_dataset, recursive)
            for src_dataset, dst_dataset, recursive in replicate]


def get_target_dataset(replication_task, src_dataset):
    return os.path.normpath(
        os.path.join(replication_task.target_dataset, os.path.relpath(src_dataset, replication_task.source_dataset)))


def resume_replications(step_templates: [ReplicationStepTemplate], dst_datasets):
    resumed = False
    for step_template in step_templates:
        if step_template.dst_dataset in dst_datasets:
            receive_resume_token = get_receive_resume_token(step_template.dst_context.shell, step_template.dst_dataset)

            if receive_resume_token is not None:
                logger.info("Resuming replication for dst_dataset %r", step_template.dst_dataset)
                run_replication_step(step_template.instantiate(receive_resume_token=receive_resume_token))
                resumed = True

    return resumed


def run_replication_steps(step_templates: [ReplicationStepTemplate], src_mountpoints, dst_mountpoints):
    for step_template in step_templates:
        src_mountpoint = src_mountpoints[step_template.src_dataset]
        src_snapshots = list_snapshots(step_template.src_context.shell, src_mountpoint)

        dst_snapshots = []
        if step_template.dst_dataset in dst_mountpoints:
            dst_snapshots = list_snapshots(step_template.dst_context.shell, dst_mountpoints[step_template.dst_dataset])

        incremental_base, snapshots = get_snapshots_to_send(src_snapshots, dst_snapshots,
                                                            step_template.replication_task)
        if incremental_base is None and dst_snapshots:
            if step_template.replication_task.allow_from_scratch:
                logger.warning("No incremental base for replication task %r on dataset %r, destroying all destination "
                               "snapshots", step_template.replication_task.id, step_template.src_dataset)
                destroy_snapshots(step_template.dst_context.shell, dst_snapshots)
            else:
                logger.warning("No incremental base for replication task %r on dataset %r and replication from scratch "
                               "is not allowed", step_template.replication_task.id, step_template.src_dataset)
                continue

        if not snapshots:
            logger.info("No snapshots to send for replication task %r on dataset %r", step_template.replication_task.id,
                        step_template.src_dataset)
            continue

        replicate_snapshots(step_template, incremental_base, snapshots)


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

    snapshots_to_send = [
        parsed_snapshot
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

    # Do not send something that will immediately be removed by retention policy
    will_be_removed = replication_task.retention_policy.calculate_delete_snapshots(snapshots_to_send, snapshots_to_send)
    snapshots_to_send = [parsed_snapshot.name
                         for parsed_snapshot in snapshots_to_send
                         if parsed_snapshot not in will_be_removed]

    return incremental_base, snapshots_to_send


def replicate_snapshots(step_template: ReplicationStepTemplate, incremental_base, snapshots):
    for snapshot in snapshots:
        run_replication_step(step_template.instantiate(incremental_base=incremental_base, snapshot=snapshot))
        incremental_base = snapshot


def run_replication_step(step: ReplicationStep):
    logger.info("For replication task %r: doing %s from %r to %r of snapshot %r recursive=%r incremental_base=%r "
                "receive_resume_token=%r", step.replication_task.id, step.replication_task.direction.value,
                step.src_dataset, step.dst_dataset, step.snapshot, step.recursive, step.incremental_base,
                step.receive_resume_token)

    process = step.dst_context.transport.replication_process(
        f"replication_task.{step.replication_task.id}",
        step.local_context.shell, step.remote_context.shell,
        step.replication_task.direction, step.src_dataset, step.dst_dataset,
        step.snapshot, step.recursive, step.incremental_base, step.receive_resume_token,
        step.replication_task.speed_limit)
    monitor = ReplicationMonitor(step.remote_context.shell, step.dst_dataset)
    ReplicationProcessRunner(process, monitor).run()
