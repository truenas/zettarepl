# -*- coding=utf-8 -*-
from collections import OrderedDict
from datetime import datetime
import logging

from zettarepl.dataset.list import *
from zettarepl.observer import (notify, ReplicationTaskStart, ReplicationTaskSuccess, ReplicationTaskSnapshotProgress,
                                ReplicationTaskSnapshotSuccess, ReplicationTaskError)
from zettarepl.snapshot.destroy import destroy_snapshots
from zettarepl.snapshot.list import *
from zettarepl.snapshot.name import parse_snapshots_names_with_multiple_schemas, parsed_snapshot_sort_key
from zettarepl.snapshot.snapshot import Snapshot
from zettarepl.transport.interface import Shell, Transport
from zettarepl.transport.local import LocalShell
from zettarepl.transport.zfscli import get_receive_resume_token

from .error import *
from .monitor import ReplicationMonitor
from .process_runner import ReplicationProcessRunner
from .task.dataset import get_target_dataset
from .task.direction import ReplicationDirection
from .task.naming_schema import replication_task_naming_schemas
from .task.should_replicate import *
from .task.task import ReplicationTask

logger = logging.getLogger(__name__)

__all__ = ["run_replication_tasks"]


class ReplicationContext:
    def __init__(self, transport: Transport, shell: Shell):
        self.transport = transport
        self.shell = shell
        self.datasets = None


class ReplicationStepTemplate:
    def __init__(self, replication_task: ReplicationTask,
                 src_context: ReplicationContext, dst_context: ReplicationContext,
                 src_dataset: str, dst_dataset: str, recursive: bool):
        self.replication_task = replication_task
        self.src_context = src_context
        self.dst_context = dst_context
        self.src_dataset = src_dataset
        self.dst_dataset = dst_dataset
        self.recursive = recursive

    def instantiate(self, **kwargs):
        return ReplicationStep(self.replication_task,
                               self.src_context, self.dst_context,
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


def run_replication_tasks(local_shell: LocalShell, transport: Transport, remote_shell: Shell,
                          replication_tasks: [ReplicationTask], observer=None):
    replication_tasks_parts = calculate_replication_tasks_parts(replication_tasks)

    started_replication_tasks_ids = set()
    failed_replication_tasks_ids = set()
    replication_tasks_parts_left = {
        replication_task.id: len([1
                                  for another_replication_task, source_dataset in replication_tasks_parts
                                  if another_replication_task == replication_task])
        for replication_task in replication_tasks
    }
    for replication_task, source_dataset in replication_tasks_parts:
        if replication_task.id in failed_replication_tasks_ids:
            continue

        local_context = ReplicationContext(None, local_shell)
        remote_context = ReplicationContext(transport, remote_shell)

        if replication_task.direction == ReplicationDirection.PUSH:
            src_context = local_context
            dst_context = remote_context
        elif replication_task.direction == ReplicationDirection.PULL:
            src_context = remote_context
            dst_context = local_context
        else:
            raise ValueError(f"Invalid replication direction: {replication_task.direction!r}")

        if replication_task.id not in started_replication_tasks_ids:
            notify(observer, ReplicationTaskStart(replication_task.id))
            started_replication_tasks_ids.add(replication_task.id)
        recoverable_error = None
        for i in range(replication_task.retries):
            try:
                run_replication_task_part(replication_task, source_dataset, src_context, dst_context, observer)
                replication_tasks_parts_left[replication_task.id] -= 1
                if replication_tasks_parts_left[replication_task.id] == 0:
                    notify(observer, ReplicationTaskSuccess(replication_task.id))
                break
            except RecoverableReplicationError as recoverable_error:
                logger.warning("For task %r at attempt %d recoverable replication error %r", replication_task.id,
                               i + 1, recoverable_error)
            except ReplicationError as e:
                logger.error("For task %r non-recoverable replication error %r", replication_task.id, e)
                notify(observer, ReplicationTaskError(replication_task.id, str(e)))
                failed_replication_tasks_ids.add(replication_task.id)
                break
            except Exception as e:
                logger.error("For task %r unhandled replication error %r", replication_task.id, e, exc_info=True)
                notify(observer, ReplicationTaskError(replication_task.id, str(e)))
                failed_replication_tasks_ids.add(replication_task.id)
                break
        else:
            logger.error("Failed replication task %r after %d retries", replication_task.id,
                         replication_task.retries)
            notify(observer, ReplicationTaskError(replication_task.id, str(recoverable_error)))
            failed_replication_tasks_ids.add(replication_task.id)


def calculate_replication_tasks_parts(replication_tasks):
    return sorted(
        sum([
            [
                (replication_task, source_dataset)
                for source_dataset in replication_task.source_datasets
            ]
            for replication_task in replication_tasks
        ], []),
        key=lambda replication_task__source_dataset: (
            replication_task__source_dataset[1],
            # Recursive replication tasks go first
            0 if replication_task__source_dataset[0].recursive else 1,
        )
    )


def run_replication_task_part(replication_task: ReplicationTask, source_dataset: str,
                              src_context: ReplicationContext, dst_context: ReplicationContext, observer=None):
    step_templates = calculate_replication_step_templates(replication_task, source_dataset,
                                                          src_context, dst_context)

    resumed = resume_replications(step_templates, observer)
    if resumed:
        step_templates = calculate_replication_step_templates(replication_task, source_dataset,
                                                              src_context, dst_context)

    run_replication_steps(step_templates, observer)


def calculate_replication_step_templates(replication_task: ReplicationTask, source_dataset: str,
                                         src_context: ReplicationContext, dst_context: ReplicationContext):
    src_context.datasets = list_datasets_with_snapshots(src_context.shell, source_dataset,
                                                        replication_task.recursive)
    dst_context.datasets = list_datasets_with_snapshots(dst_context.shell, replication_task.target_dataset,
                                                        replication_task.recursive)

    # It's not fail-safe to send recursive streams because recursive snapshots can have excludes in the past
    # or deleted empty snapshots
    return [ReplicationStepTemplate(replication_task, src_context, dst_context, src_dataset,
                                    get_target_dataset(replication_task, src_dataset), False)
            for src_dataset in src_context.datasets.keys()  # Order is right because it's OrderedDict
            if replication_task_should_replicate_dataset(replication_task, src_dataset)]


def list_datasets_with_snapshots(shell: Shell, dataset: str, recursive: bool) -> {str: [str]}:
    datasets = list_datasets(shell, dataset, recursive)
    datasets_from_snapshots = group_snapshots_by_datasets(list_snapshots(shell, dataset, recursive))
    datasets = dict({dataset: [] for dataset in datasets}, **datasets_from_snapshots)
    return OrderedDict(sorted(datasets.items(), key=lambda t: t[0]))


def resume_replications(step_templates: [ReplicationStepTemplate], observer=None):
    resumed = False
    for step_template in step_templates:
        if step_template.dst_dataset in step_template.dst_context.datasets:
            receive_resume_token = get_receive_resume_token(step_template.dst_context.shell, step_template.dst_dataset)

            if receive_resume_token is not None:
                logger.info("Resuming replication for dst_dataset %r", step_template.dst_dataset)
                run_replication_step(step_template.instantiate(receive_resume_token=receive_resume_token), observer)
                resumed = True

    return resumed


def run_replication_steps(step_templates: [ReplicationStepTemplate], observer=None):
    for step_template in step_templates:
        src_snapshots = step_template.src_context.datasets[step_template.src_dataset]
        dst_snapshots = step_template.dst_context.datasets.get(step_template.dst_dataset, [])

        incremental_base, snapshots = get_snapshots_to_send(src_snapshots, dst_snapshots,
                                                            step_template.replication_task)
        if incremental_base is None and dst_snapshots:
            if step_template.replication_task.allow_from_scratch:
                logger.warning("No incremental base for replication task %r on dataset %r, destroying all destination "
                               "snapshots", step_template.replication_task.id, step_template.src_dataset)
                destroy_snapshots(
                    step_template.dst_context.shell,
                    [Snapshot(step_template.dst_dataset, name) for name in dst_snapshots]
                )
            else:
                raise NoIncrementalBaseReplicationError(
                    f"No incremental base on dataset {step_template.src_dataset!r} and replication from scratch "
                    f"is not allowed"
                )

        if not snapshots:
            logger.info("No snapshots to send for replication task %r on dataset %r", step_template.replication_task.id,
                        step_template.src_dataset)
            continue

        replicate_snapshots(step_template, incremental_base, snapshots, observer)


def get_snapshots_to_send(src_snapshots, dst_snapshots, replication_task):
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
                (
                    parsed_snapshot.datetime == parsed_incremental_base.datetime and
                    parsed_snapshot.name > parsed_incremental_base.name
                ) or
                (
                    parsed_snapshot.datetime > parsed_incremental_base.datetime
                )
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

    return incremental_base, snapshots_to_send


def replicate_snapshots(step_template: ReplicationStepTemplate, incremental_base, snapshots, observer=None):
    for snapshot in snapshots:
        run_replication_step(step_template.instantiate(incremental_base=incremental_base, snapshot=snapshot), observer)
        incremental_base = snapshot


def run_replication_step(step: ReplicationStep, observer=None):
    logger.info("For replication task %r: doing %s from %r to %r of snapshot=%r recursive=%r incremental_base=%r "
                "receive_resume_token=%r", step.replication_task.id, step.replication_task.direction.value,
                step.src_dataset, step.dst_dataset, step.snapshot, step.recursive, step.incremental_base,
                step.receive_resume_token)

    if step.replication_task.direction == ReplicationDirection.PUSH:
        local_context = step.src_context
        remote_context = step.dst_context
    elif step.replication_task.direction == ReplicationDirection.PULL:
        local_context = step.dst_context
        remote_context = step.src_context
    else:
        raise ValueError(f"Invalid replication direction: {step.replication_task.direction!r}")

    transport = remote_context.transport

    process = transport.replication_process(
        step.replication_task.id, transport,
        local_context.shell, remote_context.shell,
        step.replication_task.direction, step.src_dataset, step.dst_dataset,
        step.snapshot, step.recursive, step.incremental_base, step.receive_resume_token,
        step.replication_task.compression, step.replication_task.speed_limit,
        step.replication_task.dedup, step.replication_task.large_block,
        step.replication_task.embed, step.replication_task.compressed)
    process.add_progress_observer(
        lambda snapshot, current, total:
            notify(observer, ReplicationTaskSnapshotProgress(step.replication_task.id, snapshot.split("@")[0],
                                                             snapshot.split("@")[1], current, total)))
    monitor = ReplicationMonitor(step.dst_context.shell, step.dst_dataset)
    ReplicationProcessRunner(process, monitor).run()

    notify(observer, ReplicationTaskSnapshotSuccess(step.replication_task.id, step.src_dataset, step.snapshot))
