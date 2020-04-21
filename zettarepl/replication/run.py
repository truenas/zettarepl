# -*- coding=utf-8 -*-
from collections import defaultdict, OrderedDict
from datetime import datetime
import logging
import os
import socket
import time

import paramiko.ssh_exception

from zettarepl.dataset.create import create_dataset
from zettarepl.dataset.data import ensure_has_no_data
from zettarepl.dataset.list import *
from zettarepl.dataset.relationship import is_child
from zettarepl.observer import (notify, ReplicationTaskStart, ReplicationTaskSuccess, ReplicationTaskSnapshotStart,
                                ReplicationTaskSnapshotProgress, ReplicationTaskSnapshotSuccess, ReplicationTaskError)
from zettarepl.snapshot.destroy import destroy_snapshots
from zettarepl.snapshot.list import *
from zettarepl.snapshot.name import parse_snapshots_names_with_multiple_schemas, parsed_snapshot_sort_key
from zettarepl.snapshot.snapshot import Snapshot
from zettarepl.transport.interface import ExecException, Shell, Transport
from zettarepl.transport.local import LocalShell
from zettarepl.transport.zfscli import get_receive_resume_token, get_property
from zettarepl.transport.zfscli.parse import zfs_bool

from .error import *
from .monitor import ReplicationMonitor
from .process_runner import ReplicationProcessRunner
from .task.dataset import get_target_dataset
from .task.direction import ReplicationDirection
from .task.naming_schema import replication_task_naming_schemas
from .task.readonly_behavior import ReadOnlyBehavior
from .task.should_replicate import *
from .task.task import ReplicationTask

logger = logging.getLogger(__name__)

__all__ = ["run_replication_tasks"]


class GlobalReplicationContext:
    def __init__(self):
        self.snapshots_sent_by_replication_step_template = defaultdict(lambda: 0)
        self.snapshots_total_by_replication_step_template = defaultdict(lambda: 0)

    @property
    def snapshots_sent(self):
        return sum(self.snapshots_sent_by_replication_step_template.values())

    @property
    def snapshots_total(self):
        return sum(self.snapshots_total_by_replication_step_template.values())


class ReplicationContext:
    def __init__(self, context: GlobalReplicationContext, transport: Transport, shell: Shell):
        self.context = context
        self.transport = transport
        self.shell = shell
        self.datasets = None
        self.datasets_readonly = None


class ReplicationStepTemplate:
    def __init__(self, replication_task: ReplicationTask,
                 src_context: ReplicationContext, dst_context: ReplicationContext,
                 src_dataset: str, dst_dataset: str):
        self.replication_task = replication_task
        self.src_context = src_context
        self.dst_context = dst_context
        self.src_dataset = src_dataset
        self.dst_dataset = dst_dataset

    def instantiate(self, **kwargs):
        return ReplicationStep(self,
                               self.replication_task,
                               self.src_context, self.dst_context,
                               self.src_dataset, self.dst_dataset,
                               **kwargs)


class ReplicationStep(ReplicationStepTemplate):
    def __init__(self, template, *args, snapshot=None, incremental_base=None, receive_resume_token=None):
        self.template = template

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
    contexts = defaultdict(GlobalReplicationContext)

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

        local_context = ReplicationContext(contexts[replication_task], None, local_shell)
        remote_context = ReplicationContext(contexts[replication_task], transport, remote_shell)

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
        recoverable_sleep = 1
        for i in range(replication_task.retries):
            if recoverable_error is not None:
                logger.info("After recoverable error sleeping for %d seconds", recoverable_sleep)
                time.sleep(recoverable_sleep)
                recoverable_sleep = min(recoverable_sleep * 2, 60)
            else:
                recoverable_sleep = 1

            try:
                try:
                    run_replication_task_part(replication_task, source_dataset, src_context, dst_context, observer)
                except socket.timeout:
                    raise RecoverableReplicationError("Network connection timeout") from None
                except paramiko.ssh_exception.NoValidConnectionsError as e:
                    raise RecoverableReplicationError(str(e).replace("[Errno None] ", "")) from None
                except (IOError, OSError) as e:
                    raise RecoverableReplicationError(str(e)) from None
                replication_tasks_parts_left[replication_task.id] -= 1
                if replication_tasks_parts_left[replication_task.id] == 0:
                    notify(observer, ReplicationTaskSuccess(replication_task.id))
                break
            except RecoverableReplicationError as e:
                logger.warning("For task %r at attempt %d recoverable replication error %r", replication_task.id,
                               i + 1, e)
                recoverable_error = e
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
    check_target_type(replication_task, source_dataset, src_context, dst_context)

    step_templates = calculate_replication_step_templates(replication_task, source_dataset,
                                                          src_context, dst_context)

    resumed = resume_replications(step_templates, observer)
    if resumed:
        step_templates = calculate_replication_step_templates(replication_task, source_dataset,
                                                              src_context, dst_context)

    run_replication_steps(step_templates, observer)


def check_target_type(replication_task: ReplicationTask, source_dataset: str,
                      src_context: ReplicationContext, dst_context: ReplicationContext):
    target_dataset = get_target_dataset(replication_task, source_dataset)

    source_dataset_type = get_property(src_context.shell, source_dataset, "type")
    try:
        target_dataset_type = get_property(dst_context.shell, target_dataset, "type")
    except ExecException as e:
        if not ("dataset does not exist" in e.stdout):
            raise
    else:
        if source_dataset_type != target_dataset_type:
            raise ReplicationError(f"Source {source_dataset!r} is a {source_dataset_type}, but target "
                                   f"{target_dataset!r} already exists and is a {target_dataset_type}")


def calculate_replication_step_templates(replication_task: ReplicationTask, source_dataset: str,
                                         src_context: ReplicationContext, dst_context: ReplicationContext):
    src_context.datasets = list_datasets_with_snapshots(src_context.shell, source_dataset,
                                                        replication_task.recursive)

    # It's not fail-safe to send recursive streams because recursive snapshots can have excludes in the past
    # or deleted empty snapshots
    source_datasets = src_context.datasets.keys()  # Order is right because it's OrderedDict
    if replication_task.replicate:
        # But when replicate is on, we have no choice
        source_datasets = [source_dataset]

    dst_context.datasets = {}
    dst_context.datasets_readonly = {}
    templates = []
    for source_dataset in source_datasets:
        if not replication_task_should_replicate_dataset(replication_task, source_dataset):
            continue

        target_dataset = get_target_dataset(replication_task, source_dataset)

        try:
            datasets = list_datasets_with_properties(dst_context.shell, target_dataset, replication_task.recursive,
                                                     ["readonly"])
        except ExecException as e:
            if "dataset does not exist" in e.stdout:
                pass
            else:
                raise
        else:
            dst_context.datasets.update(
                list_snapshots_for_datasets(dst_context.shell, target_dataset, replication_task.recursive,
                                            [dataset["name"] for dataset in datasets])
            )
            dst_context.datasets_readonly.update(**{dataset["name"]: zfs_bool(dataset["readonly"])
                                                    for dataset in datasets})

        templates.append(
            ReplicationStepTemplate(replication_task, src_context, dst_context, source_dataset, target_dataset)
        )

    return templates


def list_datasets_with_snapshots(shell: Shell, dataset: str, recursive: bool) -> {str: [str]}:
    datasets = list_datasets(shell, dataset, recursive)
    return list_snapshots_for_datasets(shell, dataset, recursive, datasets)


def list_snapshots_for_datasets(shell: Shell, dataset: str, recursive: bool, datasets: [str]) -> {str: [str]}:
    datasets_from_snapshots = group_snapshots_by_datasets(list_snapshots(shell, dataset, recursive))
    datasets = dict({dataset: [] for dataset in datasets}, **datasets_from_snapshots)
    return OrderedDict(sorted(datasets.items(), key=lambda t: t[0]))


def resume_replications(step_templates: [ReplicationStepTemplate], observer=None):
    resumed = False
    for step_template in step_templates:
        context = step_template.src_context.context

        if step_template.dst_dataset in step_template.dst_context.datasets:
            receive_resume_token = get_receive_resume_token(step_template.dst_context.shell, step_template.dst_dataset)

            if receive_resume_token is not None:
                logger.info("Resuming replication for destination dataset %r", step_template.dst_dataset)

                src_snapshots = step_template.src_context.datasets[step_template.src_dataset]
                dst_snapshots = step_template.dst_context.datasets[step_template.dst_dataset]

                incremental_base, snapshots = get_snapshots_to_send(src_snapshots, dst_snapshots,
                                                                    step_template.replication_task)
                if snapshots:
                    resumed_snapshot = snapshots[0]
                    context.snapshots_total_by_replication_step_template[step_template] = len(snapshots)
                else:
                    logger.warning("Had receive_resume_token, but there are no snapshots to send")
                    resumed_snapshot = "unknown snapshot"
                    context.snapshots_total_by_replication_step_template[step_template] = 1

                try:
                    run_replication_step(step_template.instantiate(receive_resume_token=receive_resume_token), observer,
                                         observer_snapshot=resumed_snapshot)
                except ExecException as e:
                    if "used in the initial send no longer exists" in e.stdout:
                        logger.warning("receive_resume_token for dataset %r references snapshot that no longer exists, "
                                       "discarding it", step_template.dst_dataset)
                        step_template.dst_context.shell.exec(["zfs", "recv", "-A", step_template.dst_dataset])
                        context.snapshots_total_by_replication_step_template[step_template] = 0
                    elif "destination has snapshots" in e.stdout:
                        logger.warning("receive_resume_token for dataset %r is outdated, discarding it",
                                       step_template.dst_dataset)
                        step_template.dst_context.shell.exec(["zfs", "recv", "-A", step_template.dst_dataset])
                        context.snapshots_total_by_replication_step_template[step_template] = 0
                    else:
                        raise
                else:
                    context.snapshots_sent_by_replication_step_template[step_template] = 1
                    context.snapshots_total_by_replication_step_template[step_template] = 1
                    resumed = True

    return resumed


def run_replication_steps(step_templates: [ReplicationStepTemplate], observer=None):
    for step_template in step_templates:
        if step_template.replication_task.readonly == ReadOnlyBehavior.REQUIRE:
            if not step_template.dst_context.datasets_readonly.get(step_template.dst_dataset, True):
                raise ReplicationError(
                    f"Target dataset {step_template.dst_dataset!r} exists and does hot have readonly=on property, "
                    "but replication task is set up to require this property. Refusing to replicate."
                )

    ignored_roots = set()
    for i, step_template in enumerate(step_templates):
        is_immediate_target_dataset = i == 0

        ignore = False
        for ignored_root in ignored_roots:
            if is_child(step_template.src_dataset, ignored_root):
                logger.debug("Not replicating dataset %r because it's ancestor %r did not have any snapshots",
                             step_template.src_dataset, ignored_root)
                ignore = True
        if ignore:
            continue

        src_snapshots = step_template.src_context.datasets[step_template.src_dataset]
        dst_snapshots = step_template.dst_context.datasets.get(step_template.dst_dataset, [])

        incremental_base, snapshots = get_snapshots_to_send(src_snapshots, dst_snapshots,
                                                            step_template.replication_task)
        if incremental_base is None:
            if dst_snapshots:
                if step_template.replication_task.allow_from_scratch:
                    logger.warning(
                        "No incremental base for replication task %r on dataset %r, destroying all destination "
                        "snapshots", step_template.replication_task.id, step_template.src_dataset,
                    )
                    destroy_snapshots(
                        step_template.dst_context.shell,
                        [Snapshot(step_template.dst_dataset, name) for name in dst_snapshots]
                    )
                else:
                    raise NoIncrementalBaseReplicationError(
                        f"No incremental base on dataset {step_template.src_dataset!r} and replication from scratch "
                        f"is not allowed"
                    )
            else:
                if not step_template.replication_task.allow_from_scratch:
                    if is_immediate_target_dataset:
                        # We are only interested in checking target datasets, not their children
                        ensure_has_no_data(step_template.dst_context.shell, step_template.dst_dataset)

        if not snapshots:
            logger.info("No snapshots to send for replication task %r on dataset %r", step_template.replication_task.id,
                        step_template.src_dataset)
            if not src_snapshots:
                ignored_roots.add(step_template.src_dataset)
            continue

        if is_immediate_target_dataset and step_template.dst_dataset not in step_template.dst_context.datasets:
            # Target dataset does not exist, there is a chance that intermediate datasets also do not exist
            parent = os.path.dirname(step_template.dst_dataset)
            if "/" in parent:
                create_dataset(step_template.dst_context.shell, parent)
                handle_readonly(step_template)

        replicate_snapshots(step_template, incremental_base, snapshots, observer)
        handle_readonly(step_template)


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

    return incremental_base, snapshots_to_send


def replicate_snapshots(step_template: ReplicationStepTemplate, incremental_base, snapshots, observer=None):
    step_template.src_context.context.snapshots_total_by_replication_step_template[step_template] += len(snapshots)

    for snapshot in snapshots:
        run_replication_step(step_template.instantiate(incremental_base=incremental_base, snapshot=snapshot), observer)
        incremental_base = snapshot


def run_replication_step(step: ReplicationStep, observer=None, observer_snapshot=None):
    logger.info("For replication task %r: doing %s from %r to %r of snapshot=%r incremental_base=%r "
                "receive_resume_token=%r", step.replication_task.id, step.replication_task.direction.value,
                step.src_dataset, step.dst_dataset, step.snapshot, step.incremental_base,
                step.receive_resume_token)

    observer_snapshot = observer_snapshot or step.snapshot

    notify(observer, ReplicationTaskSnapshotStart(
        step.replication_task.id, step.src_dataset, observer_snapshot,
        step.src_context.context.snapshots_sent, step.src_context.context.snapshots_total,
    ))

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
        step.replication_task.id,
        transport,
        local_context.shell,
        remote_context.shell,
        step.replication_task.direction,
        step.src_dataset,
        step.dst_dataset,
        step.snapshot,
        step.replication_task.properties,
        step.replication_task.replicate,
        step.incremental_base,
        step.receive_resume_token,
        step.replication_task.compression,
        step.replication_task.speed_limit,
        step.replication_task.dedup,
        step.replication_task.large_block,
        step.replication_task.embed,
        step.replication_task.compressed)
    process.add_progress_observer(
        lambda bytes_sent, bytes_total:
            notify(observer, ReplicationTaskSnapshotProgress(
                step.replication_task.id, step.src_dataset, observer_snapshot,
                step.src_context.context.snapshots_sent, step.src_context.context.snapshots_total,
                bytes_sent, bytes_total,
            ))
    )
    monitor = ReplicationMonitor(step.dst_context.shell, step.dst_dataset)
    ReplicationProcessRunner(process, monitor).run()

    step.template.src_context.context.snapshots_sent_by_replication_step_template[step.template] += 1
    notify(observer, ReplicationTaskSnapshotSuccess(
        step.replication_task.id, step.src_dataset, observer_snapshot,
        step.src_context.context.snapshots_sent, step.src_context.context.snapshots_total,
    ))

    if step.incremental_base is None:
        # Might have created dataset, need to set it to readonly
        handle_readonly(step.template)


def handle_readonly(step_template: ReplicationStepTemplate):
    if step_template.replication_task.readonly in (ReadOnlyBehavior.SET, ReadOnlyBehavior.REQUIRE):
        # We only want to inherit if dataset is a child of some replicated dataset
        parent = os.path.dirname(step_template.dst_dataset)
        if (
            parent in step_template.dst_context.datasets_readonly and
            step_template.dst_context.datasets_readonly.get(step_template.dst_dataset) is False
        ):
            # Parent should be `readonly=on` by now which means for this dataset `readonly=off` was set explicitly
            # Let's reset it
            step_template.dst_context.shell.exec(["zfs", "inherit", "readonly", step_template.dst_dataset])

        step_template.dst_context.datasets_readonly[step_template.dst_dataset] = True

        # We only set value is there is no parent that already has this value set
        if not step_template.dst_context.datasets_readonly.get(parent, False):
            step_template.dst_context.shell.exec(["zfs", "set", "readonly=on", step_template.dst_dataset])
