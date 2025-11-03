# -*- coding=utf-8 -*-
from collections import defaultdict, OrderedDict
from datetime import datetime
import logging
import os
import signal
import socket
import time

import paramiko.ssh_exception

from zettarepl.dataset.create import create_dataset
from zettarepl.dataset.data import DatasetIsNotMounted, list_data, ensure_has_no_data
from zettarepl.dataset.list import *
from zettarepl.dataset.relationship import is_child
from zettarepl.observer import (notify, ReplicationTaskStart, ReplicationTaskSuccess, ReplicationTaskSnapshotStart,
                                ReplicationTaskSnapshotProgress, ReplicationTaskSnapshotSuccess,
                                ReplicationTaskDataProgress, ReplicationTaskError)
from zettarepl.snapshot.list import *
from zettarepl.transport.interface import ExecException, Shell, Transport
from zettarepl.transport.local import LocalShell
from zettarepl.transport.base_ssh import BaseSshTransport
from zettarepl.transport.zfscli import get_properties, get_property
from zettarepl.transport.zfscli.exception import DatasetDoesNotExistException
from zettarepl.transport.zfscli.parse import zfs_bool
from zettarepl.transport.zfscli.warning import warnings_from_zfs_success

from .dataset_size_observer import DatasetSizeObserver
from .error import *
from .monitor import ReplicationMonitor
from .partially_complete_state import retry_contains_partially_complete_state
from .pre_retention import pre_retention
from .process_runner import ReplicationProcessRunner
from .snapshots_to_send import SnapshotsToSend, get_snapshots_to_send
from .task.dataset import get_target_dataset
from .task.direction import ReplicationDirection
from .task.encryption import ReplicationEncryption
from .task.readonly_behavior import ReadOnlyBehavior
from .task.should_replicate import replication_task_should_replicate_dataset
from .task.task import ReplicationTask

logger = logging.getLogger(__name__)

__all__ = ["run_replication_tasks"]


class GlobalReplicationContext:
    def __init__(self, now: datetime):
        self.now = now
        self.snapshots_sent_by_replication_step_template = defaultdict(lambda: 0)
        self.snapshots_total_by_replication_step_template = defaultdict(lambda: 0)
        self.last_recoverable_error = None
        self.warnings = []

    @property
    def snapshots_sent(self):
        return sum(self.snapshots_sent_by_replication_step_template.values())

    @property
    def snapshots_total(self):
        return sum(self.snapshots_total_by_replication_step_template.values())

    def add_warning(self, warning):
        if warning not in self.warnings:
            self.warnings.append(warning)


class ReplicationContext:
    def __init__(self, context: GlobalReplicationContext, transport: Transport, shell: Shell):
        self.context = context
        self.transport = transport
        self.shell = shell
        self.datasets = None
        self.datasets_encrypted = None
        self.datasets_readonly = None
        self.datasets_receive_resume_tokens = None

    def remove_dataset(self, dataset):
        for dictionary in (
            self.datasets,
            self.datasets_encrypted,
            self.datasets_readonly,
            self.datasets_receive_resume_tokens,
        ):
            if dictionary is None:
                continue

            for k in list(dictionary.keys()):
                if k == dataset or k.startswith(f"{dataset}/"):
                    dictionary.pop(k)


class ReplicationStepTemplate:
    def __init__(self, replication_task: ReplicationTask,
                 src_context: ReplicationContext, dst_context: ReplicationContext,
                 src_dataset: str, dst_dataset: str,
                 valid_properties: [str]):
        self.replication_task = replication_task
        self.src_context = src_context
        self.dst_context = dst_context
        self.src_dataset = src_dataset
        self.dst_dataset = dst_dataset
        self.valid_properties = valid_properties

    def instantiate(self, **kwargs):
        return ReplicationStep(self,
                               self.replication_task,
                               self.src_context, self.dst_context,
                               self.src_dataset, self.dst_dataset,
                               self.valid_properties,
                               **kwargs)


class ReplicationStep(ReplicationStepTemplate):
    def __init__(self, template, *args, snapshot=None, incremental_base=None, include_intermediate=None,
                 receive_resume_token=None, encryption: ReplicationEncryption=None):
        self.template = template

        super().__init__(*args)

        self.snapshot = snapshot
        self.incremental_base = incremental_base
        self.include_intermediate = include_intermediate
        self.receive_resume_token = receive_resume_token
        self.encryption = encryption
        if self.receive_resume_token is None:
            assert self.snapshot is not None
        else:
            assert self.snapshot is None
            assert self.incremental_base is None
        if self.encryption is not None:
            assert self.incremental_base is None
            assert self.receive_resume_token is None


def run_replication_tasks(now: datetime, local_shell: LocalShell, transport: Transport, remote_shell: Shell,
                          replication_tasks: [ReplicationTask], observer=None):
    contexts = defaultdict(lambda: GlobalReplicationContext(now))

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
                    retry_contains_partially_complete_state(
                        lambda: run_replication_task_part(replication_task, source_dataset, src_context, dst_context,
                                                          observer),
                    )
                except socket.timeout:
                    raise RecoverableReplicationError("Network connection timeout") from None
                except paramiko.ssh_exception.NoValidConnectionsError as e:
                    raise RecoverableReplicationError(str(e).replace("[Errno None] ", "")) from None
                except paramiko.ssh_exception.SSHException as e:
                    if isinstance(e, (paramiko.ssh_exception.AuthenticationException,
                                      paramiko.ssh_exception.BadHostKeyException,
                                      paramiko.ssh_exception.ProxyCommandFailure,
                                      paramiko.ssh_exception.ConfigParseError)):
                        raise ReplicationError(str(e).replace("[Errno None] ", "")) from None
                    else:
                        # It might be an SSH error that leaves paramiko connection in an invalid state
                        # Let's reset remote shell just in case
                        remote_shell.close()
                        raise RecoverableReplicationError(str(e).replace("[Errno None] ", "")) from None
                except ExecException as e:
                    if e.returncode == 128 + signal.SIGPIPE:
                        for warning in warnings_from_zfs_success(e.stdout):
                            contexts[replication_task].add_warning(warning)
                        raise RecoverableReplicationError(broken_pipe_error(e.stdout))
                    else:
                        raise 
                except OSError as e:
                    raise RecoverableReplicationError(str(e)) from None
                replication_tasks_parts_left[replication_task.id] -= 1
                if replication_tasks_parts_left[replication_task.id] == 0:
                    notify(observer, ReplicationTaskSuccess(replication_task.id, contexts[replication_task].warnings))
                break
            except RecoverableReplicationError as e:
                logger.warning("For task %r at attempt %d recoverable replication error %r", replication_task.id,
                               i + 1, e)
                src_context.context.last_recoverable_error = recoverable_error = e
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
                              src_context: ReplicationContext, dst_context: ReplicationContext, observer):
    target_dataset = get_target_dataset(replication_task, source_dataset)

    check_target_existence_and_type(replication_task, source_dataset, src_context, dst_context)

    step_templates = calculate_replication_step_templates(replication_task, source_dataset, src_context, dst_context)

    check_encrypted_target(replication_task, source_dataset, src_context, dst_context)

    # Remote retention has to be executed prior to running actual replication in order to free disk space or quotas
    pre_retention(src_context.context.now.replace(tzinfo=None), replication_task, src_context.datasets,
                  dst_context.datasets, target_dataset, dst_context.shell)

    with DatasetSizeObserver(
        src_context.shell, dst_context.shell,
        source_dataset, target_dataset,
        lambda src_used, dst_used: notify(observer,
                                          ReplicationTaskDataProgress(replication_task.id, source_dataset,
                                                                      src_used, dst_used))
    ):
        resumed = resume_replications(step_templates, observer)
        if resumed:
            step_templates = calculate_replication_step_templates(replication_task, source_dataset,
                                                                  src_context, dst_context)

        run_replication_steps(step_templates, observer)

    if replication_task.mount:
        mount_dst_datasets(dst_context, target_dataset, replication_task.recursive)


def check_target_existence_and_type(replication_task: ReplicationTask, source_dataset: str,
                                    src_context: ReplicationContext, dst_context: ReplicationContext):
    target_dataset = get_target_dataset(replication_task, source_dataset)

    source_dataset_type = get_property(src_context.shell, source_dataset, "type")
    try:
        target_dataset_type = get_property(dst_context.shell, target_dataset, "type")
    except DatasetDoesNotExistException:
        pass
    else:
        if replication_task.only_from_scratch:
            raise ReplicationError(f"Target dataset {target_dataset!r} already exists")

        if source_dataset_type != target_dataset_type:
            raise ReplicationError(f"Source {source_dataset!r} is a {source_dataset_type}, but target "
                                   f"{target_dataset!r} already exists and is a {target_dataset_type}")


def check_encrypted_target(replication_task: ReplicationTask, source_dataset: str,
                           src_context: ReplicationContext, dst_context: ReplicationContext):
    dst_dataset = get_target_dataset(replication_task, source_dataset)

    if dst_dataset not in dst_context.datasets:
        if replication_task.encryption:
            if replication_task.encryption.inherit:
                if not existing_parent_is_encrypted(dst_context.shell, dst_dataset):
                    raise ReplicationError(
                        f"Encryption inheritance requested for destination dataset {dst_dataset!r}, but its existing "
                        f"parent is not encrypted."
                    )

        else:
            if new_dataset_should_be_encrypted(dst_context.shell, dst_dataset):
                if replication_task.properties:
                    if not src_context.datasets_encrypted[source_dataset]:
                        raise ReplicationError(
                            f"Destination dataset {dst_dataset!r} must be encrypted (as one of its ancestors is "
                            f"encrypted). Refusing to transfer unencrypted source dataset {source_dataset!r}. "
                            f"Please, set up replication task encryption in order to replicate this dataset."
                        )
                else:
                    raise ReplicationError(
                        f"Destination dataset {dst_dataset!r} must be encrypted (as one of its ancestors is "
                        f"encrypted). Refusing to transfer source dataset {source_dataset!r} without properties and "
                        f"without replication task encryption."
                    )

        return

    try:
        properties = get_properties(dst_context.shell, dst_dataset, {"encryption": str, "encryptionroot": str})
    except ExecException as e:
        logger.debug("Encryption not supported on shell %r: %r (exit code = %d)", dst_context.shell,
                     e.stdout.split("\n")[0], e.returncode)
        return

    if replication_task.encryption and properties["encryption"] == "off":
        raise ReplicationError(f"Encryption requested for destination dataset {dst_dataset!r}, but it already exists "
                               "and is not encrypted.")

    if dst_context.datasets[dst_dataset]:
        return

    if dst_context.datasets_receive_resume_tokens.get(dst_dataset) is not None:
        return

    if properties["encryption"] == "off":
        return

    if properties["encryptionroot"] == dst_dataset:
        raise ReplicationError(f"Destination dataset {dst_dataset!r} already exists and is its own encryption root. "
                               "This configuration is not supported yet. If you want to replicate into an encrypted "
                               "dataset, please, encrypt its parent dataset.")

    try:
        index = list_data(dst_context.shell, dst_dataset)
    except DatasetIsNotMounted:
        logger.debug("Encrypted dataset %r is not mounted, not trying to destroy", dst_dataset)
    else:
        if not index:
            logger.info("Encrypted destination dataset %r does not have matching snapshots or data, destroying it",
                        dst_dataset)
            dst_context.shell.exec(["zfs", "destroy", dst_dataset])
            dst_context.datasets.pop(dst_dataset, None)
            dst_context.datasets_readonly.pop(dst_dataset, None)


def new_dataset_should_be_encrypted(shell, dataset):
    return existing_parent_is_encrypted(shell, dataset)


def existing_parent_is_encrypted(shell, dataset):
    parent = dataset
    while "/" in parent:
        parent = os.path.dirname(parent)
        try:
            encryption = get_property(shell, parent, "encryption")
            if encryption != "off":
                return True
        except DatasetDoesNotExistException:
            pass
        except ExecException as e:
            logger.debug("Encryption not supported on shell %r: %r (exit code = %d)", shell, e.stdout.split("\n")[0],
                         e.returncode)
            return False

    return False


def calculate_replication_step_templates(replication_task: ReplicationTask, source_dataset: str,
                                         src_context: ReplicationContext, dst_context: ReplicationContext):
    src_context.datasets = list_datasets_with_snapshots(src_context.shell, source_dataset,
                                                        replication_task.recursive)
    if replication_task.properties:
        src_context.datasets_encrypted = get_datasets_encrypted(src_context.shell, source_dataset,
                                                                replication_task.recursive)

        if replication_task.encryption:
            for dataset, encrypted in src_context.datasets_encrypted.items():
                if encrypted:
                    if replication_task_should_replicate_dataset(replication_task, dataset):
                        raise ReplicationError(
                            f"Re-encrypting already encrypted source dataset {dataset!r} while preserving its "
                            "properties is not supported"
                        )

    # It's not fail-safe to send recursive streams because recursive snapshots can have excludes in the past
    # or deleted empty snapshots
    source_datasets = src_context.datasets.keys()  # Order is right because it's OrderedDict
    if replication_task.replicate:
        # But when replicate is on, we have no choice
        source_datasets = [source_dataset]

    dst_context.datasets = {}
    dst_context.datasets_readonly = {}
    dst_context.datasets_receive_resume_tokens = {}
    templates = []
    for source_dataset in source_datasets:
        if not replication_task_should_replicate_dataset(replication_task, source_dataset):
            continue

        target_dataset = get_target_dataset(replication_task, source_dataset)

        valid_properties = set()
        referenced_properties = (
            set(replication_task.properties_exclude) |
            set(replication_task.properties_override.keys())
        )
        if referenced_properties:
            for property, (value, source) in get_properties(
                src_context.shell, source_dataset, {p: str for p in referenced_properties}, True
            ).items():
                if source == "-":
                    # Invalid properties (like `mountpoint` for VOLUME) will be seen like this
                    # Properties like `used` will be seen like this too, but they will never appear in
                    # `referenced_properties` because no one excludes or overrides them.
                    continue

                valid_properties.add(property)

        try:
            datasets = list_datasets_with_properties(dst_context.shell, target_dataset, replication_task.recursive,
                                                     {"readonly": str, "receive_resume_token": str})
        except DatasetDoesNotExistException:
            pass
        else:
            dst_context.datasets.update(
                list_snapshots_for_datasets(dst_context.shell, target_dataset, replication_task.recursive,
                                            [dataset["name"] for dataset in datasets])
            )
            dst_context.datasets_readonly.update(**{dataset["name"]: zfs_bool(dataset["readonly"])
                                                    for dataset in datasets})
            dst_context.datasets_receive_resume_tokens.update(**{
                dataset["name"]: dataset["receive_resume_token"] if dataset["receive_resume_token"] != "-" else None
                for dataset in datasets
            })

        templates.append(
            ReplicationStepTemplate(replication_task, src_context, dst_context, source_dataset, target_dataset,
                                    valid_properties)
        )

    return templates


def list_datasets_with_snapshots(shell: Shell, dataset: str, recursive: bool) -> {str: [str]}:
    datasets = list_datasets(shell, dataset, recursive)
    return list_snapshots_for_datasets(shell, dataset, recursive, datasets)


def list_snapshots_for_datasets(shell: Shell, dataset: str, recursive: bool, datasets: [str]) -> {str: [str]}:
    datasets_from_snapshots = group_snapshots_by_datasets(list_snapshots(shell, dataset, recursive))
    datasets = dict({dataset: [] for dataset in datasets}, **datasets_from_snapshots)
    return OrderedDict(sorted(datasets.items(), key=lambda t: t[0]))


def get_datasets_encrypted(shell: Shell, dataset: str, recursive: bool):
    try:
        return {
            dataset["name"]: dataset["encryption"] != "off"
            for dataset in list_datasets_with_properties(shell, dataset, recursive, {"encryption": str})
        }
    except ExecException as e:
        logger.debug("Encryption not supported on shell %r: %r (exit code = %d)", shell, e.stdout.split("\n")[0],
                     e.returncode)
        return defaultdict(lambda: False)


def resume_replications(step_templates: [ReplicationStepTemplate], observer=None):
    resumed = False
    for step_template in step_templates:
        context = step_template.src_context.context

        if step_template.replication_task.replicate:
            for dst_dataset, token in step_template.dst_context.datasets_receive_resume_tokens.items():
                if token is not None:
                    logger.warning(
                        "Discarding receive_resume_token for destination dataset %r as it is not supported in "
                        "`replicate` mode",
                        dst_dataset,
                    )
                    step_template.dst_context.shell.exec(["zfs", "recv", "-A", dst_dataset])
                    return False

        if step_template.dst_dataset in step_template.dst_context.datasets:
            receive_resume_token = step_template.dst_context.datasets_receive_resume_tokens.get(
                step_template.dst_dataset
            )

            if receive_resume_token is not None:
                logger.info("Resuming replication for destination dataset %r", step_template.dst_dataset)

                src_snapshots = step_template.src_context.datasets[step_template.src_dataset]
                dst_snapshots = step_template.dst_context.datasets[step_template.dst_dataset]

                incremental_base, snapshots = get_snapshots_to_send(
                    src_snapshots, dst_snapshots, step_template.replication_task, step_template.src_context.shell,
                    step_template.src_dataset,
                )[:2]
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
                message = (
                    f"Target dataset {step_template.dst_dataset!r} exists and does not have readonly=on property, "
                    "but replication task is set up to require this property. Refusing to replicate."
                )
                try:
                    target_type = get_property(step_template.dst_context.shell, step_template.dst_dataset, "type")
                except Exception:
                    pass
                else:
                    if target_type == "volume":
                        message += (
                            f" Please run \"zfs set readonly=on {step_template.dst_dataset}\" on the target system "
                            "to fix this."
                        )

                raise ReplicationError(message)

    plan = []
    ignored_roots = set()
    for i, step_template in enumerate(step_templates):
        is_immediate_target_dataset = i == 0

        ignore = False
        for ignored_root in ignored_roots:
            if is_child(step_template.src_dataset, ignored_root):
                logger.debug("Not replicating dataset %r because its ancestor %r did not have any snapshots",
                             step_template.src_dataset, ignored_root)
                ignore = True
        if ignore:
            continue

        src_snapshots = step_template.src_context.datasets[step_template.src_dataset]
        dst_snapshots = step_template.dst_context.datasets.get(step_template.dst_dataset, [])

        snapshots_to_send = get_snapshots_to_send(
            src_snapshots, dst_snapshots, step_template.replication_task, step_template.src_context.shell,
            step_template.src_dataset,
        )
        if snapshots_to_send.incremental_base is None and snapshots_to_send.snapshots:
            if dst_snapshots:
                if step_template.replication_task.allow_from_scratch:
                    logger.warning(
                        "No incremental base for replication task %r on dataset %r, destroying destination dataset",
                        step_template.replication_task.id, step_template.src_dataset,
                    )
                    step_template.dst_context.shell.exec(["zfs", "destroy", "-r", step_template.dst_dataset])
                    step_template.dst_context.remove_dataset(step_template.dst_dataset)
                else:
                    raise NoIncrementalBaseReplicationError(
                        f"No incremental base on dataset {step_template.src_dataset!r} and replication from scratch "
                        f"is not allowed"
                    )
            else:
                if not step_template.replication_task.allow_from_scratch:
                    if is_immediate_target_dataset:
                        # We are only interested in checking target datasets, not their children

                        allowed_empty_children = []
                        if step_template.replication_task.recursive:
                            allowed_dst_child_datasets = {
                                get_target_dataset(step_template.replication_task, dataset)
                                for dataset in (
                                    set(step_template.src_context.datasets) -
                                    set(step_template.replication_task.exclude)
                                )
                                if dataset != step_template.src_dataset and is_child(dataset, step_template.src_dataset)
                            }
                            existing_dst_child_datasets = {
                                dataset
                                for dataset in step_template.dst_context.datasets
                                if dataset != step_template.dst_dataset and is_child(dataset, step_template.dst_dataset)
                            }
                            allowed_empty_children = list(allowed_dst_child_datasets & existing_dst_child_datasets)

                        ensure_has_no_data(step_template.dst_context.shell, step_template.dst_dataset,
                                           allowed_empty_children)

        if snapshots_to_send.incremental_base is not None and step_template.replication_task.replicate:
            snapshots_to_send = check_base_consistency_for_full_replication(step_template, snapshots_to_send)

        incremental_base, snapshots, include_intermediate, empty_is_successful = snapshots_to_send

        if not snapshots:
            logger.info("No snapshots to send for replication task %r on dataset %r", step_template.replication_task.id,
                        step_template.src_dataset)

            if is_immediate_target_dataset and incremental_base is None and not empty_is_successful:
                raise ReplicationError(
                    f"Dataset {step_template.src_dataset!r} does not have any matching snapshots to replicate"
                )

            if not src_snapshots:
                ignored_roots.add(step_template.src_dataset)

            continue

        if is_immediate_target_dataset and step_template.dst_dataset not in step_template.dst_context.datasets:
            # Target dataset does not exist, there is a chance that intermediate datasets also do not exist
            parent = os.path.dirname(step_template.dst_dataset)
            if "/" in parent:
                create_dataset(step_template.dst_context.shell, parent)

        encryption = None
        if (
                step_template.replication_task.encryption and
                step_template.dst_dataset not in step_template.dst_context.datasets
        ):
            if is_immediate_target_dataset:
                encryption = step_template.replication_task.encryption
            else:
                encryption = ReplicationEncryption(True, None, None, None)

        step_template.src_context.context.snapshots_total_by_replication_step_template[step_template] += len(snapshots)
        plan.append((step_template, incremental_base, snapshots, include_intermediate, encryption))

    for step_template, incremental_base, snapshots, include_intermediate, encryption in plan:
        replicate_snapshots(step_template, incremental_base, snapshots, include_intermediate, encryption, observer)
        handle_readonly(step_template)


def check_base_consistency_for_full_replication(
    step_template: ReplicationStepTemplate,
    snapshots_to_send: SnapshotsToSend,
) -> SnapshotsToSend:
    for src_dataset, src_dataset_snapshots in step_template.src_context.datasets.items():
        if not is_child(src_dataset, step_template.src_dataset):
            continue

        if snapshots_to_send.incremental_base not in src_dataset_snapshots:
            continue

        dst_dataset = get_target_dataset(step_template.replication_task, src_dataset)
        if snapshots_to_send.incremental_base not in step_template.dst_context.datasets.get(dst_dataset, []):
            if step_template.dst_context.context.last_recoverable_error is not None:
                partial_snapshot = f"{step_template.dst_dataset}@{snapshots_to_send.incremental_base}"
                logger.warning(
                    f"Full ZFS replication failed, and there are retries left. Removing partial snapshot "
                    f"{partial_snapshot} and starting again."
                )
                try:
                    step_template.dst_context.shell.exec(["zfs", "destroy", "-r", partial_snapshot])
                except Exception as e:
                    logger.warning(f"Destroying partial snapshot failed: {e!r}")
                    text = "Full "
                else:
                    src_snapshots = step_template.src_context.datasets[step_template.src_dataset]
                    dst_snapshots = step_template.dst_context.datasets[step_template.dst_dataset]
                    dst_snapshots.remove(snapshots_to_send.incremental_base)

                    return get_snapshots_to_send(
                        src_snapshots, dst_snapshots, step_template.replication_task, step_template.src_context.shell,
                        step_template.src_dataset,
                    )
            else:
                text = "Last full "

            text += (
                "ZFS replication failed to transfer all the children of the snapshot "
                f"{step_template.src_dataset}@{snapshots_to_send.incremental_base}. "
            )

            if step_template.dst_context.context.last_recoverable_error is not None:
                text += f"The error was: {str(step_template.dst_context.context.last_recoverable_error).rstrip('.')}. "

            text += (
                f"The snapshot {dst_dataset}@{snapshots_to_send.incremental_base} was not transferred. Please run "
                f"`zfs destroy -r {step_template.dst_dataset}@{snapshots_to_send.incremental_base}` on the target "
                "system and run replication again."
            )

            raise ReplicationError(text)

    return snapshots_to_send


def replicate_snapshots(step_template: ReplicationStepTemplate, incremental_base, snapshots, include_intermediate,
                        encryption, observer):
    for snapshot in snapshots:
        step = step_template.instantiate(incremental_base=incremental_base, snapshot=snapshot,
                                         include_intermediate=include_intermediate, encryption=encryption)
        run_replication_step(step, observer)
        incremental_base = snapshot
        encryption = None


def run_replication_step(step: ReplicationStep, observer=None, observer_snapshot=None):
    logger.info(
        "For replication task %r: doing %s from %r to %r of snapshot=%r incremental_base=%r include_intermediate=%r "
        "receive_resume_token=%r encryption=%r",
        step.replication_task.id, step.replication_task.direction.value, step.src_dataset, step.dst_dataset,
        step.snapshot, step.incremental_base, step.include_intermediate, step.receive_resume_token,
        step.encryption is not None,
    )

    observer_snapshot = observer_snapshot or step.snapshot

    notify(observer, ReplicationTaskSnapshotStart(
        step.replication_task.id, step.src_dataset, observer_snapshot,
        step.src_context.context.snapshots_sent, step.src_context.context.snapshots_total,
    ))

    # Umount target dataset because we will be overwriting its contents and children mountpoints
    # will become dangling. ZFS will mount entire directory structure again after receiving.
    try:
        step.dst_context.shell.exec(["zfs", "umount", step.dst_dataset])
    except ExecException:
        pass

    if step.replication_task.direction == ReplicationDirection.PUSH:
        local_context = step.src_context
        remote_context = step.dst_context
    elif step.replication_task.direction == ReplicationDirection.PULL:
        local_context = step.dst_context
        remote_context = step.src_context
    else:
        raise ValueError(f"Invalid replication direction: {step.replication_task.direction!r}")

    if step.replication_task.replicate:
        raw = any(step.src_context.datasets_encrypted.values())
    else:
        raw = step.replication_task.properties and step.src_context.datasets_encrypted[step.src_dataset]

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
        step.replication_task.mount,
        step.replication_task.properties,
        list(set(step.replication_task.properties_exclude) & step.valid_properties),
        {k: v for k, v in step.replication_task.properties_override.items() if k in step.valid_properties},
        step.replication_task.replicate,
        step.encryption,
        step.incremental_base,
        step.include_intermediate,
        step.receive_resume_token,
        step.replication_task.compression,
        step.replication_task.speed_limit,
        step.replication_task.dedup,
        step.replication_task.large_block,
        step.replication_task.embed,
        step.replication_task.compressed,
        raw,
    )
    process.add_progress_observer(
        lambda bytes_sent, bytes_total:
            notify(observer, ReplicationTaskSnapshotProgress(
                step.replication_task.id, step.src_dataset, observer_snapshot,
                step.src_context.context.snapshots_sent, step.src_context.context.snapshots_total,
                bytes_sent, bytes_total,
            ))
    )
    process.add_warning_observer(step.template.src_context.context.add_warning)
    monitor = ReplicationMonitor(step.dst_context.shell, step.dst_dataset)
    try:
        ReplicationProcessRunner(process, monitor).run()
    except ExecException as e:
        if "contains partially-complete state" in e.stdout and step.receive_resume_token:
            raise ContainsPartiallyCompleteState() from None

        raise

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
        try:
            # We only want to inherit if dataset is a child of some replicated dataset
            parent = os.path.dirname(step_template.dst_dataset)
            if (
                parent in step_template.dst_context.datasets_readonly and
                step_template.dst_context.datasets_readonly.get(step_template.dst_dataset) is False
            ):
                # Parent should be `readonly=on` by now which means for this dataset `readonly=off` was set explicitly
                # Let's reset it
                action = "inherit"
                step_template.dst_context.shell.exec(["zfs", "inherit", "readonly", step_template.dst_dataset])

            step_template.dst_context.datasets_readonly[step_template.dst_dataset] = True

            # We only set value is there is no parent that already has this value set
            if not step_template.dst_context.datasets_readonly.get(parent, False):
                action = "set"
                step_template.dst_context.shell.exec(["zfs", "set", "readonly=on", step_template.dst_dataset])
        except ExecException as e:
            if (
                    e.stdout.strip().endswith("permission denied") and
                    isinstance(step_template.dst_context.shell.transport, BaseSshTransport) and
                    step_template.dst_context.shell.transport.username != "root"
            ):
                raise ReplicationError(
                    f"cannot {action} `readonly` property for {step_template.dst_dataset!r}: permission denied. "
                    "Please either allow your replication user to change dataset properties or set `readonly` "
                    "replication task option to `IGNORE`"
                )

            raise


def mount_dst_datasets(dst_context: ReplicationContext, dst_dataset: str, recursive: bool):
    dst_datasets = list_datasets_with_properties(dst_context.shell, dst_dataset, recursive, {
        "type": str,
        "mounted": bool,
        "canmount": str,
        "mountpoint": str
    })
    for properties in sorted(dst_datasets, key=lambda dataset: len(dataset["name"])):
        if properties["type"] != "filesystem":
            continue
        if properties["mounted"]:
            continue
        if properties["canmount"] in ["no", "noauto", "off"]:
            continue
        if properties["mountpoint"] in ["none", "legacy"]:
            continue

        try:
            dst_context.shell.exec(["zfs", "mount", properties["name"]])
        except ExecException as e:
            if not ("encryption key not loaded" in e.stdout):
                for warning in e.stdout.splitlines():
                    dst_context.context.add_warning(warning)


def broken_pipe_error(error):
    if error:
        if "\n" in error:
            if not error.endswith("\n"):
                error += "\n"
        else:
            if not error.endswith("."):
                error += "."
            error += " "
    error += "Broken pipe."

    return error
