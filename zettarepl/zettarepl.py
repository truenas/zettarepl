# -*- coding=utf-8 -*-
from collections import namedtuple
from datetime import datetime
import functools
import logging
import threading

from zettarepl.dataset.relationship import is_child
from zettarepl.observer import (notify, PeriodicSnapshotTaskStart, PeriodicSnapshotTaskSuccess,
                                PeriodicSnapshotTaskError, ReplicationTaskScheduled)
from zettarepl.replication.run import run_replication_tasks
from zettarepl.replication.task.dataset import get_target_dataset
from zettarepl.replication.task.direction import ReplicationDirection
from zettarepl.replication.task.snapshot_owner import *
from zettarepl.replication.task.snapshot_query import *
from zettarepl.replication.task.task import *
from zettarepl.retention.calculate import calculate_snapshots_to_remove
from zettarepl.retention.snapshot_removal_date_snapshot_owner import SnapshotRemovalDateSnapshotOwner
from zettarepl.scheduler.clock import Clock
from zettarepl.scheduler.tz_clock import TzClock
from zettarepl.scheduler.scheduler import Scheduler
from zettarepl.snapshot.create import *
from zettarepl.snapshot.destroy import destroy_snapshots
from zettarepl.snapshot.empty import get_empty_snapshots_for_deletion
from zettarepl.snapshot.list import *
from zettarepl.snapshot.name import get_snapshot_name, parse_snapshot_name, parsed_snapshot_sort_key
from zettarepl.snapshot.snapshot import Snapshot
from zettarepl.snapshot.task.snapshot_owner import PeriodicSnapshotTaskSnapshotOwner
from zettarepl.snapshot.task.task import PeriodicSnapshotTask
from zettarepl.transport.compare import are_same_host
from zettarepl.transport.local import LocalShell
from zettarepl.transport.timeout import ShellTimeoutContext
from zettarepl.truenas.removal_dates import get_removal_dates
from zettarepl.utils.itertools import bisect_by_class, select_by_class, sortedgroupby
from zettarepl.utils.logging import ReplicationTaskLoggingLevelFilter

logger = logging.getLogger(__name__)

__all__ = ["Zettarepl"]

ScheduledPeriodicSnapshotTask = namedtuple("ScheduledPeriodicSnapshotTask", [
    "task", "snapshot_name", "parsed_snapshot_name",
])


def create_zettarepl(definition, clock_args=None):
    clock_args = clock_args or []

    clock = Clock(*clock_args)
    tz_clock = TzClock(definition.timezone, clock.now)

    scheduler = Scheduler(clock, tz_clock)
    local_shell = LocalShell()

    return Zettarepl(scheduler, local_shell, definition.max_parallel_replication_tasks, definition.use_removal_dates)


class Zettarepl:
    def __init__(self, scheduler, local_shell, max_parallel_replication_tasks=None, use_removal_dates=False):
        self.scheduler = scheduler
        self.local_shell = local_shell
        self.max_parallel_replication_tasks = max_parallel_replication_tasks
        self.use_removal_dates = use_removal_dates

        self.observer = None

        self.tasks = []

        self.tasks_lock = threading.Lock()
        self.running_tasks = []
        self.pending_tasks = []
        self.retention_datetime = None
        self.retention_running = False
        self.retention_shells = {}

    def set_observer(self, observer):
        self.observer = observer

    def set_tasks(self, tasks):
        self.tasks = tasks

        self.scheduler.set_tasks(list(filter(self._is_scheduler_task, tasks)))

    def _is_scheduler_task(self, task):
        if isinstance(task, PeriodicSnapshotTask):
            return True

        if isinstance(task, ReplicationTask):
            return task.auto and task.schedule is not None

        return False

    def run(self):
        for scheduled in self.scheduler.schedule():
            logger.debug("Scheduled: %r", scheduled)

            self.retention_datetime = scheduled.datetime.datetime

            tasks = scheduled.tasks
            if tasks:
                logger.info("Scheduled tasks: %r", tasks)

                periodic_snapshot_tasks, tasks = bisect_by_class(PeriodicSnapshotTask, tasks)
                self._run_periodic_snapshot_tasks(scheduled.datetime.offset_aware_datetime, periodic_snapshot_tasks)

                replication_tasks, tasks = bisect_by_class(ReplicationTask, tasks)
                replication_tasks.extend(
                    self._replication_tasks_for_periodic_snapshot_tasks(
                        bisect_by_class(ReplicationTask, self.tasks)[0], periodic_snapshot_tasks))
                self._spawn_replication_tasks(scheduled.datetime.offset_aware_datetime, replication_tasks)

                assert tasks == []

    def _run_periodic_snapshot_tasks(self, now, tasks):
        scheduled_tasks = []
        for task in tasks:
            snapshot_name = get_snapshot_name(now, task.naming_schema)

            try:
                parsed_snapshot_name = parse_snapshot_name(snapshot_name, task.naming_schema)
            except ValueError as e:
                logger.warning(
                    "Unable to parse snapshot name %r with naming schema %r: %s. Skipping task %r",
                    snapshot_name,
                    task.naming_schema,
                    str(e),
                    task,
                )

                notify(self.observer, PeriodicSnapshotTaskError(task.id, "Unable to parse snapshot name %r: %s" % (
                    snapshot_name,
                    str(e),
                )))
                continue

            scheduled_tasks.append(ScheduledPeriodicSnapshotTask(
                task,
                snapshot_name,
                parsed_snapshot_name,
            ))

        scheduled_tasks = sorted(scheduled_tasks, key=lambda scheduled_task: (
            # Common sorting order
            parsed_snapshot_sort_key(scheduled_task.parsed_snapshot_name),
            # Recursive snapshot with same name as non-recursive should go first
            0 if scheduled_task.task.recursive else 1,
            # Recursive snapshots without exclude should go first
            0 if not scheduled_task.task.exclude else 1,
        ))

        tasks_with_snapshot_names = [
            (scheduled_task.task, scheduled_task.snapshot_name)
            for scheduled_task in scheduled_tasks
        ]

        created_snapshots = set()
        for task, snapshot_name in tasks_with_snapshot_names:
            snapshot = Snapshot(task.dataset, snapshot_name)
            if snapshot in created_snapshots:
                notify(self.observer, PeriodicSnapshotTaskSuccess(task.id))
                continue

            options = notify(self.observer, PeriodicSnapshotTaskStart(task.id))
            try:
                create_snapshot(self.local_shell, snapshot, task.recursive, task.exclude, options.properties)
            except CreateSnapshotError as e:
                logger.warning("Error creating %r: %r", snapshot, e)

                notify(self.observer, PeriodicSnapshotTaskError(task.id, str(e)))
            else:
                logger.info("Created %r", snapshot)
                created_snapshots.add(snapshot)

                notify(self.observer, PeriodicSnapshotTaskSuccess(task.id))

        empty_snapshots = get_empty_snapshots_for_deletion(self.local_shell, tasks_with_snapshot_names)
        if empty_snapshots:
            logger.info("Destroying empty snapshots: %r", empty_snapshots)
            destroy_snapshots(self.local_shell, empty_snapshots)

    def _replication_tasks_for_periodic_snapshot_tasks(self, replication_tasks, periodic_snapshot_tasks):
        result = []
        for replication_task in replication_tasks:
            if (
                replication_task.auto and
                replication_task.schedule is None and
                any(periodic_snapshot_task in replication_task.periodic_snapshot_tasks
                    for periodic_snapshot_task in periodic_snapshot_tasks)
            ):
                result.append(replication_task)

        return result

    def _spawn_replication_tasks(self, now: datetime, replication_tasks):
        with self.tasks_lock:
            for replication_task in replication_tasks:
                if any(rt == replication_task for now, rt in self.pending_tasks):
                    logger.info("Replication task %r is already pending", replication_task)
                    continue

                if (reason := self._cannot_spawn_replication_task_reason(replication_task)) is None:
                    self._spawn_replication_task(now, replication_task)
                else:
                    logger.info("Replication task %r can't execute in parallel because %r, delaying it",
                                replication_task, reason)
                    notify(self.observer, ReplicationTaskScheduled(replication_task.id, reason))
                    self.pending_tasks.append((now, replication_task))

            if not self.pending_tasks and not self.running_tasks and not self.retention_running:
                self._spawn_retention()

    def _can_spawn_replication_task(self, replication_task: ReplicationTask):
        return self._cannot_spawn_replication_task_reason(replication_task) is None

    def _cannot_spawn_replication_task_reason(self, replication_task: ReplicationTask):
        if self.retention_running:
            return "Waiting for retention to complete"

        if self.max_parallel_replication_tasks is not None:
            if len(self.running_tasks) >= self.max_parallel_replication_tasks:
                return f"Waiting for {len(self.running_tasks)} running replication tasks to finish"

        for running_task in self.running_tasks:
            if not self._replication_tasks_can_run_in_parallel(replication_task, running_task):
                logger.info("Replication tasks %r and %r cannot run in parallel", replication_task, running_task)
                return "Waiting for another concurrent replication task to complete"

        return None

    def _replication_tasks_can_run_in_parallel(self, t1: ReplicationTask, t2: ReplicationTask):
        if t1.direction == t2.direction:
            if not are_same_host(t1.transport, t2.transport):
                return True

            return not any(
                (
                    is_child(get_target_dataset(t1, dataset1), get_target_dataset(t2, dataset2)) or
                    is_child(get_target_dataset(t2, dataset2), get_target_dataset(t1, dataset1))
                )
                for dataset1 in t1.source_datasets
                for dataset2 in t2.source_datasets
            )
        else:
            if t1.direction == ReplicationDirection.PULL and t2.direction == ReplicationDirection.PUSH:
                t1, t2 = t2, t1
            # Now t1 is PUSH, t2 is PULL

            return (
                # Do not write to local dataset from which we are pushing
                all(
                    (
                        not is_child(t2.target_dataset, source_dataset) and
                        not is_child(source_dataset, t2.target_dataset)
                    )
                    for source_dataset in t1.source_datasets
                ) and

                # Do not write to remote dataset from which we are pulling
                all(
                    (
                        not is_child(t1.target_dataset, source_dataset) and
                        not is_child(source_dataset, t1.target_dataset)
                    )
                    for source_dataset in t2.source_datasets
                )
            )

    def _spawn_replication_task(self, now: datetime, replication_task):
        self.running_tasks.append(replication_task)
        threading.Thread(name=f"replication_task__{replication_task.id}",
                         target=functools.partial(self._run_replication_task, now, replication_task)).start()

    def _run_replication_task(self, now: datetime, replication_task: ReplicationTask):
        try:
            shell = replication_task.transport.shell(replication_task.transport)
            try:
                ReplicationTaskLoggingLevelFilter.levels[replication_task.id] = replication_task.logging_level
                run_replication_tasks(now, self.local_shell, replication_task.transport, shell, [replication_task],
                                      self.observer)
            finally:
                shell.close()
        except Exception:
            logger.error("Unhandled exception while running replication task %r", replication_task, exc_info=True)
        finally:
            with self.tasks_lock:
                self.running_tasks.remove(replication_task)

                self._spawn_pending_tasks()

                if not self.running_tasks and not self.retention_running:
                    self._spawn_retention()

    def _spawn_pending_tasks(self):
        for now, pending_task in list(self.pending_tasks):
            if self._can_spawn_replication_task(pending_task):
                self._spawn_replication_task(now, pending_task)
                self.pending_tasks.remove((now, pending_task))

    def _spawn_retention(self):
        self.retention_running = True
        threading.Thread(name=f"retention", target=self._run_retention).start()

    def _run_retention(self):
        self.retention_shells = {}
        try:
            try:
                self._run_local_retention(self.retention_datetime)
                self._run_remote_retention(self.retention_datetime)
            finally:
                for shell in self.retention_shells.values():
                    shell.close()
        except Exception:
            logger.error("Unhandled exception while running retention", exc_info=True)

        with self.tasks_lock:
            self.retention_running = False
            self._spawn_pending_tasks()

    def _transport_for_replication_tasks(self, replication_tasks):
        return sortedgroupby(replication_tasks, lambda replication_task: replication_task.transport, False)

    def _is_push_replication_task(self, replication_task: ReplicationTask):
        return replication_task.direction == ReplicationDirection.PUSH

    def _is_pull_replication_task(self, replication_task: ReplicationTask):
        return replication_task.direction == ReplicationDirection.PULL

    def _get_retention_shell(self, transport):
        if transport not in self.retention_shells:
            self.retention_shells[transport] = transport.shell(transport)

        return self.retention_shells[transport]

    def _run_local_retention(self, now: datetime):
        periodic_snapshot_tasks = select_by_class(PeriodicSnapshotTask, self.tasks)
        replication_tasks = select_by_class(ReplicationTask, self.tasks)

        push_replication_tasks_that_can_hold = [replication_task for replication_task in replication_tasks
                                                if replication_task.hold_pending_snapshots]
        pull_replications_tasks = list(filter(self._is_pull_replication_task, replication_tasks))

        snapshot_removal_date_owner = None
        if self.use_removal_dates:
            try:
                removal_dates = get_removal_dates()
            except Exception:
                logger.warning("Skipping local retention: unhandled exception getting snapshot removal dates",
                               exc_info=True)
                return
            else:
                if removal_dates is None:
                    logger.warning("Skipping local retention: snapshot removal dates are not loaded yet")
                    return

                snapshot_removal_date_owner = SnapshotRemovalDateSnapshotOwner(now, removal_dates)

        local_snapshots_queries = []
        local_snapshots_queries.extend([
            (periodic_snapshot_task.dataset, periodic_snapshot_task.recursive)
            for periodic_snapshot_task in periodic_snapshot_tasks
        ])
        local_snapshots_queries.extend(replication_tasks_source_datasets_queries(push_replication_tasks_that_can_hold))
        local_snapshots_queries.extend([
            (replication_task.target_dataset, replication_task.recursive)
            for replication_task in pull_replications_tasks
        ])
        if snapshot_removal_date_owner:
            local_snapshots_queries.extend([(dataset, False) for dataset in snapshot_removal_date_owner.datasets])
        local_snapshots = multilist_snapshots(self.local_shell, local_snapshots_queries)
        local_snapshots_grouped = group_snapshots_by_datasets(local_snapshots)

        owners = []
        owners.extend([
            PeriodicSnapshotTaskSnapshotOwner(now, periodic_snapshot_task)
            for periodic_snapshot_task in periodic_snapshot_tasks
        ])

        # These are always only PUSH replication tasks
        for transport, replication_tasks in self._transport_for_replication_tasks(push_replication_tasks_that_can_hold):
            shell = self._get_retention_shell(transport)
            owners.extend(pending_push_replication_task_snapshot_owners(local_snapshots_grouped, shell,
                                                                        replication_tasks))

        if snapshot_removal_date_owner:
            owners.append(snapshot_removal_date_owner)

        for transport, replication_tasks in self._transport_for_replication_tasks(pull_replications_tasks):
            shell = self._get_retention_shell(transport)
            remote_snapshots_queries = replication_tasks_source_datasets_queries(replication_tasks)
            try:
                remote_snapshots = multilist_snapshots(shell, remote_snapshots_queries)
            except Exception as e:
                logger.warning("Local retention failed: error listing snapshots on %r: %r", transport, e)
                return
            remote_snapshots_grouped = group_snapshots_by_datasets(remote_snapshots)
            owners.extend([
                executed_pull_replication_task_snapshot_owner(now, replication_task, remote_snapshots_grouped,
                                                              local_snapshots_grouped)
                for replication_task in replication_tasks
            ])

        snapshots_to_destroy = calculate_snapshots_to_remove(owners, local_snapshots)
        logger.info("Retention destroying local snapshots: %r", snapshots_to_destroy)
        destroy_snapshots(self.local_shell, snapshots_to_destroy)

    def _run_remote_retention(self, now: datetime):
        push_replication_tasks = list(
            filter(self._is_push_replication_task, select_by_class(ReplicationTask, self.tasks)))
        local_snapshots_grouped = group_snapshots_by_datasets(multilist_snapshots(
            self.local_shell, replication_tasks_source_datasets_queries(push_replication_tasks)))
        for transport, replication_tasks in self._transport_for_replication_tasks(push_replication_tasks):
            shell = self._get_retention_shell(transport)
            remote_snapshots_queries = replication_tasks_target_datasets_queries(replication_tasks)
            try:
                # Prevent hanging remote from breaking all the replications
                with ShellTimeoutContext(3600):
                    remote_snapshots = multilist_snapshots(shell, remote_snapshots_queries)
            except Exception as e:
                logger.warning("Remote retention failed on %r: error listing snapshots: %r",
                               transport, e)
                continue
            remote_snapshots_grouped = group_snapshots_by_datasets(remote_snapshots)
            owners = [
                ExecutedReplicationTaskSnapshotOwner(now, replication_task, local_snapshots_grouped,
                                                     remote_snapshots_grouped)
                for replication_task in replication_tasks
            ]

            snapshots_to_destroy = calculate_snapshots_to_remove(owners, remote_snapshots)
            logger.info("Retention on %r destroying snapshots: %r", transport, snapshots_to_destroy)
            try:
                # Prevent hanging remote from breaking all the replications
                with ShellTimeoutContext(3600):
                    destroy_snapshots(shell, snapshots_to_destroy)
            except Exception as e:
                logger.warning("Remote retention failed on %r: error destroying snapshots: %r",
                               transport, e)
                continue
