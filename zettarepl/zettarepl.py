# -*- coding=utf-8 -*-
from datetime import datetime
import logging

from zettarepl.observer import notify, PeriodicSnapshotTaskStart, PeriodicSnapshotTaskSuccess, PeriodicSnapshotTaskError
from zettarepl.replication.run import run_replication_tasks
from zettarepl.replication.task.direction import ReplicationDirection
from zettarepl.replication.task.snapshot_owner import *
from zettarepl.replication.task.task import *
from zettarepl.retention.calculate import calculate_snapshots_to_remove
from zettarepl.snapshot.create import *
from zettarepl.snapshot.destroy import destroy_snapshots
from zettarepl.snapshot.empty import get_empty_snapshots_for_deletion
from zettarepl.snapshot.list import *
from zettarepl.snapshot.snapshot import Snapshot
from zettarepl.snapshot.task.snapshot_owner import PeriodicSnapshotTaskSnapshotOwner
from zettarepl.snapshot.task.task import PeriodicSnapshotTask
from zettarepl.utils.itertools import bisect, bisect_by_class, select_by_class, sortedgroupby

logger = logging.getLogger(__name__)

__all__ = ["Zettarepl"]


def replication_tasks_source_datasets_queries(replication_tasks: [ReplicationTask]):
    return sum([
        [
            (source_dataset, replication_task.recursive)
            for source_dataset in replication_task.source_datasets
        ]
        for replication_task in replication_tasks
    ], [])


class Zettarepl:
    def __init__(self, scheduler, local_shell):
        self.scheduler = scheduler
        self.local_shell = local_shell

        self.observer = None

        self.tasks = []

        self.shells = {}

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
            logger.info("Scheduled tasks: %r", scheduled)

            tasks = scheduled.tasks

            periodic_snapshot_tasks, tasks = bisect_by_class(PeriodicSnapshotTask, tasks)
            self._run_periodic_snapshot_tasks(scheduled.datetime.datetime, periodic_snapshot_tasks)

            replication_tasks, tasks = bisect_by_class(ReplicationTask, tasks)
            replication_tasks.extend(
                self._replication_tasks_for_periodic_snapshot_tasks(
                    bisect_by_class(ReplicationTask, self.tasks)[0], periodic_snapshot_tasks))
            self._run_replication_tasks(replication_tasks)

            assert tasks == []

            self._run_local_retention(scheduled.datetime.datetime)
            self._run_remote_retention(scheduled.datetime.datetime)

            for shell in self.shells.values():
                shell.close()

            self.shells = {}

    def _run_periodic_snapshot_tasks(self, now, tasks):
        tasks_with_snapshot_names = sorted(
            [(task, now.strftime(task.naming_schema)) for task in tasks],
            key=lambda task_with_snapshot_name: (
                # Lexicographically less snapshots should go first
                task_with_snapshot_name[1],
                # Recursive snapshot with same name as non-recursive should go first
                0 if task_with_snapshot_name[0].recursive else 1,
                # Recursive snapshots without exclude should go first
                0 if not task_with_snapshot_name[0].exclude else 1,
            )
        )

        created_snapshots = set()
        for task, snapshot_name in tasks_with_snapshot_names:
            snapshot = Snapshot(task.dataset, snapshot_name)
            if snapshot in created_snapshots:
                continue

            notify(self.observer, PeriodicSnapshotTaskStart(task.id))
            try:
                create_snapshot(self.local_shell, snapshot, task.recursive, task.exclude)
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
            if any(periodic_snapshot_task in replication_task.periodic_snapshot_tasks
                   for periodic_snapshot_task in periodic_snapshot_tasks):
                result.append(replication_task)

        return result

    def _run_replication_tasks(self, replication_tasks):
        for transport, replication_tasks in self._transport_for_replication_tasks(replication_tasks):
            remote_shell = self._get_shell(transport)

            push_replication_tasks, replication_tasks = bisect(self._is_push_replication_task, replication_tasks)
            run_replication_tasks(self.local_shell, transport, remote_shell, push_replication_tasks, self.observer)

            pull_replication_tasks, replication_tasks = bisect(self._is_pull_replication_task, replication_tasks)
            run_replication_tasks(self.local_shell, transport, remote_shell, pull_replication_tasks, self.observer)

            assert replication_tasks == []

    def _transport_for_replication_tasks(self, replication_tasks):
        return sortedgroupby(replication_tasks, lambda replication_task: replication_task.transport)

    def _is_push_replication_task(self, replication_task: ReplicationTask):
        return replication_task.direction == ReplicationDirection.PUSH

    def _is_pull_replication_task(self, replication_task: ReplicationTask):
        return replication_task.direction == ReplicationDirection.PULL

    def _get_shell(self, transport):
        if transport not in self.shells:
            self.shells[transport] = transport.shell(transport)

        return self.shells[transport]

    def _run_local_retention(self, now: datetime):
        periodic_snapshot_tasks = select_by_class(PeriodicSnapshotTask, self.tasks)
        replication_tasks = select_by_class(ReplicationTask, self.tasks)

        push_replication_tasks_that_can_hold = [replication_task for replication_task in replication_tasks
                                                if replication_task.hold_pending_snapshots]
        pull_replications_tasks = list(filter(self._is_pull_replication_task, replication_tasks))

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
        local_snapshots = multilist_snapshots(self.local_shell, local_snapshots_queries)
        local_snapshots_grouped = group_snapshots_by_datasets(local_snapshots)

        owners = []
        owners.extend([
            PeriodicSnapshotTaskSnapshotOwner(now, periodic_snapshot_task)
            for periodic_snapshot_task in periodic_snapshot_tasks
        ])

        # These are always only PUSH replication tasks
        for transport, replication_tasks in self._transport_for_replication_tasks(push_replication_tasks_that_can_hold):
            shell = self._get_shell(transport)
            owners.extend(pending_push_replication_task_snapshot_owners(local_snapshots_grouped, shell,
                                                                        replication_tasks))

        for transport, replication_tasks in self._transport_for_replication_tasks(pull_replications_tasks):
            shell = self._get_shell(transport)
            remote_snapshots_grouped = group_snapshots_by_datasets(multilist_snapshots(
                shell, replication_tasks_source_datasets_queries(replication_tasks)))
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
            shell = self._get_shell(transport)
            remote_snapshots = multilist_snapshots(shell, [
                (replication_task.target_dataset, replication_task.recursive)
                for replication_task in replication_tasks
            ])
            remote_snapshots_grouped = group_snapshots_by_datasets(remote_snapshots)
            owners = [
                ExecutedReplicationTaskSnapshotOwner(now, replication_task, local_snapshots_grouped,
                                                     remote_snapshots_grouped)
                for replication_task in replication_tasks
            ]

            snapshots_to_destroy = calculate_snapshots_to_remove(owners, remote_snapshots)
            logger.info("Retention on transport %r destroying snapshots: %r", transport, snapshots_to_destroy)
            destroy_snapshots(shell, snapshots_to_destroy)
