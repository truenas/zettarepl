# -*- coding=utf-8 -*-
from datetime import datetime
import logging

from zettarepl.replication.run import run_replication_tasks
from zettarepl.replication.task.direction import ReplicationDirection
from zettarepl.replication.task.snapshot_owner import pending_replication_task_snapshot_owners
from zettarepl.replication.task.task import *
from zettarepl.retention.calculate import calculate_snapshots_to_remove
from zettarepl.snapshot.create import *
from zettarepl.snapshot.destroy import destroy_snapshots
from zettarepl.snapshot.empty import get_empty_snapshots_for_deletion
from zettarepl.snapshot.list import multilist_snapshots
from zettarepl.snapshot.snapshot import Snapshot
from zettarepl.snapshot.task.snapshot_owner import PeriodicSnapshotTaskSnapshotOwner
from zettarepl.snapshot.task.task import PeriodicSnapshotTask
from zettarepl.utils.itertools import bisect, bisect_by_class, select_by_class, sortedgroupby

logger = logging.getLogger(__name__)

__all__ = ["Zettarepl"]


class Zettarepl:
    def __init__(self, scheduler, local_shell):
        self.scheduler = scheduler
        self.local_shell = local_shell

        self.tasks = []

        self.shells = []

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

            for shell in self.shells:
                shell.close()

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

            try:
                create_snapshot(self.local_shell, snapshot, task.recursive, task.exclude)
            except CreateSnapshotError as e:
                logger.warning("Error creating %r: %r", snapshot, e)
            else:
                logger.info("Created %r", snapshot)
                created_snapshots.add(snapshot)

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
            run_replication_tasks(self.local_shell, transport, remote_shell, push_replication_tasks)

            pull_replication_tasks, replication_tasks = bisect(self._is_pull_replication_task, replication_tasks)
            run_replication_tasks(self.local_shell, transport, remote_shell, pull_replication_tasks)

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

        owners = []

        owners.extend([
            PeriodicSnapshotTaskSnapshotOwner(now, periodic_snapshot_task)
            for periodic_snapshot_task in periodic_snapshot_tasks
        ])

        replication_tasks_that_can_hold = [replication_task for replication_task in replication_tasks
                                           if replication_task.hold_pending_snapshots]
        snapshots = multilist_snapshots(self.local_shell, [
            (replication_task.target_dataset, replication_task.recursive)
            for replication_task in replication_tasks_that_can_hold
        ])
        for transport, replication_tasks in self._transport_for_replication_tasks(replication_tasks_that_can_hold):
            shell = self._get_shell(transport)
            owners.extend(pending_replication_task_snapshot_owners(snapshots, shell, replication_tasks))

        snapshots_to_destroy = calculate_snapshots_to_remove(owners, snapshots)
        logger.info("Retention destroying snapshots: %r", snapshots_to_destroy)
        destroy_snapshots(self.local_shell, snapshots_to_destroy)
