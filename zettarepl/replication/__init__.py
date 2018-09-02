# -*- coding=utf-8 -*-
import logging

from zettarepl.replication.task import *
from zettarepl.snapshot import *
from zettarepl.snapshot.create import *
from zettarepl.snapshot.task import *
from zettarepl.utils.itertools import bisect, bisect_by_class, sortedgroupby

logger = logging.getLogger(__name__)

__all__ = ["Replication"]


class Replication:
    def __init__(self, scheduler, local_shell):
        self.scheduler = scheduler
        self.local_shell = local_shell

        self.tasks = []

    def set_tasks(self, tasks):
        self.tasks = tasks

        self.scheduler.set_tasks(list(filter(self._is_scheduler_task, tasks)))

    def _is_scheduler_task(self, task):
        if isinstance(task, PeriodicSnapshotTask):
            return True

        if isinstance(task, ReplicationTask):
            return task.auto

        return False

    def run(self):
        while True:
            scheduled = self.scheduler.schedule()
            logger.info("Scheduled tasks: %r", scheduled)

            tasks = scheduled.tasks

            periodic_snapshot_tasks, tasks = bisect_by_class(PeriodicSnapshotTask, tasks)
            self._run_periodic_snapshot_tasks(scheduled.datetime, periodic_snapshot_tasks)

            replication_tasks, tasks = bisect_by_class(ReplicationTask, tasks)
            replication_tasks.extend(
                self._replication_tasks_for_periodic_snapshot_tasks(
                    bisect_by_class(ReplicationTask, self.tasks)[0], periodic_snapshot_tasks))
            self._run_replication_tasks(replication_tasks)

            assert tasks == []

    def _run_periodic_snapshot_tasks(self, now, tasks):
        tasks_with_snapshot_names = sorted(
            [(task, now.strftime(task.naming_schema)) for task in tasks],
            # Lexicographically less snapshots should go first
            # Recursive snapshot with same name as non-recursive should go first
            key=lambda task_with_snapshot_name: (task_with_snapshot_name[1], not task_with_snapshot_name[0].recursive)
        )

        created_snapshots = set()
        for task, snapshot_name in tasks_with_snapshot_names:
            snapshot = Snapshot(task.dataset, snapshot_name)
            if snapshot in created_snapshots:
                continue

            try:
                create_snapshot(self.local_shell, snapshot, task.recursive, task.recursive_exclude)
            except CreateSnapshotError as e:
                logger.warning("Error creating %r: %r", snapshot, e)
            else:
                logger.info("Created %r", snapshot)
                created_snapshots.add(snapshot)

    def _replication_tasks_for_periodic_snapshot_tasks(self, replication_tasks, periodic_snapshot_tasks):
        result = []
        for replication_task in replication_tasks:
            if any(periodic_snapshot_task in replication_task.periodic_snapshot_tasks
                   for periodic_snapshot_task in periodic_snapshot_tasks):
                result.append(replication_task)

        return result

    def _run_replication_tasks(self, replication_tasks):
        for transport, replication_tasks in sortedgroupby(
                replication_tasks, lambda replication_task: replication_task.transport):
            push_replication_tasks, replication_tasks = bisect(
                lambda replication_task: replication_task.direction == ReplicationDirection.PUSH, replication_tasks)
            self._run_push_replication_tasks(transport, replication_tasks)

            #pull_replication_tasks, replication_tasks = bisect(
            #    lambda replication_task: replication_task.direction == ReplicationDirection.PUSH, replication_tasks)

    def _run_push_replication_tasks(self, transport, replication_tasks):
        shell = transport.create_shell()
        replication_tasks = sorted(replication_tasks, key=lambda replication_task: (
            replication_task.source_dataset,
            not replication_task.recursive,
        ))
        for replication_task in replication_tasks:
            pass
            #dst_mountpoint =