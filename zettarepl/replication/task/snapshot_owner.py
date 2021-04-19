# -*- coding=utf-8 -*-
from datetime import datetime
import enum
import logging

from zettarepl.dataset.relationship import is_child
from zettarepl.retention.snapshot_owner import SnapshotOwner
from zettarepl.snapshot.list import multilist_snapshots, group_snapshots_by_datasets
from zettarepl.snapshot.name import *
from zettarepl.transport.interface import Shell

from .dataset import *
from .naming_schema import replication_task_naming_schemas
from .should_replicate import *
from .task import ReplicationTask

logger = logging.getLogger(__name__)

__all__ = ["PendingPushReplicationTaskSnapshotOwner", "pending_push_replication_task_snapshot_owners",
           "ExecutedReplicationTaskSnapshotOwner", "executed_pull_replication_task_snapshot_owner"]


class BaseReplicationTaskSnapshotOwner(SnapshotOwner):
    class Side(enum.Enum):
        SOURCE = 1
        TARGET = 2

    def __init__(self, replication_task: ReplicationTask, side: Side):
        self.replication_task = replication_task
        self.side = side

        self.naming_schemas = replication_task_naming_schemas(replication_task)

    def get_naming_schemas(self):
        return self.naming_schemas

    def owns_dataset(self, dataset: str):
        if self.side == BaseReplicationTaskSnapshotOwner.Side.SOURCE:
            return replication_task_should_replicate_dataset(self.replication_task, dataset)

        if self.side == BaseReplicationTaskSnapshotOwner.Side.TARGET:
            return replication_task_replicates_target_dataset(self.replication_task, dataset)

        raise ValueError(self.side)

    def owns_snapshot(self, dataset: str, parsed_snapshot_name: ParsedSnapshotName):
        return replication_task_should_replicate_parsed_snapshot(self.replication_task, parsed_snapshot_name)

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.replication_task.id!r}>"


class PendingPushReplicationTaskSnapshotOwner(BaseReplicationTaskSnapshotOwner):
    def __init__(self, replication_task: ReplicationTask, src_snapshots: {str: [str]}, dst_snapshots: {str: [str]}):
        assert replication_task.hold_pending_snapshots

        super().__init__(replication_task, BaseReplicationTaskSnapshotOwner.Side.SOURCE)
        self.src_snapshots = src_snapshots
        self.dst_snapshots = dst_snapshots

    def wants_to_delete(self):
        return False

    def should_retain(self, dataset: str, parsed_snapshot_name: ParsedSnapshotName):
        target_dataset = get_target_dataset(self.replication_task, dataset)
        return (
            replication_task_should_replicate_dataset(self.replication_task, dataset) and
            parsed_snapshot_name.name in self.src_snapshots.get(dataset, []) and
            parsed_snapshot_name.name not in self.dst_snapshots.get(target_dataset, [])
        )


def pending_push_replication_task_snapshot_owners(src_snapshots: {str: [str]}, shell: Shell,
                                                  replication_tasks: [ReplicationTask]):
    replication_tasks = [replication_task for replication_task in replication_tasks
                         if replication_task.hold_pending_snapshots]

    if replication_tasks:
        dst_snapshots_queries = [(replication_task.target_dataset, replication_task.recursive)
                                 for replication_task in replication_tasks]
        try:
            dst_snapshots = multilist_snapshots(shell, dst_snapshots_queries)
        except Exception as e:
            logger.error("Failed to list snapshots with %r: %r. Assuming remote has no snapshots", shell, e)
            dst_snapshots = {}
        else:
            dst_snapshots = group_snapshots_by_datasets(dst_snapshots)

        return [PendingPushReplicationTaskSnapshotOwner(replication_task, src_snapshots, dst_snapshots)
                for replication_task in replication_tasks]

    return []


class ExecutedReplicationTaskSnapshotOwner(BaseReplicationTaskSnapshotOwner):
    def __init__(self, now: datetime, replication_task: ReplicationTask, src_snapshots: {str: [str]},
                 dst_snapshots: {str: [str]}):
        self.now = now
        super().__init__(replication_task, BaseReplicationTaskSnapshotOwner.Side.TARGET)
        self.src_snapshots = src_snapshots
        self.dst_snapshots = dst_snapshots

        parsed_src_snapshots_names = {
            dataset: parse_snapshots_names_with_multiple_schemas(snapshots, self.get_naming_schemas())
            for dataset, snapshots in self.src_snapshots.items()
        }
        parsed_dst_snapshots_names = {
            dataset: parse_snapshots_names_with_multiple_schemas(snapshots, self.get_naming_schemas())
            for dataset, snapshots in self.dst_snapshots.items()
        }

        self.delete_snapshots = {
            dst_dataset: self.replication_task.retention_policy.calculate_delete_snapshots(
                now,
                parsed_src_snapshots_names.get(get_source_dataset(self.replication_task, dst_dataset), []),
                parsed_dst_snapshots_names.get(dst_dataset, []),
            )
            for dst_dataset in self.dst_snapshots.keys()
        }

    def wants_to_delete(self):
        return True

    def should_retain(self, dataset: str, parsed_snapshot_name: ParsedSnapshotName):
        return (
            self.owns_dataset(dataset) and
            parsed_snapshot_name.name not in self.delete_snapshots[dataset]
        )


def executed_pull_replication_task_snapshot_owner(now: datetime, replication_task: ReplicationTask,
                                                  remote_snapshots: {str: [str]}, local_snapshots: {str: [str]}):
    return ExecutedReplicationTaskSnapshotOwner(
        now, replication_task, remote_snapshots,
        {
            dataset: snapshots
            for dataset, snapshots in local_snapshots.items()
            if is_child(dataset, replication_task.target_dataset)
        }
    )
