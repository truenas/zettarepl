# -*- coding=utf-8 -*-
import enum
import logging

from zettarepl.retention.snapshot_owner import SnapshotOwner
from zettarepl.snapshot.list import multilist_snapshots, group_snapshots_by_datasets
from zettarepl.snapshot.name import *
from zettarepl.transport.interface import Shell

from .dataset import get_target_dataset
from .naming_schema import replication_task_naming_schemas
from .should_replicate import *
from .task import ReplicationTask

logger = logging.getLogger(__name__)

__all__ = ["PendingReplicationTaskSnapshotOwner"]


class BaseReplicationTaskSnapshotOwner(SnapshotOwner):
    class Side(enum.Enum):
        LOCAL = 1
        ENUM = 2

    def __init__(self, replication_task: ReplicationTask, side: Side):
        assert replication_task.hold_pending_snapshots

        self.replication_task = replication_task
        self.side = side

        self.naming_schemas = replication_task_naming_schemas(replication_task)

    def get_naming_schemas(self):
        return self.naming_schemas

    def owns_dataset(self, dataset: str):
        return (
            replication_task_should_replicate_dataset(self.replication_task, dataset)
            if self.side == BaseReplicationTaskSnapshotOwner.Side.LOCAL
            else replication_task_replicates_target_dataset(self.replication_task, dataset)
        )

    def owns_snapshot(self, parsed_snapshot_name: ParsedSnapshotName):
        return replication_task_should_replicate_parsed_snapshot(self.replication_task, parsed_snapshot_name)


class PendingReplicationTaskSnapshotOwner(BaseReplicationTaskSnapshotOwner):
    def __init__(self, replication_task: ReplicationTask, src_snapshots: {str: [str]}, dst_snapshots: {str: [str]}):
        super().__init__(replication_task, BaseReplicationTaskSnapshotOwner.Side.LOCAL)
        self.src_snapshots = src_snapshots
        self.dst_snapshots = dst_snapshots

    def should_retain(self, dataset: str, parsed_snapshot_name: ParsedSnapshotName):
        target_dataset = get_target_dataset(self.replication_task, dataset)
        return (
            replication_task_should_replicate_dataset(self.replication_task, dataset) and
            parsed_snapshot_name.name in self.src_snapshots.get(dataset, []) and
            parsed_snapshot_name.name not in self.dst_snapshots.get(target_dataset, [])
        )


def pending_replication_task_snapshot_owners(src_snapshots: {str: [str]},
                                             shell: Shell, replication_tasks: [ReplicationTask]):
    replication_tasks = [replication_task for replication_task in replication_tasks
                         if replication_task.hold_pending_snapshots]

    if replication_tasks:
        try:
            dst_snapshots = group_snapshots_by_datasets(
                multilist_snapshots(shell, [(replication_task.target_dataset, replication_task.recursive)
                                            for replication_task in replication_tasks])
            )
        except Exception:
            logger.error("Failed to list snapshots with shell %r, assuming remote has no snapshots", shell,
                         exc_info=True)
            dst_snapshots = {}

        return [PendingReplicationTaskSnapshotOwner(replication_task, src_snapshots, dst_snapshots)
                for replication_task in replication_tasks]

    return []
