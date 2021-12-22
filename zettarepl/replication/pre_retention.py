# -*- coding=utf-8 -*-
from datetime import datetime
import logging

from zettarepl.dataset.relationship import is_child
from zettarepl.retention.calculate import calculate_snapshots_to_remove
from zettarepl.snapshot.destroy import destroy_snapshots
from zettarepl.snapshot.snapshot import Snapshot
from zettarepl.transport.timeout import ShellTimeoutContext

from .snapshots_to_send import get_parsed_incremental_base
from .task.dataset import get_source_dataset
from .task.snapshot_owner import ExecutedReplicationTaskSnapshotOwner
from .task.task import ReplicationTask

logger = logging.getLogger(__name__)

__all__ = ["pre_retention"]


class RetentionBeforePushReplicationSnapshotOwner(ExecutedReplicationTaskSnapshotOwner):
    def __init__(self, target_dataset: str, *args, **kwargs):
        self.target_dataset = target_dataset
        super().__init__(*args, **kwargs)

        for dst_dataset, snapshots in self.delete_snapshots.items():
            incremental_base = get_parsed_incremental_base(
                self.parsed_src_snapshots_names.get(get_source_dataset(self.replication_task, dst_dataset), []),
                self.parsed_dst_snapshots_names[dst_dataset]
            )
            if incremental_base:
                try:
                    snapshots.remove(incremental_base)
                except ValueError:
                    pass

    def owns_dataset(self, dataset: str):
        # FIXME: Replication tasks that have multiple source datasets are executed as independent parts.
        # Retention has to be executed as independent parts too. Part 2 retention will not be executed until part 1
        # replication is completed. That might lead to disk space / quota overflow which could have been prevented
        # if all retentions were executed first.
        return is_child(dataset, self.target_dataset) and super().owns_dataset(dataset)


def pre_retention(now: datetime, replication_task: ReplicationTask, source_snapshots: {str: [str]},
                  target_snapshots: {str: [str]}, target_dataset: str, target_shell):
    owners = [
        RetentionBeforePushReplicationSnapshotOwner(target_dataset, now, replication_task, source_snapshots,
                                                    target_snapshots)
    ]
    remote_snapshots = sum(
        [
            [
                Snapshot(dataset, snapshot)
                for snapshot in snapshots
            ]
            for dataset, snapshots in target_snapshots.items()
        ],
        []
    )

    snapshots_to_destroy = calculate_snapshots_to_remove(owners, remote_snapshots)
    logger.info("Pre-retention destroying snapshots: %r", snapshots_to_destroy)
    with ShellTimeoutContext(3600):
        destroy_snapshots(target_shell, snapshots_to_destroy)
