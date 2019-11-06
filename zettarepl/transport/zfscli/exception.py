# -*- coding=utf-8 -*-
import logging

from zettarepl.snapshot.list import list_snapshots
from zettarepl.snapshot.snapshot import Snapshot
from zettarepl.replication.task.direction import ReplicationDirection
from zettarepl.transport.interface import ExecException, ReplicationProcess

logger = logging.getLogger(__name__)

__all__ = ["ZfsCliExceptionHandler"]


class ZfsCliExceptionHandler:
    def __init__(self, replication_process: ReplicationProcess):
        self.replication_process = replication_process

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        if (
            self.replication_process.properties and
            isinstance(exc_val, ExecException) and
            exc_val.stdout.endswith(
                f"cannot mount '{self.replication_process.target_dataset}': mountpoint or dataset is busy\n"
            )
        ):
            if self.replication_process.direction == ReplicationDirection.PUSH:
                dst_shell = self.replication_process.remote_shell
            else:
                dst_shell = self.replication_process.local_shell

            try:
                snapshots = list_snapshots(dst_shell, self.replication_process.target_dataset, False)
            except Exception as e:
                logger.warning(
                    "Caught 'mountpoint or dataset is busy' and was not able to list snapshots on destination side: "
                    "%r. Assuming replication failure.",
                    e
                )
                return

            snapshot = Snapshot(self.replication_process.target_dataset, self.replication_process.snapshot)
            if snapshot not in snapshots:
                logger.warning(
                    "Caught 'mountpoint or dataset is busy' and %r does not exist on destination side. "
                    "Assuming replication failure.",
                    snapshot,
                )
                return

            # It's ok, snapshot was transferred successfully, just were not able to mount dataset on specified
            # mountpoint
            logger.info(
                "Caught 'mountpoint or dataset is busy' but %r is present on remote side. "
                "Assuming replication success.",
                snapshot,
            )
            return True
