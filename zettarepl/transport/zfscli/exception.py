# -*- coding=utf-8 -*-
import logging
import re
import textwrap

from zettarepl.snapshot.list import list_snapshots
from zettarepl.snapshot.snapshot import Snapshot
from zettarepl.replication.error import ReplicationError, RecoverableReplicationError
from zettarepl.replication.task.direction import ReplicationDirection
from zettarepl.transport.interface import ExecException, ReplicationProcess
from zettarepl.utils.re import re_search_to

logger = logging.getLogger(__name__)

__all__ = ["DatasetDoesNotExistException", "ZfsCliExceptionHandler", "ZfsSendRecvExceptionHandler"]


class DatasetDoesNotExistException(ExecException):
    pass


class ZfsCliExceptionHandler:
    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        if isinstance(exc_val, ExecException):
            if "dataset does not exist" in exc_val.stdout:
                raise DatasetDoesNotExistException(exc_val.returncode, exc_val.stdout) from None


class ZfsSendRecvExceptionHandler:
    def __init__(self, replication_process: ReplicationProcess):
        self.replication_process = replication_process

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        m = {}
        valid_errors = ("failed to create mountpoint.*", "mountpoint or dataset is busy")
        valid_pylibzfs_errors = ("failed to create mountpoint.*",)
        if (
            isinstance(exc_val, ExecException) and
            (
                # Regular zfs CLI
                (
                    re_search_to(
                        m,
                        f"cannot mount '(?P<dataset>.+)': (?P<error>({'|'.join(valid_errors)}))\n",
                        exc_val.stdout,
                    ) and (
                        m["dataset"] == self.replication_process.target_dataset or
                        (
                            m["error"].startswith("failed to create mountpoint") and
                            m["dataset"].endswith(f"/{self.replication_process.target_dataset}")
                        )
                    )
                # py-libzfs
                ) or (
                    re_search_to(
                        m,
                        f"(?P<error>({'|'.join(valid_pylibzfs_errors)}))\n",
                        exc_val.stdout,
                    )
                )
            ) and (
                self.replication_process.properties if m["error"] == "mountpoint or dataset is busy" else True
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
                    "Caught %r and was not able to list snapshots on destination side: %r. Assuming replication "
                    "failure.",
                    m["error"],
                    e
                )
                return

            snapshot = Snapshot(self.replication_process.target_dataset, self.replication_process.snapshot)
            if snapshot not in snapshots:
                logger.warning(
                    "Caught %r and %r does not exist on destination side. Assuming replication failure.",
                    m["error"],
                    snapshot,
                )
                return

            # It's ok, snapshot was transferred successfully, just were not able to mount dataset on specified
            # mountpoint
            logger.info(
                "Caught %r but %r is present on remote side. Assuming replication success.",
                m["error"],
                snapshot,
            )
            return True

        if (
            self.replication_process.incremental_base and
            isinstance(exc_val, ExecException)
        ):
            match = None
            snapshot = None
            incremental_base = None

            # OpenZFS
            m = re.search(r"could not send (?P<snapshot>.+):\s*"
                          r"incremental source \((?P<incremental_base>.+)\) is not earlier than it",
                          exc_val.stdout)
            if m:
                match = m.group(0)
                snapshot = m.group("snapshot")
                incremental_base = m.group("incremental_base")

            # ZoL
            m = re.search(r"warning: cannot send (?P<snapshot>.+): not an earlier snapshot from the same fs",
                          exc_val.stdout)
            if m:
                match = m.group(0)
                snapshot = m.group("snapshot")
                incremental_base = self.replication_process.incremental_base

            if match is not None:
                text = textwrap.dedent(f"""\
                    Replication cannot continue because existing snapshot
                    {incremental_base} is newer than
                    {snapshot}, but has an older date
                    in the snapshot name. To resolve the error, rename
                    {snapshot} with a date that is older than
                    {incremental_base} or delete snapshot
                    {snapshot} from both the source and destination.
                """)
                exc_val.stdout = exc_val.stdout.replace(match, match + f"\n{text.rstrip()}")
                return

        if (
            isinstance(exc_val, ExecException) and
            (
                re.search(r"cannot send .+:\s*signal received", exc_val.stdout) or
                "cannot receive new filesystem stream: checksum mismatch or incomplete stream" in exc_val.stdout
            )
        ):
            raise RecoverableReplicationError(str(exc_val)) from None

        if (
            isinstance(exc_val, ExecException) and
            (
                # OpenZFS
                re.search(r"cannot send .+: snapshot .+ does not exist", exc_val.stdout) or
                # ZoL
                re.search(r"cannot open '.+@.+': dataset does not exist", exc_val.stdout)
            )
        ):
            raise RecoverableReplicationError(str(exc_val)) from None

        if (
            isinstance(exc_val, ExecException) and
            "zfs receive -F cannot be used to destroy an encrypted filesystem" in exc_val.stdout.strip()
        ):
            raise ReplicationError(
                f"Unable to send encrypted dataset {self.replication_process.source_dataset!r} to existing "
                f"unencrypted or unrelated dataset {self.replication_process.target_dataset!r}"
            ) from None
