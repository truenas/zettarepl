# -*- coding=utf-8 -*-
import re
import logging

from zettarepl.transport.interface import ExecException, Shell
from zettarepl.utils.itertools import sortedgroupby

from .snapshot import Snapshot

logger = logging.getLogger(__name__)

__all__ = ["destroy_snapshots"]

ARG_MAX = 262000  # FreeBSD, on Linux it is even higher
MAX_BATCH_SIZE = 100  # Deleting too many snapshots at once can cause performance issues


def destroy_snapshots(shell: Shell, snapshots: [Snapshot]):
    for dataset, snapshots in sortedgroupby(snapshots, lambda snapshot: snapshot.dataset):
        names = {snapshot.name for snapshot in snapshots}

        logger.info("On %r for dataset %r destroying snapshots %r", shell, dataset, names)

        while names:
            chunk = set()
            sum_len = len(dataset)
            for name in sorted(names):
                if len(chunk) >= MAX_BATCH_SIZE:
                    break

                new_sum_len = sum_len + len(name) + 1
                if new_sum_len >= ARG_MAX:
                    break

                chunk.add(name)
                sum_len = new_sum_len

            args = ["zfs", "destroy", f"{dataset}@" + ",".join(sorted(chunk))]
            try:
                try:
                    shell.exec(args, timeout=3600)  # Destroying snapshots can take a really long time
                except ExecException as e:
                    if "could not find any snapshots to destroy; check snapshot names" in e.stdout:
                        # Snapshots might be already removed by another process
                        pass
                    else:
                        raise

                names -= chunk
            except ExecException as e:
                if m := re.search(r"cannot destroy snapshot .+?@(.+?): dataset is busy", e.stdout):
                    reason = "busy"
                    discard_names = [m.group(1)]
                elif m := re.search(r"cannot destroy '.+?@(.+?)': snapshot has dependent clones", e.stdout):
                    reason = "cloned"
                    discard_names = [m.group(1)]
                elif discard_names := re.findall(r"cannot destroy snapshot .+?@(.+?): it's being held", e.stdout):
                    reason = "held"
                else:
                    raise

                logger.info("Snapshots %r on dataset %r are %s, skipping", discard_names, dataset, reason)
                names -= set(discard_names)
