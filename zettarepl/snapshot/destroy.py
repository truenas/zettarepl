# -*- coding=utf-8 -*-
import re
import logging

from zettarepl.transport.interface import ExecException, Shell
from zettarepl.utils.itertools import sortedgroupby

from .snapshot import Snapshot

logger = logging.getLogger(__name__)

__all__ = ["destroy_snapshots"]


def destroy_snapshots(shell: Shell, snapshots: [Snapshot]):
    for dataset, snapshots in sortedgroupby(snapshots, lambda snapshot: snapshot.dataset):
        names = [snapshot.name for snapshot in snapshots]

        logger.info("On %r for dataset %r destroying snapshots %r", shell, dataset, names)

        while names:
            args = ["zfs", "destroy", f"{dataset}@" + ",".join(names)]
            try:
                shell.exec(args)
                break
            except ExecException as e:
                m = re.search(r"cannot destroy snapshot .+?@(.+?): dataset is busy", e.stdout)
                if m is None:
                    m = re.search(r"cannot destroy .+?@(.+?)': snapshot has dependent clones", e.stdout)
                if m is None:
                    raise

                name = m.group(1)
                logger.info("Snapshot %r on dataset %r is busy, skipping", name, dataset)
                names.remove(name)
