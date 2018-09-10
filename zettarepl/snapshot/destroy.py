# -*- coding=utf-8 -*-
import logging

from zettarepl.transport.interface import Shell
from zettarepl.utils.itertools import sortedgroupby

from .snapshot import Snapshot

logger = logging.getLogger(__name__)

__all__ = ["destroy_snapshots"]


def destroy_snapshots(shell: Shell, snapshots: [Snapshot]):
    for dataset, snapshots in sortedgroupby(snapshots, lambda snapshot: snapshot.dataset):
        names = [snapshot.name for snapshot in snapshots]

        logger.info("On %r for dataset %r destroying snapshots %r", shell, dataset, names)
        args = ["zfs", "destroy", f"{dataset}@" + "%".join(names)]

        shell.exec(args)
