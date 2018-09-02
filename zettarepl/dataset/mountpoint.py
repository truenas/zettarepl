# -*- coding=utf-8 -*-
import logging
import os

from zettarepl.transport.shell.interface import Shell

from .exclude import should_exclude
from .mtab import Mtab

logger = logging.getLogger(__name__)

__all__ = ["dataset_mountpoints"]


def dataset_mountpoints(shell: Shell, name: str, recursive: bool, exclude: [str], mtab: Mtab):
    args = ["zfs", "list", "-H"]
    if recursive:
        args.append("-r")
    args.extend(["-o", "name,mountpoint", name])

    mountpoints = {}
    for line in shell.exec(args).strip().split("\n"):
        dataset, mountpoint = line.split("\t", 1)

        if should_exclude(dataset, exclude):
            continue

        if mountpoint == "none":
            logger.info("Dataset %r is not mounted", dataset)
            continue

        if mountpoint == "legacy":
            mountpoint = mtab.get(dataset)
            if mountpoint is None:
                logger.warning("Unable to get legacy mountpoint for dataset %r from mount output", dataset)
                continue

        if mountpoint == "-":
            head, tail = os.path.split(dataset)
            mountpoint = os.path.join(mountpoints[head], tail)

        mountpoints[dataset] = mountpoint

    return mountpoints
