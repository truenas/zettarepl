# -*- coding=utf-8 -*-
import logging

from zettarepl.transport.shell.interface import *

from . import Snapshot

logger = logging.getLogger(__name__)

__all__ = ["CreateSnapshotError", "create_snapshot"]


class CreateSnapshotError(Exception):
    pass


def create_snapshot(shell: Shell, snapshot: Snapshot, recursive: bool, recursive_exclude: [str]):
    logger.info("On %r creating %s snapshot %r", shell, "recursive" if recursive else "non-recursive", snapshot)

    args = ["zfs", "snapshot"]

    if recursive:
        args.extend(["-r"])

    args.extend(snapshot)

    try:
        shell.exec(args)
    except ExecException as e:
        raise CreateSnapshotError(e)

    return
