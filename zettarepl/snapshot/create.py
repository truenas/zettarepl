# -*- coding=utf-8 -*-
import logging

from zettarepl.transport.interface import *
from zettarepl.transport.utils import put_file

from .snapshot import Snapshot

logger = logging.getLogger(__name__)

__all__ = ["CreateSnapshotError", "create_snapshot"]


class CreateSnapshotError(Exception):
    pass


def create_snapshot(shell: Shell, snapshot: Snapshot, recursive: bool, exclude: [str]):
    logger.info("On %r creating %s snapshot %r", shell, "recursive" if recursive else "non-recursive", snapshot)

    if exclude:
        program = put_file("zcp/recursive_snapshot_exclude.lua", shell)

        pool_name = snapshot.dataset.split("/")[0]

        args = ["zfs", "program", pool_name, program, snapshot.dataset, snapshot.name] + exclude
    else:
        args = ["zfs", "snapshot"]

        if recursive:
            args.extend(["-r"])

        args.append(str(snapshot))

    try:
        shell.exec(args)
    except ExecException as e:
        raise CreateSnapshotError(e)

    return
