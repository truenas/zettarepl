# -*- coding=utf-8 -*-
import logging
import os
import re
import typing

from zettarepl.transport.interface import *
from zettarepl.transport.utils import put_file

from .snapshot import Snapshot

logger = logging.getLogger(__name__)

__all__ = ["CreateSnapshotError", "create_snapshot"]


class CreateSnapshotError(Exception):
    pass


def create_snapshot(shell: Shell, snapshot: Snapshot, recursive: bool, exclude: [str], properties: {str: typing.Any}):
    logger.info("On %r creating %s snapshot %r", shell, "recursive" if recursive else "non-recursive", snapshot)

    if exclude:
        # TODO: support adding properties to snapshots created by channel program

        program = put_file("zcp/recursive_snapshot_exclude.lua", shell)

        pool_name = snapshot.dataset.split("/")[0]

        args = ["zfs", "program", pool_name, program, snapshot.dataset, snapshot.name] + exclude

        try:
            shell.exec(args)
        except ExecException as e:
            errors = []
            for snapshot, error in re.findall(r"snapshot=(.+?) error=([0-9]+)", e.stdout):
                errors.append((snapshot, os.strerror(int(error))))
            if errors:
                raise CreateSnapshotError(
                    "Failed to create following snapshots:\n" +
                    "\n".join([f"{snapshot!r}: {error}" for snapshot, error in errors])
                ) from None
            else:
                raise CreateSnapshotError(e) from None
    else:
        args = ["zfs", "snapshot"]

        if recursive:
            args.extend(["-r"])

        if properties:
            args.extend(sum([["-o", f"{k}={v}"] for k, v in properties.items()], []))

        args.append(str(snapshot))

        try:
            shell.exec(args)
        except ExecException as e:
            raise CreateSnapshotError(e) from None

    return
