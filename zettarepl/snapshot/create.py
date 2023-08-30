# -*- coding=utf-8 -*-
import io
import logging
import os
import re
import typing
import tempfile

from zettarepl.transport.interface import *
from zettarepl.transport.utils import put_buffer
from zettarepl.dataset.list import list_datasets
from zettarepl.dataset.exclude import should_exclude
from zettarepl.zcp.render_zcp import render_zcp

from .snapshot import Snapshot

logger = logging.getLogger(__name__)

__all__ = ["CreateSnapshotError", "create_snapshot"]


class CreateSnapshotError(Exception):
    pass


def iterate_excluded_datasets(exclude_rules: [str], datasets: typing.Iterable):
    for dataset in datasets:
        if should_exclude(dataset, exclude_rules):
            yield dataset


def create_snapshot(shell: Shell, snapshot: Snapshot, recursive: bool, exclude_rules: [str], properties: {str: typing.Any}):
    logger.info("On %r creating %s snapshot %r", shell, "recursive" if recursive else "non-recursive", snapshot)

    if exclude_rules:
        # TODO: support adding properties to snapshots created by channel program

        pool_name = snapshot.dataset.split("/")[0]

        snapshot_program = io.BytesIO()
        render_zcp(
            snapshot_program,
            snapshot.dataset,
            snapshot.name,
            iterate_excluded_datasets(exclude_rules, list_datasets(shell, snapshot.dataset, recursive)),
        )
        program = put_buffer(snapshot_program, "recursive_snapshot_exclude.lua", shell)

        args = ["zfs", "program", pool_name, program]

        try:
            shell.exec(args)
        except ExecException as e:
            logger.debug(e)
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
