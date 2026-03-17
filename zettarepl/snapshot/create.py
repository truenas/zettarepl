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
    def __init__(self, error, snapshots_errors: list[tuple[Snapshot, str]]):
        self.error = error
        self.snapshots_errors = snapshots_errors
        super().__init__(error, snapshots_errors)

    def __str__(self):
        lines = ["Failed to create following snapshots:\n"]

        datasets = sorted(set(s.dataset for s, _ in self.snapshots_errors))
        errors_by_dataset = {s.dataset: error for s, error in self.snapshots_errors}

        # Find top-level datasets (not nested under another failing dataset)
        top_level = []
        for ds in datasets:
            if not any(ds != other and ds.startswith(other + "/") for other in datasets):
                top_level.append(ds)

        for ds in top_level:
            name = self.snapshots_errors[0][0].name
            error = errors_by_dataset[ds]
            nested = sum(1 for other in datasets if other != ds and other.startswith(ds + "/"))
            snapshot_str = f"{ds}@{name}"
            if nested > 0:
                lines.append(f"'{snapshot_str}' (and {nested} nested datasets): {error}")
            else:
                lines.append(f"'{snapshot_str}': {error}")

        return "\n".join(lines) + "\n"


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
            snapshots_errors = []
            for snapshot, error in re.findall(r"snapshot=(.+?) error=([0-9]+)", e.stdout):
                snapshot_error = os.strerror(int(error))
                if snapshot_error == "File exists":
                    snapshot_error = "snapshot already exists"

                snapshots_errors.append((Snapshot(*snapshot.split("@", 1)), snapshot_error))
            if snapshots_errors:
                raise CreateSnapshotError("no snapshots were created", snapshots_errors) from None
            else:
                raise CreateSnapshotError("no snapshots were created", []) from None
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
            error = e.stdout
            snapshots_errors = []
            for line, snapshot_id, snapshot_error in re.findall(r"(cannot create snapshot (.+): (.+)\n)", error):
                error = error.replace(line, "")
                if snapshot_error == "dataset already exists":
                    snapshot_error = "snapshot already exists"
                snapshots_errors.append((Snapshot(*snapshot_id.strip("'").split("@", 1)), snapshot_error))

            raise CreateSnapshotError(error.strip(), snapshots_errors) from None

    return
