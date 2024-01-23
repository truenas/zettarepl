# -*- coding=utf-8 -*-
import logging
import os

from zettarepl.dataset.relationship import is_immediate_child
from zettarepl.replication.error import ReplicationError
from zettarepl.transport.interface import Shell
from zettarepl.transport.zfscli import get_properties
from zettarepl.transport.zfscli.exception import DatasetDoesNotExistException

logger = logging.getLogger(__name__)

__all__ = ["DatasetIsNotMounted", "list_data", "ensure_has_no_data"]


class DatasetIsNotMounted(Exception):
    pass


def list_data(shell: Shell, dataset: str):
    index, dst_properties = inspect_data(shell, dataset)

    if index is None:
        raise DatasetIsNotMounted()

    return index


def ensure_has_no_data(shell: Shell, dataset: str, allowed_empty_children: [str]):
    allowed_empty_immediate_children = [
        child
        for child in allowed_empty_children
        if is_immediate_child(child, dataset)
    ]

    index, dst_properties = inspect_data(shell, dataset, [
        os.path.basename(child)
        for child in allowed_empty_children
    ])

    for child in allowed_empty_immediate_children:
        ensure_has_no_data(shell, child, allowed_empty_children)

    if index is not None:
        if index:
            raise ReplicationError(
                f"Target dataset {dataset!r} does not have matching snapshots but has data (e.g. {index[0]!r}) and "
                f"replication from scratch is not allowed. Refusing to overwrite existing data."
            )

        return

    if dst_properties is None:
        return

    if dst_properties["type"] == "filesystem":
        used_property = "used"
    elif dst_properties["type"] == "volume":
        used_property = "referenced"
    else:
        raise ReplicationError(f"Target dataset {dataset!r} has invalid type {dst_properties['type']!r}")

    # Empty datasets on large pool configurations can have huge size
    if dst_properties[used_property] > 1024 * 1024 * 10:
        raise ReplicationError(
            f"Target dataset {dataset!r} does not have matching snapshots but has data "
            f"({dst_properties[used_property]} bytes used) and replication from scratch is not allowed. "
            "Refusing to overwrite existing data."
        )


def inspect_data(shell: Shell, dataset: str, exclude: [str]=None):
    exclude = exclude or []

    try:
        dst_properties = get_properties(shell, dataset, {
            "type": str,
            "mounted": bool,
            "mountpoint": str,
            "referenced": int,
            "snapdir": str,
            "used": int,
        })
    except DatasetDoesNotExistException:
        return None, None
    else:
        if (
                dst_properties["type"] == "filesystem" and
                dst_properties["mounted"] and
                dst_properties["mountpoint"] != "legacy"
        ):
            try:
                index = shell.ls(dst_properties["mountpoint"])
            except Exception as e:
                logger.warning(
                    "An exception occurred while listing dataset %r mountpoint %r on shell %r: %r. "
                    "Assuming dataset is not mounted",
                    dataset, dst_properties["mountpoint"], shell, e,
                )
            else:
                if dst_properties["snapdir"] == "visible" and ".zfs" in index:
                    index.remove(".zfs")

                for excluded in exclude:
                    if excluded not in index:
                        continue

                    child_mountpoint = os.path.join(dst_properties["mountpoint"], excluded)
                    try:
                        if not shell.is_dir(child_mountpoint):
                            continue
                    except Exception as e:
                        logger.warning(
                            "An exception occurred while checking if %r on shell %r is a directory: %r. "
                            "Assuming it is not",
                            child_mountpoint, shell, e,
                        )
                        continue

                    child_dataset = os.path.join(dataset, excluded)
                    try:
                        child_properties = get_properties(shell, child_dataset, {
                            "type": str,
                            "mounted": bool,
                            "mountpoint": str,
                        })
                    except Exception as e:
                        logger.warning(
                            "An exception occurred while getting properties for dataset %r on shell %r: %r. "
                            "Assuming it does not exist",
                            child_dataset, shell, e,
                        )
                        continue

                    if child_properties["type"] == "filesystem":
                        if child_properties["mounted"] and child_properties["mountpoint"] == child_mountpoint:
                            index.remove(excluded)
                        else:
                            try:
                                child_contents = shell.ls(child_mountpoint)
                            except Exception as e:
                                logger.warning(
                                    "An exception occurred while listing %r on shell %r: %r. Assuming it is not empty",
                                    child_mountpoint, shell, e,
                                )
                                continue
                            else:
                                if not child_contents:
                                    index.remove(excluded)

                return index, dst_properties

        return None, dst_properties
