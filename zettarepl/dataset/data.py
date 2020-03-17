# -*- coding=utf-8 -*-
import logging

from zettarepl.replication.error import ReplicationError
from zettarepl.transport.interface import ExecException, Shell
from zettarepl.transport.zfscli import get_properties

logger = logging.getLogger(__name__)

__all__ = ["ensure_has_no_data"]


def ensure_has_no_data(shell: Shell, dataset: str):
    try:
        dst_properties = get_properties(shell, dataset, {
            "type": str,
            "mounted": bool,
            "mountpoint": str,
            "referenced": int,
            "snapdir": str,
            "used": int,
        })
    except ExecException as e:
        if not ("dataset does not exist" in e.stdout):
            raise
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
                    "An exception occurred while listing dataset %r mountpoint %r: %r. Assuming dataset is not mounted",
                    dataset, dst_properties["mountpoint"], e,
                )
            else:
                if dst_properties["snapdir"] == "visible" and ".zfs" in index:
                    index.remove(".zfs")

                if index:
                    raise ReplicationError(
                        f"Target dataset {dataset!r} does not have snapshots but has data (e.g. {index[0]!r} and "
                        f"replication from scratch is not allowed. Refusing to overwrite existing data."
                    )

                return

        if dst_properties["type"] == "filesystem":
            used_property = "used"
        elif dst_properties["type"] == "volume":
            used_property = "referenced"
        else:
            raise ReplicationError(f"Target dataset {dataset!r} has invalid type {dst_properties['type']!r}")

        # Empty datasets on large pool configurations can have really big size
        if dst_properties[used_property] > 1024 * 1024 * 10:
            raise ReplicationError(
                f"Target dataset {dataset!r} does not have snapshots but has data ({dst_properties[used_property]} "
                f"bytes used) and replication from scratch is not allowed. Refusing to overwrite existing data."
            )
