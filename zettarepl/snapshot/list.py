# -*- coding=utf-8 -*-
import logging
import os

from zettarepl.transport.interface import Shell

logger = logging.getLogger(__name__)

__all__ = ["list_snapshots"]


def list_snapshots(shell: Shell, mountpoint: str) -> [str]:
    return list(filter(None, shell.exec(["ls", "-1", os.path.join(mountpoint, ".zfs/snapshot")]).split("\n")))
