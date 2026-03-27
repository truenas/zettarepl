# -*- coding=utf-8 -*-
import logging
import re

logger = logging.getLogger(__name__)

__all__ = ["warnings_from_zfs_success"]


def warnings_from_zfs_success(stdout: str) -> list[str]:
    if re.search(r"cannot receive .+ property", stdout):
        return stdout.splitlines()

    return []
