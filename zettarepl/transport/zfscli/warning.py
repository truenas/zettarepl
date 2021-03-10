# -*- coding=utf-8 -*-
import logging
import re

logger = logging.getLogger(__name__)

__all__ = ["warnings_from_zfs_success"]


def warnings_from_zfs_success(stdout):
    if re.search(r"cannot receive .+ property", stdout):
        return [stdout.rstrip("\n")]

    return []
