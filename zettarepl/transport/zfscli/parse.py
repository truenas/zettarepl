# -*- coding=utf-8 -*-
import logging

logger = logging.getLogger(__name__)

__all__ = ["zfs_bool"]


def zfs_bool(v):
    return {
        "off": False,
        "on": True,

        "no": False,
        "yes": True,
    }[v]
