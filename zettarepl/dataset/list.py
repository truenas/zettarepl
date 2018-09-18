# -*- coding=utf-8 -*-
import logging

from zettarepl.transport.interface import Shell

logger = logging.getLogger(__name__)

__all__ = ["list_datasets"]


def list_datasets(shell: Shell, dataset: str, recursive: bool):
    args = ["zfs", "list", "-t", "filesystem", "-H", "-o", "name", "-s", "name"]
    if recursive:
        args.extend(["-r"])
    else:
        args.extend(["-d", "1"])
    args.append(dataset)
    return list(filter(None, shell.exec(args).split("\n")))
