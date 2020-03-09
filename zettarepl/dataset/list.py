# -*- coding=utf-8 -*-
import logging

from zettarepl.transport.interface import Shell

logger = logging.getLogger(__name__)

__all__ = ["list_datasets", "list_datasets_with_properties"]


def list_datasets(shell: Shell, dataset: str=None, recursive: bool=True):
    return [dataset["name"] for dataset in list_datasets_with_properties(shell, dataset, recursive)]


def list_datasets_with_properties(shell: Shell, dataset: str=None, recursive: bool=True, properties=None):
    properties = properties or []

    properties = ["name"] + properties

    args = ["zfs", "list", "-t", "filesystem,volume", "-H", "-o", ",".join(properties), "-s", "name"]
    if recursive:
        args.extend(["-r"])
    else:
        args.extend(["-d", "1"])
    if dataset is not None:
        args.append(dataset)

    return [dict(zip(properties, line.split("\t"))) for line in filter(None, shell.exec(args).split("\n"))]
