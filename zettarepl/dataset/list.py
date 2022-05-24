# -*- coding=utf-8 -*-
import logging

from zettarepl.transport.interface import Shell
from zettarepl.transport.zfscli import ZfsCliExceptionHandler, parse_property

logger = logging.getLogger(__name__)

__all__ = ["list_datasets", "list_datasets_with_properties"]


def list_datasets(shell: Shell, dataset: str=None, recursive: bool=True):
    return [dataset["name"] for dataset in list_datasets_with_properties(shell, dataset, recursive)]


def list_datasets_with_properties(shell: Shell, dataset: str=None, recursive: bool=True, properties=None):
    properties = properties or {}

    properties["name"] = str

    args = ["zfs", "list", "-t", "filesystem,volume", "-H", "-o", ",".join(properties.keys()), "-s", "name"]
    if recursive:
        args.extend(["-r"])
    else:
        args.extend(["-d", "0"])
    if dataset is not None:
        args.append(dataset)

    with ZfsCliExceptionHandler():
        output = shell.exec(args)

    return [
        {
            property: parse_property(value, properties[property])
            for property, value in zip(properties, line.split("\t"))
        }
        for line in filter(None, output.split("\n"))
    ]
