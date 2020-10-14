# -*- coding=utf-8 -*-
import logging

from .exception import ZfsCliExceptionHandler
from .parse import zfs_bool

logger = logging.getLogger(__name__)

__all__ = ["zfs_send", "zfs_recv", "get_receive_resume_token", "get_properties", "get_property"]


def zfs_send(source_dataset: str,
             snapshot: str,
             properties: bool,
             replicate: bool,
             incremental_base: str,
             receive_resume_token: str,
             dedup: bool,
             large_block: bool,
             embed: bool,
             compressed: bool,
             raw: bool,
             report_progress=False):
    send = ["zfs", "send"]

    if embed:
        send.append("-e")

    if report_progress:
        send.append("-V")

    if receive_resume_token is None:
        assert snapshot is not None

        if replicate:
            send.append("-R")
        else:
            if properties:
                send.append("-p")

        if raw:
            send.append("-w")

        if incremental_base is not None:
            send.extend(["-i", f"{source_dataset}@{incremental_base}"])

        if dedup:
            send.append("-D")

        if large_block:
            send.append("-L")

        if compressed:
            send.append("-c")

        send.append(f"{source_dataset}@{snapshot}")
    else:
        assert snapshot is None
        assert incremental_base is None

        send.extend(["-t", receive_resume_token])

    return send


def zfs_recv(target_dataset, properties: dict):
    result = ["zfs", "recv", "-s", "-F"]

    for k, v in properties.items():
        result.extend(["-o", f"{k}={v}"])

    result.append(target_dataset)

    return result


def get_receive_resume_token(shell, dataset):
    return get_property(shell, dataset, "receive_resume_token")


def get_properties(shell, dataset, properties: {str: type}, include_source: bool = False):
    for k, v in properties.items():
        if v == bool:
            properties[k] = zfs_bool

    with ZfsCliExceptionHandler():
        output = shell.exec(["zfs", "get", "-H", "-p", ",".join(properties.keys()), dataset])

    result = {}
    for line in output.strip().split("\n"):
        name, property, value, source = line.split("\t", 3)
        result[property] = None if value == "-" else properties[property](value)
        if include_source:
            result[property] = result[property], source
    return result


def get_property(shell, dataset, property, type=str, include_source: bool = False):
    return get_properties(shell, dataset, {property: type}, include_source)[property]
