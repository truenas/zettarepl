# -*- coding=utf-8 -*-
from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from zettarepl.transport.interface import Shell

from .exception import ZfsCliExceptionHandler
from .parse import zfs_bool

logger = logging.getLogger(__name__)

__all__ = ["zfs_send", "zfs_recv", "get_receive_resume_token", "get_properties_recursive", "get_properties",
           "get_property"]


def zfs_send(source_dataset: str,
             snapshot: str | None,
             properties: bool,
             replicate: bool,
             incremental_base: str | None,
             include_intermediate: bool,
             receive_resume_token: str | None,
             dedup: bool,
             large_block: bool,
             embed: bool,
             compressed: bool,
             raw: bool,
             report_progress: bool = False) -> list[str]:
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
            if include_intermediate:
                send.append("-I")
            else:
                send.append("-i")

            send.append(f"{source_dataset}@{incremental_base}")

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


def zfs_recv(target_dataset: str, mount: bool, properties_exclude: list[str],
             properties_override: dict[str, str]) -> list[str]:
    result = ["zfs", "recv", "-s", "-F"]

    if not mount:
        result.append("-u")

    result.extend(sum([["-x", property] for property in properties_exclude], []))
    result.extend(sum([["-o", f"{property}={value}"] for property, value in properties_override.items()], []))

    result.append(target_dataset)

    return result


def get_receive_resume_token(shell: "Shell", dataset: str) -> str | None:
    return get_property(shell, dataset, "receive_resume_token")


def get_properties_recursive(shell: "Shell", datasets: list[str],
                             properties: dict[str, Callable],
                             include_source: bool = False,
                             recursive: bool = False) -> dict[str, dict[str, object]]:
    with ZfsCliExceptionHandler():
        cmd = ["zfs", "get", "-H", "-p", "-t", "filesystem,volume"]
        if recursive:
            cmd.append("-r")
        cmd.append(",".join(properties.keys()))
        cmd.extend(datasets)
        output = shell.exec(cmd)

    result = {}
    for line in output.strip().split("\n"):
        name, property, value, source = line.split("\t", 3)
        result.setdefault(name, {})
        result[name][property] = parse_property(value, properties[property])
        if include_source:
            result[name][property] = result[name][property], source

    return result


def get_properties(shell: Shell, dataset: str, properties: dict[str, Callable],
                   include_source: bool = False) -> dict[str, Any]:
    return get_properties_recursive(shell, [dataset], properties, include_source)[dataset]


def get_property(shell: Shell, dataset: str, property: str, type: Callable = str, include_source: bool = False) -> Any:
    return get_properties(shell, dataset, {property: type}, include_source)[property]


def parse_property(value: str, type: Callable) -> Any:
    if type == bool:
        type = zfs_bool

    if value == "-":
        return None

    return type(value)
