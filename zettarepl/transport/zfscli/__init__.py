# -*- coding=utf-8 -*-
import logging

logger = logging.getLogger(__name__)

__all__ = ["zfs_send", "zfs_recv", "get_receive_resume_token", "get_property"]


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
             report_progress=False):
    send = ["zfs", "send"]

    if dedup:
        send.append("-D")

    if large_block:
        send.append("-L")

    if embed:
        send.append("-e")

    if compressed:
        send.append("-c")

    if report_progress:
        send.append("-V")

    if receive_resume_token is None:
        assert snapshot is not None

        if replicate:
            send.append("-R")
        else:
            if properties:
                send.append("-p")

        if incremental_base is not None:
            send.extend(["-i", f"{source_dataset}@{incremental_base}"])

        send.append(f"{source_dataset}@{snapshot}")
    else:
        assert snapshot is None
        assert incremental_base is None

        send.extend(["-t", receive_resume_token])

    return send


def zfs_recv(target_dataset):
    return ["zfs", "recv", "-s", "-F", target_dataset]


def get_receive_resume_token(shell, dataset):
    return get_property(shell, dataset, "receive_resume_token")


def get_property(shell, dataset, property, type=str):
    value = shell.exec(["zfs", "get", "-H", "-p", property, dataset]).split("\t")[2]
    return None if value == "-" else type(value)
