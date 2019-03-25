# -*- coding=utf-8 -*-
import logging

logger = logging.getLogger(__name__)

__all__ = ["zfs_send", "zfs_recv", "get_receive_resume_token"]


def zfs_send(source_dataset: str, snapshot: str, recursive: bool, incremental_base: str, receive_resume_token: str,
             dedup: bool, large_block: bool, embed: bool, compressed: bool, report_progress=False):
    send = ["zfs", "send", "-p"]

    if recursive:
        send.append("-R")

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
    receive_resume_token = shell.exec(["zfs", "get", "-H", "receive_resume_token", dataset]).split("\t")[2]
    return None if receive_resume_token == "-" else receive_resume_token
