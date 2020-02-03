# -*- coding=utf-8 -*-
import logging

from zettarepl.transport.interface import Shell

logger = logging.getLogger(__name__)

__all__ = ["create_dataset"]


def create_dataset(shell: Shell, dataset: str):
    shell.exec(["zfs", "create", "-p", dataset])
