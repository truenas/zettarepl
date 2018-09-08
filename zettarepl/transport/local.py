# -*- coding=utf-8 -*-
import logging

from zettarepl.utils.shlex import pipe

from .interface import Transport
from .shell.interface import Shell
from .shell.local import LocalShell
from .zfscli import *

logger = logging.getLogger(__name__)

__all__ = ["LocalTransport"]


class LocalTransport(Transport):
    @classmethod
    def from_data(cls, data):
        return LocalTransport()

    def __hash__(self):
        return 1

    def create_shell(self):
        return LocalShell()

    def push_snapshot(self, shell: Shell, source_dataset: str, target_dataset: str, snapshot: str, recursive: bool,
                      incremental_base: str, receive_resume_token: str):
        shell.exec(pipe(zfs_send(source_dataset, snapshot, recursive, incremental_base, receive_resume_token),
                        zfs_recv(target_dataset)))
