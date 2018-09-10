# -*- coding=utf-8 -*-
import logging
import shutil
import subprocess

from zettarepl.utils.shlex import pipe

from .interface import Transport, Shell, ExecException
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


class LocalShell(Shell):
    def exec(self, args, encoding="utf8"):
        logger.debug("Running %r", args)

        result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding=encoding)
        if result.returncode != 0:
            logger.debug("Error %r: %r", result.returncode, result.stdout)
            raise ExecException(result.returncode, result.stdout)

        logger.debug("Success: %r", result.stdout)
        return result.stdout

    def put_file(self, f, dst_path):
        with open(dst_path, "wb") as f2:
            shutil.copyfileobj(f, f2)
