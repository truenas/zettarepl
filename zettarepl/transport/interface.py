# -*- coding=utf-8 -*-
import logging

from zettarepl.replication.task.direction import ReplicationDirection

logger = logging.getLogger(__name__)

__all__ = ["AsyncExec", "ExecException", "Shell", "ReplicationProcess", "Transport"]


class AsyncExec:
    def __init__(self, shell, args, encoding="utf8", stdout=None):
        self.shell = shell
        self.args = args
        self.encoding = encoding
        self.stdout = stdout

    def run(self):
        raise NotImplementedError

    def wait(self):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError


class ExecException(Exception):
    pass


class Shell:
    async_exec: AsyncExec.__class__ = NotImplemented

    def __init__(self, transport):
        self.transport = transport

    def exec(self, args, encoding="utf8", stdout=None):
        return self.exec_async(args, encoding, stdout).wait()

    def exec_async(self, args, encoding="utf8", stdout=None):
        async_exec = self.async_exec(self, args, encoding, stdout)
        async_exec.run()
        return async_exec

    def put_file(self, f, dst_path):
        raise NotImplementedError


class ReplicationProcess:
    def __init__(self,
                 local_shell: Shell, remote_shell: Shell,
                 direction: ReplicationDirection,
                 source_dataset: str, target_dataset: str,
                 snapshot: str, recursive: bool,
                 incremental_base: str, receive_resume_token: str,
                 speed_limit: int):
        self.local_shell = local_shell
        self.remote_shell = remote_shell
        self.direction = direction
        self.source_dataset = source_dataset
        self.target_dataset = target_dataset
        self.snapshot = snapshot
        self.recursive = recursive
        self.incremental_base = incremental_base
        self.receive_resume_token = receive_resume_token
        self.speed_limit = speed_limit

    def run(self):
        raise NotImplementedError

    def wait(self):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError


class Transport:
    shell: Shell.__class__ = NotImplementedError

    replication_process: ReplicationProcess.__class__ = NotImplementedError

    @classmethod
    def from_data(cls, data):
        raise NotImplementedError

    def __hash__(self):
        raise NotImplementedError
