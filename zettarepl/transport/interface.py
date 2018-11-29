# -*- coding=utf-8 -*-
import itertools
import logging

from zettarepl.replication.task.compression import ReplicationCompression
from zettarepl.replication.task.direction import ReplicationDirection

logger = logging.getLogger(__name__)

__all__ = ["AsyncExec", "ExecException", "Shell", "ReplicationProcess", "Transport"]


class AsyncExec:
    _logger_counter = itertools.count(1)

    def __init__(self, shell, args, encoding="utf8", stdout=None):
        self.shell = shell
        self.args = args
        self.encoding = encoding
        self.stdout = stdout

        self.logger = self.shell.logger.getChild("async_exec").getChild(str(next(self._logger_counter)))

    def run(self):
        raise NotImplementedError

    def wait(self):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError


class ExecException(Exception):
    def __init__(self, returncode, stdout):
        self.returncode = returncode
        self.stdout = stdout

        super().__init__(returncode, stdout)


class Shell:
    _logger_counter = itertools.count(1)

    async_exec: AsyncExec.__class__ = NotImplemented

    def __init__(self, transport):
        self.transport = transport

        self.logger = self.transport.logger.getChild("shell").getChild(str(next(self._logger_counter)))

    def close(self):
        raise NotImplementedError

    def exec(self, args, encoding="utf8", stdout=None):
        return self.exec_async(args, encoding, stdout).wait()

    def exec_async(self, args, encoding="utf8", stdout=None):
        async_exec = self.async_exec(self, args, encoding, stdout)
        async_exec.run()
        return async_exec

    def exists(self, path):
        raise NotImplementedError

    def ls(self, path):
        raise NotImplementedError

    def put_file(self, f, dst_path):
        raise NotImplementedError

    def __repr__(self):
        return "<Shell(%r)>" % self.transport


class ReplicationProcess:
    def __init__(self, replication_task_id, transport,
                 local_shell: Shell, remote_shell: Shell,
                 direction: ReplicationDirection,
                 source_dataset: str, target_dataset: str,
                 snapshot: str, recursive: bool,
                 incremental_base: str, receive_resume_token: str,
                 compression: ReplicationCompression, speed_limit: int,
                 dedup: bool, large_block: bool, embed: bool, compressed: bool):
        self.replication_task_id = replication_task_id
        self.transport = transport
        self.local_shell = local_shell
        self.remote_shell = remote_shell
        self.direction = direction
        self.source_dataset = source_dataset
        self.target_dataset = target_dataset
        self.snapshot = snapshot
        self.recursive = recursive
        self.incremental_base = incremental_base
        self.receive_resume_token = receive_resume_token
        self.compression = compression
        self.speed_limit = speed_limit
        self.dedup = dedup
        self.large_block = large_block
        self.embed = embed
        self.compressed = compressed

        self.logger = self.transport.logger.getChild("replication_process").getChild(replication_task_id)

    def run(self):
        raise NotImplementedError

    def wait(self):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError


class Transport:
    logger: logging.Logger = NotImplementedError

    shell: Shell.__class__ = NotImplementedError

    replication_process: ReplicationProcess.__class__ = NotImplementedError

    @classmethod
    def from_data(cls, data):
        raise NotImplementedError

    def __hash__(self):
        raise NotImplementedError
