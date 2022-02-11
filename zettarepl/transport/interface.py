# -*- coding=utf-8 -*-
import itertools
import logging
import threading

from zettarepl.replication.task.compression import ReplicationCompression
from zettarepl.replication.task.direction import ReplicationDirection
from zettarepl.replication.task.encryption import ReplicationEncryption
from zettarepl.transport.timeout import get_shell_timeout
from zettarepl.utils.lang import undefined
from zettarepl.utils.logging import PrefixLoggerAdapter

logger = logging.getLogger(__name__)

__all__ = ["AsyncExec", "ExecException", "Shell", "ReplicationProcess", "Transport"]


class AsyncExec:
    _logger_counter = itertools.count(1)

    """
    :param Shell shell: The shell to run command on
    :param [str] args: Command arguments
    :param str encoding: Encoding to decode command output
    :param fd stdout: Queue to stream command output line-by-line instead of returning it upon command completion
    """
    def __init__(self, shell, args, encoding="utf8", stdout=None):
        self.shell = shell
        self.args = args
        self.encoding = encoding
        self.stdout = stdout

        self.logger = PrefixLoggerAdapter(self.shell.logger, f"async_exec:{next(self._logger_counter)}")

    def run(self):
        raise NotImplementedError

    def wait(self, timeout=None):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError

    def _copy_stdout_from(self, file_like):
        def target():
            try:
                while True:
                    line = self._stdout_file_like_readline(file_like)
                    if not line:
                        break

                    self.stdout.put(line)
            except Exception as e:
                self.logger.warning("Copying stdout from %r failed: %r", file_like, e)
            finally:
                self.stdout.put(None)

        if self.stdout is not None:
            threading.Thread(daemon=True, name=f"{threading.current_thread().name}.stdout_copy", target=target).start()

    def _stdout_file_like_readline(self, file_like):
        return file_like.readline()


class ExecException(Exception):
    def __init__(self, returncode, stdout):
        self.returncode = returncode
        self.stdout = stdout

        super().__init__(returncode, stdout)

    def __str__(self):
        return self.stdout.strip() or f"Command failed with code {self.returncode}"


class Shell:
    _logger_counter = itertools.count(1)

    async_exec: AsyncExec.__class__ = NotImplemented

    def __init__(self, transport):
        self.transport = transport

        self.logger = PrefixLoggerAdapter(self.transport.logger, f"shell:{next(self._logger_counter)}")

    def close(self):
        raise NotImplementedError

    def exec(self, args, encoding="utf8", stdout=None, timeout=undefined):
        if timeout is undefined:
            timeout = get_shell_timeout()
        return self.exec_async(args, encoding, stdout).wait(timeout)

    def exec_async(self, args, encoding="utf8", stdout=None):
        async_exec = self.async_exec(self, args, encoding, stdout)
        async_exec.run()
        return async_exec

    def exists(self, path):
        raise NotImplementedError

    def ls(self, path):
        raise NotImplementedError

    def is_dir(self, path):
        raise NotImplementedError

    def put_file(self, f, dst_path):
        raise NotImplementedError

    def __repr__(self):
        return "<Shell(%r)>" % self.transport


class ReplicationProcess:
    def __init__(self,
                 replication_task_id,
                 transport,
                 local_shell: Shell,
                 remote_shell: Shell,
                 direction: ReplicationDirection,
                 source_dataset: str,
                 target_dataset: str,
                 snapshot: str,
                 properties: bool,
                 properties_exclude: [str],
                 properties_override: {str: str},
                 replicate: bool,
                 encryption: ReplicationEncryption,
                 incremental_base: str,
                 receive_resume_token: str,
                 compression: ReplicationCompression,
                 speed_limit: int,
                 dedup: bool,
                 large_block: bool,
                 embed: bool,
                 compressed: bool,
                 raw: bool):
        self.replication_task_id = replication_task_id
        self.transport = transport
        self.local_shell = local_shell
        self.remote_shell = remote_shell
        self.direction = direction
        self.source_dataset = source_dataset
        self.target_dataset = target_dataset
        self.snapshot = snapshot
        self.properties = properties
        self.properties_exclude = properties_exclude
        self.properties_override = properties_override
        self.replicate = replicate
        self.encryption = encryption
        self.incremental_base = incremental_base
        self.receive_resume_token = receive_resume_token
        self.compression = compression
        self.speed_limit = speed_limit
        self.dedup = dedup
        self.large_block = large_block
        self.embed = embed
        self.compressed = compressed
        self.raw = raw

        self.logger = PrefixLoggerAdapter(self.transport.logger, f"replication_process:{replication_task_id}")

        self.progress_observers = []
        self.warning_observers = []

    def add_progress_observer(self, progress_observer):
        self.progress_observers.append(progress_observer)

    def notify_progress_observer(self, bytes_sent, bytes_total):
        for progress_observer in self.progress_observers:
            try:
                progress_observer(bytes_sent, bytes_total)
            except Exception:
                self.logger.warning("Error notifying replication progress observer %r", progress_observer,
                                    exc_info=True)

    def add_warning_observer(self, warning_observer):
        self.warning_observers.append(warning_observer)

    def notify_warning_observer(self, warning):
        self.logger.info("Warning: %r", warning)

        for warning_observer in self.warning_observers:
            try:
                warning_observer(warning)
            except Exception:
                self.logger.warning("Error notifying replication warning observer %r", warning_observer,
                                    exc_info=True)

    def run(self):
        raise NotImplementedError

    def wait(self):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError


class Transport:
    logger: logging.Logger = NotImplemented

    shell: Shell.__class__ = NotImplemented

    replication_process: ReplicationProcess.__class__ = NotImplemented

    @classmethod
    def from_data(cls, data):
        raise NotImplementedError

    def __hash__(self):
        return hash(self._descriptor())

    def __eq__(self, other):
        return other.__class__ == self.__class__ and other._descriptor() == self._descriptor()

    def _descriptor(self):
        raise NotImplementedError
