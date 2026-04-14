# -*- coding=utf-8 -*-
from __future__ import annotations

from collections.abc import Callable
import itertools
import logging
import queue
import threading
import typing

from zettarepl.replication.task.compression import ReplicationCompression
from zettarepl.replication.task.direction import ReplicationDirection
from zettarepl.replication.task.encryption import ReplicationEncryption
from zettarepl.utils.lang import undefined
from zettarepl.utils.logging import PrefixLoggerAdapter

if typing.TYPE_CHECKING:
    from zettarepl.transport.local import LocalShell

logger = logging.getLogger(__name__)

__all__ = ["AsyncExec", "ExecException", "Shell", "ReplicationProcess", "Transport"]


class AsyncExec:
    logger: logging.Logger | logging.LoggerAdapter[typing.Any]

    _logger_counter = itertools.count(1)

    """
    :param Shell shell: The shell to run command on
    :param [str] args: Command arguments
    :param str encoding: Encoding to decode command output
    :param fd stdout: Queue to stream command output line-by-line instead of returning it upon command completion
    """
    def __init__(
        self,
        shell: Shell,
        args: list[str],
        encoding: str = "utf8",
        stdout: queue.Queue[str | None] | None = None,
    ) -> None:
        self.shell = shell
        self.args = args
        self.encoding = encoding
        self.stdout = stdout

        self.logger = PrefixLoggerAdapter(self.shell.logger, f"async_exec:{next(self._logger_counter)}")

    def run(self) -> None:
        raise NotImplementedError

    def wait(self, timeout: float | None = None) -> str | None:
        raise NotImplementedError

    def stop(self) -> None:
        raise NotImplementedError

    def _copy_stdout_from(self, file_like: typing.IO[str]) -> None:
        def target() -> None:
            try:
                while True:
                    line = self._stdout_file_like_readline(file_like)
                    if not line:
                        break

                    self.stdout.put(line)  # type: ignore[union-attr]
            except Exception as e:
                self.logger.warning("Copying stdout from %r failed: %r", file_like, e)
            finally:
                self.stdout.put(None)  # type: ignore[union-attr]

        if self.stdout is not None:
            threading.Thread(daemon=True, name=f"{threading.current_thread().name}.stdout_copy", target=target).start()

    def _stdout_file_like_readline(self, file_like: typing.IO[str]) -> str:
        return file_like.readline()


class ExecException(Exception):
    def __init__(self, returncode: int, stdout: str | None) -> None:
        self.returncode = returncode
        self.stdout = stdout or ""

        super().__init__(returncode, stdout)

    def __str__(self) -> str:
        return self.stdout.strip() or f"Command failed with code {self.returncode}"


class Shell:
    _logger_counter = itertools.count(1)

    async_exec: type[AsyncExec]

    def __init__(self, transport: Transport) -> None:
        self.transport = transport

        self.logger = PrefixLoggerAdapter(self.transport.logger, f"shell:{next(self._logger_counter)}")

    def close(self) -> None:
        raise NotImplementedError

    @typing.overload
    def exec(
        self,
        args: list[str],
        encoding: str,
        stdout: queue.Queue[str | None],
        timeout: float | object,
    ) -> None: ...

    @typing.overload
    def exec(
        self,
        args: list[str],
        encoding: str = "utf8",
        stdout: None = None,
        timeout: float | object = undefined,
    ) -> str: ...

    def exec(
        self,
        args: list[str],
        encoding: str = "utf8",
        stdout: queue.Queue[str | None] | None = None,
        timeout: float | object = undefined,
    ) -> str | None:
        if timeout is undefined:
            timeout = 600

        return self.exec_async(args, encoding, stdout).wait(timeout)  # type: ignore[arg-type]

    def exec_async(
        self,
        args: list[str],
        encoding: str = "utf8",
        stdout: queue.Queue[str | None] | None = None
    ) -> AsyncExec:
        async_exec = self.async_exec(self, args, encoding, stdout)
        async_exec.run()
        return async_exec

    def exists(self, path: str) -> bool:
        raise NotImplementedError

    def ls(self, path: str) -> list[str]:
        raise NotImplementedError

    def is_dir(self, path: str) -> bool:
        raise NotImplementedError

    def put_file(self, f: typing.IO[bytes], dst_path: str) -> None:
        raise NotImplementedError

    def __repr__(self) -> str:
        return "<Shell(%r)>" % self.transport


class ReplicationProcess:
    def __init__(
        self,
        replication_task_id: str,
        transport: Transport,
        local_shell: LocalShell,
        remote_shell: Shell,
        direction: ReplicationDirection,
        source_dataset: str,
        target_dataset: str,
        snapshot: str | None,
        mount: bool,
        properties: bool,
        properties_exclude: list[str],
        properties_override: dict[str, str],
        replicate: bool,
        encryption: ReplicationEncryption | None,
        incremental_base: str | None,
        include_intermediate: bool,
        receive_resume_token: str | None,
        compression: ReplicationCompression | None,
        speed_limit: int | None,
        dedup: bool,
        large_block: bool,
        embed: bool,
        compressed: bool,
        raw: bool,
    ) -> None:
        self.replication_task_id = replication_task_id
        self.transport = transport
        self.local_shell = local_shell
        self.remote_shell = remote_shell
        self.direction = direction
        self.source_dataset = source_dataset
        self.target_dataset = target_dataset
        self.snapshot = snapshot
        self.mount = mount
        self.properties = properties
        self.properties_exclude = properties_exclude
        self.properties_override = properties_override
        self.replicate = replicate
        self.encryption = encryption
        self.incremental_base = incremental_base
        self.include_intermediate = include_intermediate
        self.receive_resume_token = receive_resume_token
        self.compression = compression
        self.speed_limit = speed_limit
        self.dedup = dedup
        self.large_block = large_block
        self.embed = embed
        self.compressed = compressed
        self.raw = raw

        self.logger = PrefixLoggerAdapter(self.transport.logger, f"replication_process:{replication_task_id}")

        self.progress_observers: list[Callable[[int, int], None]] = []
        self.warning_observers: list[Callable[[str], None]] = []

    def add_progress_observer(self, progress_observer: Callable[[int, int], None]) -> None:
        self.progress_observers.append(progress_observer)

    def notify_progress_observer(self, bytes_sent: int, bytes_total: int) -> None:
        for progress_observer in self.progress_observers:
            try:
                progress_observer(bytes_sent, bytes_total)
            except Exception:
                self.logger.warning("Error notifying replication progress observer %r", progress_observer,
                                    exc_info=True)

    def add_warning_observer(self, warning_observer: Callable[[str], None]) -> None:
        self.warning_observers.append(warning_observer)

    def notify_warning_observer(self, warning: str) -> None:
        self.logger.info("Warning: %r", warning)

        for warning_observer in self.warning_observers:
            try:
                warning_observer(warning)
            except Exception:
                self.logger.warning("Error notifying replication warning observer %r", warning_observer,
                                    exc_info=True)

    def run(self) -> None:
        raise NotImplementedError

    def wait(self) -> None:
        raise NotImplementedError

    def stop(self) -> None:
        raise NotImplementedError


class Transport:
    logger: logging.Logger | logging.LoggerAdapter[typing.Any]

    shell: type[Shell]

    replication_process: type[ReplicationProcess]

    @classmethod
    def from_data(cls, data: dict[str, typing.Any]) -> Transport:
        raise NotImplementedError

    def __hash__(self) -> int:
        return hash(self._descriptor())

    def __eq__(self, other: typing.Any) -> bool:
        return (
            isinstance(other, Transport) and
            other.__class__ == self.__class__ and
            other._descriptor() == self._descriptor()
        )

    def _descriptor(self) -> object:
        raise NotImplementedError
