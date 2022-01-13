# -*- coding=utf-8 -*-
import atexit
import contextlib
import logging
import os
import shutil
import signal
import subprocess

from zettarepl.replication.error import ReplicationConfigurationError
from zettarepl.utils.shlex import pipe

from .async_exec_tee import AsyncExecTee
from .encryption_context import EncryptionContext
from .interface import *
from .progress_report_mixin import ProgressReportMixin
from .utils import get_properties_override
from .zfscli import *
from .zfscli.exception import ZfsSendRecvExceptionHandler

logger = logging.getLogger(__name__)

__all__ = ["LocalShell", "LocalTransport"]

_pgids = set()


@atexit.register
def _kill_pgids():
    for pgid in _pgids:
        with contextlib.suppress(Exception):
            os.killpg(pgid, signal.SIGTERM)


class LocalAsyncExec(AsyncExec):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.process = None
        self.pgid = None

    def run(self):
        self.logger.debug("Running %r", self.args)
        self.process = subprocess.Popen(self.args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                        encoding=self.encoding, preexec_fn=os.setsid)
        try:
            self.pgid = os.getpgid(self.process.pid)
        except ProcessLookupError:
            pass
        else:
            _pgids.add(self.pgid)
        self._copy_stdout_from(self.process.stdout)

    def wait(self, timeout=None):
        if self.stdout is None:
            try:
                stdout, stderr = self.process.communicate(timeout=timeout)
            except subprocess.TimeoutExpired:
                self.logger.debug("Timeout")
                self.process.kill()
                raise TimeoutError()
            finally:
                if self.pgid is not None:
                    _pgids.discard(self.pgid)
        else:
            try:
                self.process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                self.logger.debug("Timeout")
                self.process.kill()
                raise TimeoutError()
            finally:
                if self.pgid is not None:
                    _pgids.discard(self.pgid)
                self.process.stdout.close()
            stdout = None

        if self.process.returncode != 0:
            self.logger.debug("Error %r: %r", self.process.returncode, stdout)
            raise ExecException(self.process.returncode, stdout)

        self.logger.debug("Success: %r", stdout)
        return stdout

    def stop(self):
        self.logger.debug("Stopping")
        if self.pgid is not None:
            _pgids.discard(self.pgid)
            with contextlib.suppress(ProcessLookupError):
                os.killpg(self.pgid, signal.SIGTERM)
        with contextlib.suppress(ProcessLookupError):
            self.process.terminate()
        try:
            self.process.wait(10)
        except subprocess.TimeoutExpired:
            logger.warning("Timeout waiting for process to terminate properly, killing process")
            if self.pgid is not None:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(self.pgid, signal.SIGKILL)
            with contextlib.suppress(ProcessLookupError):
                self.process.kill()


class LocalShell(Shell):
    async_exec = LocalAsyncExec

    def __init__(self, transport=None):
        super().__init__(transport or LocalTransport())

    def close(self):
        pass

    def exists(self, path):
        return os.path.exists(path)

    def ls(self, path):
        return os.listdir(path)

    def is_dir(self, path):
        return os.path.isdir(path)

    def put_file(self, f, dst_path):
        with open(dst_path, "wb") as f2:
            shutil.copyfileobj(f, f2)


class LocalReplicationProcess(ReplicationProcess, ProgressReportMixin):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.async_exec = None
        self.encryption_context = None

    def run(self):
        if self.compression is not None:
            raise ReplicationConfigurationError("compression is not supported for local replication (it has no sense)")

        if self.speed_limit is not None:
            raise ReplicationConfigurationError("speed-limit is not supported for local replication (it has no sense)")

        report_progress = self._zfs_send_can_report_progress()

        send = zfs_send(self.source_dataset,
                        self.snapshot,
                        self.properties,
                        self.replicate,
                        self.incremental_base,
                        self.include_intermediate,
                        self.receive_resume_token,
                        self.dedup,
                        self.large_block,
                        self.embed,
                        self.compressed,
                        self.raw,
                        report_progress)

        if self.encryption:
            self.encryption_context = EncryptionContext(self, self.local_shell)

        properties_override = get_properties_override(self, self.encryption_context)

        recv = zfs_recv(self.target_dataset, self.properties_exclude, properties_override)

        send = self._wrap_send(send)

        self.async_exec = AsyncExecTee(self.local_shell, pipe(send, recv))
        self.async_exec.run()

        if report_progress:
            self._start_progress_observer()

    def wait(self):
        success = False
        try:
            with ZfsSendRecvExceptionHandler(self):
                self.async_exec.wait()
                success = True
        finally:
            if self.encryption_context is not None:
                self.encryption_context.exit(success)

            self._stop_progress_observer()

    def stop(self):
        return self.async_exec.stop()

    def _get_send_shell(self):
        return self.local_shell


class LocalTransport(Transport):
    logger = logger

    @classmethod
    def from_data(cls, data):
        return LocalTransport()

    def _descriptor(self):
        return 1

    def __repr__(self):
        return "<LocalTransport()>"

    shell = LocalShell

    replication_process = LocalReplicationProcess
