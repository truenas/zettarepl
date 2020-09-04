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

from .interface import *
from .zfscli import *
from .zfscli.exception import ZfsCliExceptionHandler

logger = logging.getLogger(__name__)

__all__ = ["LocalShell", "LocalTransport"]


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
            atexit.register(self._atexit)
        self._copy_stdout_from(self.process.stdout)

    def wait(self):
        if self.stdout is None:
            stdout, stderr = self.process.communicate()
        else:
            self.process.wait()
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

    def _atexit(self):
        with contextlib.suppress(ProcessLookupError):
            os.killpg(self.pgid, signal.SIGTERM)


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


class LocalReplicationProcess(ReplicationProcess):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.async_exec = None

    def run(self):
        if self.compression is not None:
            raise ReplicationConfigurationError("compression is not supported for local replication (it has no sense)")

        if self.speed_limit is not None:
            raise ReplicationConfigurationError("speed-limit is not supported for local replication (it has no sense)")

        self.async_exec = self.local_shell.exec_async(
            pipe(
                zfs_send(self.source_dataset,
                         self.snapshot,
                         self.properties,
                         self.replicate,
                         self.incremental_base,
                         self.receive_resume_token,
                         self.dedup,
                         self.large_block,
                         self.embed,
                         self.compressed,
                         self.raw),
                zfs_recv(self.target_dataset)
            )
        )

    def wait(self):
        with ZfsCliExceptionHandler(self):
            self.async_exec.wait()

    def stop(self):
        return self.async_exec.stop()


class LocalTransport(Transport):
    logger = logger

    @classmethod
    def from_data(cls, data):
        return LocalTransport()

    def __hash__(self):
        return 1

    def __repr__(self):
        return "<LocalTransport()>"

    shell = LocalShell

    replication_process = LocalReplicationProcess
