# -*- coding=utf-8 -*-
import logging
import shutil
import subprocess

from zettarepl.utils.shlex import pipe

from .interface import *
from .zfscli import *

logger = logging.getLogger(__name__)

__all__ = ["LocalShell", "LocalTransport"]


class LocalAsyncExec(AsyncExec):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.process = None

    def run(self):
        logger.debug("Running %r", self.args)
        self.process = subprocess.Popen(self.args, stdout=self.stdout or subprocess.PIPE, stderr=subprocess.STDOUT,
                                        encoding=self.encoding)

    def wait(self):
        stdout, stderr = self.process.communicate()
        if self.process.returncode != 0:
            logger.debug("Error %r: %r", self.process.returncode, stdout)
            raise ExecException(self.process.returncode, stdout)

        logger.debug("Success: %r", stdout)
        return stdout

    def stop(self):
        logger.debug("Terminating process")
        self.process.terminate()
        try:
            self.process.wait(10)
        except subprocess.TimeoutExpired:
            logger.warning("Timeout waiting for process to terminate properly, killing process")
            self.process.kill()


class LocalShell(Shell):
    async_exec = LocalAsyncExec

    def __init__(self, transport=None):
        super().__init__(transport)

    def put_file(self, f, dst_path):
        with open(dst_path, "wb") as f2:
            shutil.copyfileobj(f, f2)


class LocalReplicationProcess(ReplicationProcess):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.async_exec = None

    def run(self):
        self.async_exec = self.local_shell.exec_async(
            pipe(
                zfs_send(self.source_dataset, self.snapshot, self.recursive, self.incremental_base,
                         self.receive_resume_token),
                zfs_recv(self.target_dataset)
            )
        )

    def wait(self):
        return self.async_exec.wait()

    def stop(self):
        return self.async_exec.stop()


class LocalTransport(Transport):
    @classmethod
    def from_data(cls, data):
        return LocalTransport()

    def __hash__(self):
        return 1

    shell = LocalShell

    replication_process = LocalReplicationProcess
