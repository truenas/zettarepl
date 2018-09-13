# -*- coding=utf-8 -*-
import errno
import logging
import io
import os
import re
import shlex
import threading

import paramiko

from .interface import *

logger = logging.getLogger(__name__)

__all__ = ["SshTransport"]


class SshTransportAsyncExec(AsyncExec):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.stdin_fd, self.stdout_fd, self.stderr_fd = (None, None, None)

    def run(self):
        client = self.shell.get_client()

        logger.debug("Running %r", self.args)
        self.stdin_fd, self.stdout_fd, self.stderr_fd = client.exec_command(
            " ".join([shlex.quote(arg) for arg in self.args]) + " 2>&1", timeout=10)
        if self.stdout is not None:
            threading.Thread(daemon=True, name=f"{threading.current_thread().name}.ssh.stdout_copy",
                             target=self._copy, args=(self.stdout_fd, self.stdout)).start()

    def wait(self):
        logger.debug("Waiting for exit status")
        exitcode = self.stdout_fd.channel.recv_exit_status()
        stdout = self.stdout_fd.read().decode(self.encoding)
        if exitcode != 0:
            logger.debug("Error %r: %r", exitcode, stdout)
            raise ExecException(exitcode, stdout)

        logger.debug("Success: %r", stdout)
        return stdout

    def stop(self):
        self.stdout_fd.close()

    def _copy(self, file_like, descriptor):
        try:
            with os.fdopen(descriptor, "w") as f:
                for line in file_like.readlines():
                    f.write(line)
        except Exception as e:
            logger.warning("Copying between from SSH %r to file descriptor %r failed: %r", file_like, descriptor, e)


class SshTransportShell(Shell):
    async_exec = SshTransportAsyncExec

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._client = None
        self._sftp = None

    def get_client(self):
        if self._client is None:
            logger.debug("Connecting...")
            hke = paramiko.hostkeys.HostKeyEntry.from_line(" ".join([self.transport.hostname, self.transport.host_key]))
            self._client = paramiko.SSHClient()
            self._client.get_host_keys().add(self.transport.hostname, hke.key.get_name(), hke.key)
            self._client.connect(
                self.transport.hostname,
                self.transport.port,
                self.transport.username,
                pkey={
                    "EC": paramiko.ECDSAKey,
                    "RSA": paramiko.RSAKey,
                    "DSA": paramiko.DSSKey,
                }[re.search("BEGIN (EC|RSA|DSA) PRIVATE KEY", self.transport.private_key).group(1)].from_private_key(
                    io.StringIO(self.transport.private_key)),
                timeout=10,
                allow_agent=False,
                look_for_keys=False,
                banner_timeout=10,
                auth_timeout=10,
            )

        return self._client

    def get_sftp(self):
        if self._sftp is None:
            client = self.get_client()

            self._sftp = client.open_sftp()

        return self._sftp

    def exists(self, path):
        try:
            self.get_sftp().stat(path)
            return True
        except IOError as e:
            if e.errno == errno.ENOENT:
                return False

            raise

    def ls(self, path):
        return self.get_sftp().listdir(path)

    def put_file(self, f, dst_path):
        sftp = self.get_sftp()
        incomplete_path = dst_path + ".incomplete"
        sftp.putfo(f, incomplete_path)
        sftp.rename(incomplete_path, dst_path)


class BaseSshTransport(Transport):
    shell = SshTransportShell

    def __init__(self, hostname, port, username, private_key, host_key):
        self.hostname = hostname
        self.port = port
        self.username = username
        self.private_key = private_key
        self.host_key = host_key

    def __hash__(self):
        return hash([self.hostname, self.port, self.username, self.private_key, self.host_key])

    @classmethod
    def from_data(cls, data):
        data.setdefault("port", 22)
        data.setdefault("username", "root")
        data["private_key"] = data.pop("private-key")
        data["host_key"] = data.pop("host-key")
        return data
