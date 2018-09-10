# -*- coding=utf-8 -*-
import logging
import io
import os
import re
import shlex
import threading

import paramiko

from .interface import Transport, Shell, ExecException

logger = logging.getLogger(__name__)

__all__ = ["SshTransport"]


class BaseSshTransport(Transport):
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

    def create_shell(self):
        return SshTransportShell(self.hostname, self.port, self.username, self.private_key, self.host_key)


class SshTransportShell(Shell):
    def __init__(self, hostname, port, username, private_key, host_key):
        self.hostname = hostname
        self.port = port
        self.username = username
        self.private_key = private_key
        self.host_key = host_key

        self.logger = logger.getChild(f"{self.username}@{self.hostname}")

        self._client = None

    def _get_client(self):
        if self._client is None:
            self.logger.debug("Connecting...")
            hke = paramiko.hostkeys.HostKeyEntry.from_line(" ".join([self.hostname, self.host_key]))
            self._client = paramiko.SSHClient()
            self._client.get_host_keys().add(self.hostname, hke.key.get_name(), hke.key)
            self._client.connect(
                self.hostname,
                self.port,
                self.username,
                pkey={
                    "EC": paramiko.ECDSAKey,
                    "RSA": paramiko.RSAKey,
                    "DSA": paramiko.DSSKey,
                }[re.search("BEGIN (EC|RSA|DSA) PRIVATE KEY", self.private_key).group(1)].from_private_key(
                    io.StringIO(self.private_key)),
                timeout=10,
                allow_agent=False,
                look_for_keys=False,
                banner_timeout=10,
                auth_timeout=10,
            )

        return self._client

    def exec(self, args, encoding="utf8", stdout=None):
        stdout_pipe = stdout

        client = self._get_client()

        self.logger.debug("Running %r", args)
        stdin, stdout, stderr = client.exec_command(" ".join([shlex.quote(arg) for arg in args]) + " 2>&1", timeout=10)
        if stdout_pipe is not None:
            threading.Thread(daemon=True, target=self._copy, args=(stdout, stdout_pipe)).start()
        self.logger.debug("Waiting for exit status")
        exitcode = stdout.channel.recv_exit_status()
        stdout = stdout.read().decode(encoding)
        if exitcode != 0:
            logger.debug("Error %r: %r", exitcode, stdout)
            raise ExecException(exitcode, stdout)

        logger.debug("Success: %r", stdout)
        return stdout

    def put_file(self, f, dst_path):
        client = self._get_client()

        sftp = client.open_sftp()
        sftp.putfo(f, dst_path)
        sftp.close()

    def _copy(self, file_like, descriptor):
        with os.fdopen(descriptor, "w") as f:
            for line in file_like.readlines():
                f.write(line)
