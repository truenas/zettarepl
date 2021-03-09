# -*- coding=utf-8 -*-
import errno
import logging
import io
import shlex
import socket
import stat
import threading

import paramiko

from .interface import *

logger = logging.getLogger(__name__)

__all__ = ["SshTransportAsyncExec", "SshTransportShell", "BaseSshTransport"]


class SshTransportAsyncExec(AsyncExec):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.stdin_fd, self.stdout_fd, self.stderr_fd = (None, None, None)

    def run(self):
        client = self.shell.get_client()

        self.logger.debug("Running %r", self.args)
        self.stdin_fd, self.stdout_fd, self.stderr_fd = client.exec_command(
            "sh -c " + shlex.quote(" ".join([shlex.quote(arg) for arg in self.args]) + " 2>&1"), timeout=10)
        self._copy_stdout_from(self.stdout_fd)

    def wait(self):
        stdout = self._read_stdout()

        self.logger.debug("Waiting for exit status")
        exitcode = self.stdout_fd.channel.recv_exit_status()

        if exitcode != 0:
            self.logger.debug("Error %r: %r", exitcode, stdout)
            raise ExecException(exitcode, stdout)

        self.logger.debug("Success: %r", stdout)
        return stdout

    def _read_stdout(self):
        if self.stdout is None:
            self.logger.debug("Reading stdout")

            stdout = b""
            while True:
                try:
                    read = self.stdout_fd.read(8192)
                    if not read:
                        break
                    stdout += read
                except socket.timeout:
                    if self.stdout_fd.channel.exit_status_ready():
                        break
                    else:
                        continue
                except IOError as e:
                    self.logger.debug("Unable to read stdout: %r", e)
                    return None

            return stdout.decode(self.encoding)

    def stop(self):
        self.logger.debug("Stopping")
        self.stdout_fd.close()


class SshTransportShell(Shell):
    async_exec = SshTransportAsyncExec

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._client = None
        self._sftp = None

    def close(self):
        if self._client is not None:
            self._client.close()
            self._client = None
        if self._sftp is not None:
            self._sftp.close()
            self._sftp = None

    def get_client(self):
        if self._client is None:
            self.logger.debug("Connecting...")
            hkes = [paramiko.hostkeys.HostKeyEntry.from_line(line) for line in self.transport.get_host_key_entries()]
            client = paramiko.SSHClient()
            if any(threading.current_thread().name.startswith(prefix)
                   for prefix in ("replication_task__", "retention")):
                client.set_log_channel(f"zettarepl.paramiko.{threading.current_thread().name}")
            for hke in hkes:
                client.get_host_keys().add(hke.hostnames[0], hke.key.get_name(), hke.key)
            client.connect(
                self.transport.hostname,
                self.transport.port,
                self.transport.username,
                pkey=self._parse_private_key(self.transport.private_key),
                timeout=self.transport.connect_timeout,
                allow_agent=False,
                look_for_keys=False,
                banner_timeout=self.transport.connect_timeout,
                auth_timeout=self.transport.connect_timeout,
            )
            self._client = client

        return self._client

    def _parse_private_key(self, private_key):
        for key_class in (paramiko.RSAKey, paramiko.DSSKey, paramiko.ECDSAKey, paramiko.Ed25519Key):
            try:
                return key_class.from_private_key(io.StringIO(private_key))
            except paramiko.SSHException as e:
                saved_exception = e

        raise saved_exception

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

    def is_dir(self, path):
        return stat.S_ISDIR(self.get_sftp().lstat(path).st_mode)

    def put_file(self, f, dst_path):
        sftp = self.get_sftp()
        incomplete_path = dst_path + ".incomplete"
        sftp.putfo(f, incomplete_path)
        sftp.rename(incomplete_path, dst_path)


class BaseSshTransport(Transport):
    shell = SshTransportShell

    def __init__(self, hostname, port, username, private_key, host_key, connect_timeout):
        self.hostname = hostname
        self.port = port
        self.username = username
        self.private_key = private_key
        self.host_key = host_key
        self.connect_timeout = connect_timeout

        self.logger = logger.getChild(f"{self.username}@{self.hostname}")

    def __hash__(self):
        return hash((self.hostname, self.port, self.username, self.private_key, self.host_key))

    def __repr__(self):
        return f"<SSH Transport({self.username}@{self.hostname})>"

    @classmethod
    def from_data(cls, data):
        data.setdefault("port", 22)
        data.setdefault("username", "root")
        data.setdefault("connect-timeout", 10)
        data["private_key"] = data.pop("private-key")
        data["host_key"] = data.pop("host-key")
        data["connect_timeout"] = data.pop("connect-timeout")

        for entry in get_host_key_entries(data["hostname"], data["port"], data["host_key"]):
            hke = paramiko.hostkeys.HostKeyEntry.from_line(entry)
            if hke is None:
                raise ValueError("Invalid SSH host key")

        return data

    def get_host_key_entries(self):
        return get_host_key_entries(self.hostname, self.port, self.host_key)


def get_host_key_entries(hostname, port, host_keys):
    return [
        f"{hostname} {host_key}" if port == 22 else f"[{hostname}]:{port} {host_key}"
        for host_key in host_keys.split("\n")
        if host_key.strip() and not host_key.strip().startswith("#")
    ]
