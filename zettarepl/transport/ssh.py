# -*- coding=utf-8 -*-
import enum
import logging
import os
import subprocess
import tempfile

from zettarepl.utils.shlex import pipe

from .base_ssh import BaseSshTransport
from .interface import Shell
from .local import LocalShell
from .zfscli import *

logger = logging.getLogger(__name__)

__all__ = ["SshTransport", "SshTransportCipher"]


class SshTransportCipher(enum.Enum):
    STANDARD = "standard"
    FAST = "fast"
    DISABLED = "disabled"


class SshClientCapabilities:
    def __init__(self, executable, supports_none_cipher):
        self.executable = executable
        self.supports_none_cipher = supports_none_cipher

    @classmethod
    def discover(cls):
        executable = "ssh"
        supports_none_cipher = False

        patched_executable = "/usr/local/bin/ssh"
        if os.path.exists(patched_executable):
            executable = patched_executable

            result = subprocess.run([patched_executable, "-ononeenabled=yes", "-p0", "root@localhost"],
                                     stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding="utf8")
            supports_none_cipher = "Bad configuration option" not in result.stdout

        return SshClientCapabilities(executable, supports_none_cipher)


class SshTransport(BaseSshTransport):
    _client_capabilities = None

    def __init__(self, client_capabilities, cipher, **kwargs):
        super().__init__(**kwargs)
        self.client_capabilities = client_capabilities
        self.cipher = cipher

    @classmethod
    def from_data(cls, data):
        if cls._client_capabilities is None:
            cls._client_capabilities = SshClientCapabilities.discover()

        data = super().from_data(data)

        data.setdefault("cipher", "standard")
        data["cipher"] = SshTransportCipher(data["cipher"])

        data["client_capabilities"] = cls._client_capabilities
        if data["cipher"] is SshTransportCipher.DISABLED and not data["client_capabilities"].supports_none_cipher:
            raise ValueError("Local SSH client does not support disabling cipher")

        return SshTransport(**data)

    def __hash__(self):
        return hash([hash(super()), self.cipher])

    def push_snapshot(self, *args, **kwargs):
        with tempfile.NamedTemporaryFile() as private_key_file:
            with tempfile.NamedTemporaryFile() as host_key_file:
                os.chmod(private_key_file.name, 0o600)
                private_key_file.write(self.private_key.encode("ascii"))
                private_key_file.flush()

                os.chmod(host_key_file.name, 0o600)
                host_key_file.write(" ".join([self.hostname, self.host_key]).encode("ascii"))
                host_key_file.flush()

                kwargs.update(private_key_file=private_key_file.name, host_key_file=host_key_file.name)

                return self._push_snapshot(*args, **kwargs)

    def _push_snapshot(self, shell: Shell, source_dataset: str, target_dataset: str, snapshot: str, recursive: bool,
                       incremental_base: str, receive_resume_token: str,
                       private_key_file: str, host_key_file: str):
        cmd = [self.client_capabilities.executable]

        cmd.extend({
            SshTransportCipher.STANDARD: [],
            SshTransportCipher.FAST: ["-c", "arcfour256,arcfour128,blowfish-cbc,aes128-ctr,aes192-ctr,aes256-ctr"],
            SshTransportCipher.DISABLED: ["-ononeenabled=yes", "-ononeswitch=yes"],
        }[self.cipher])

        cmd.extend(["-i", private_key_file])

        cmd.extend(["-o", f"UserKnownHostsFile={host_key_file}"])
        cmd.extend(["-o", "StrictHostKeyChecking=yes"])

        cmd.extend(["-o", "BatchMode=yes"])
        cmd.extend(["-o", "ConnectTimeout=10"])

        cmd.extend([f"-p{self.port}"])
        cmd.extend([f"{self.username}@{self.hostname}"])

        cmd.extend(zfs_recv(target_dataset))

        commands = [zfs_send(source_dataset, snapshot, recursive, incremental_base, receive_resume_token)]
        commands.append(cmd)

        local_shell = LocalShell()
        local_shell.exec(pipe(*commands))
