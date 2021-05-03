# -*- coding=utf-8 -*-
import enum
import logging
import os
import subprocess
import tempfile

from zettarepl.replication.task.direction import ReplicationDirection
from zettarepl.utils.shlex import implode, pipe

from .async_exec_tee import AsyncExecTee
from .base_ssh import BaseSshTransport
from .encryption_context import EncryptionContext
from .interface import *
from .progress_report_mixin import ProgressReportMixin
from .utils import get_properties_override
from .zfscli import *
from .zfscli.exception import ZfsSendRecvExceptionHandler
from .zfscli.warning import warnings_from_zfs_success

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


class SshReplicationProcess(ReplicationProcess, ProgressReportMixin):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.private_key_file = None
        self.host_key_file = None

        self.async_exec = None
        self.encryption_context = None

    def run(self):
        report_progress = self._zfs_send_can_report_progress()

        self.private_key_file = tempfile.NamedTemporaryFile("w")
        os.chmod(self.private_key_file.name, 0o600)
        self.private_key_file.write(self.transport.private_key)
        self.private_key_file.flush()

        self.host_key_file = tempfile.NamedTemporaryFile("w")
        os.chmod(self.host_key_file.name, 0o600)
        self.host_key_file.write("\n".join(self.transport.get_host_key_entries()))
        self.host_key_file.flush()

        try:
            cmd = [self.transport.client_capabilities.executable]

            cmd.extend({
               SshTransportCipher.STANDARD: [],
               SshTransportCipher.FAST: ["-c", "aes128-ctr,aes192-ctr,aes256-ctr"],
               SshTransportCipher.DISABLED: ["-ononeenabled=yes", "-ononeswitch=yes"],
            }[self.transport.cipher])

            cmd.extend(["-i", self.private_key_file.name])

            cmd.extend(["-o", f"UserKnownHostsFile={self.host_key_file.name}"])
            cmd.extend(["-o", "StrictHostKeyChecking=yes"])

            cmd.extend(["-o", "BatchMode=yes"])
            cmd.extend(["-o", f"ConnectTimeout={self.transport.connect_timeout}"])

            cmd.extend([f"-p{self.transport.port}"])
            cmd.extend([f"{self.transport.username}@{self.transport.hostname}"])

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
                self.encryption_context = EncryptionContext(self, self._get_recv_shell())

            properties_exclude = self.properties_exclude
            properties_override = get_properties_override(self, self.encryption_context)
            if not self._zfs_recv_can_exclude_properties():
                properties_exclude = []
                properties_override = {}

            recv = zfs_recv(self.target_dataset, properties_exclude, properties_override)

            send = self._wrap_send(send)

            if self.compression is not None:
                send = pipe(send, self.compression.compress)
                recv = pipe(self.compression.decompress, recv)

            if self.speed_limit is not None:
                send = pipe(send, ["throttle", "-B", str(self.speed_limit)])

            if self.direction == ReplicationDirection.PUSH:
                commands = [send, cmd + [implode(recv)]]
            elif self.direction == ReplicationDirection.PULL:
                commands = [cmd + [implode(send)], recv]
            else:
                raise ValueError(f"Invalid replication direction: {self.direction!r}")

            self.async_exec = AsyncExecTee(self.local_shell, pipe(*commands))
            self.async_exec.run()

            if report_progress:
                self._start_progress_observer()

        except Exception:
            self.private_key_file.close()
            self.host_key_file.close()
            raise

    def wait(self):
        success = False
        try:
            with ZfsSendRecvExceptionHandler(self):
                stdout = self.async_exec.wait()
                logger.debug("Success: %r", stdout)

                for warning in warnings_from_zfs_success(stdout):
                    self.notify_warning_observer(warning)

                success = True
        finally:
            self.private_key_file.close()
            self.host_key_file.close()

            if self.encryption_context is not None:
                self.encryption_context.exit(success)

            self._stop_progress_observer()

    def stop(self):
        return self.async_exec.stop()

    def _get_send_shell(self):
        if self.direction == ReplicationDirection.PUSH:
            return self.local_shell
        elif self.direction == ReplicationDirection.PULL:
            return self.remote_shell
        else:
            raise ValueError(f"Invalid replication direction: {self.direction!r}")

    def _get_recv_shell(self):
        if self.direction == ReplicationDirection.PUSH:
            return self.remote_shell
        elif self.direction == ReplicationDirection.PULL:
            return self.local_shell
        else:
            raise ValueError(f"Invalid replication direction: {self.direction!r}")

    def _zfs_recv_can_exclude_properties(self):
        if self.direction == ReplicationDirection.PUSH:
            try:
                self.remote_shell.exec(["zfs", "recv", "-x"])
            except ExecException as e:
                if "invalid option" in e.stdout:
                    return False

        return True


class SshTransport(BaseSshTransport):
    system_client_capabilities = None

    def __init__(self, client_capabilities, cipher, **kwargs):
        super().__init__(**kwargs)
        self.client_capabilities = client_capabilities
        self.cipher = cipher

    @classmethod
    def from_data(cls, data):
        if cls.system_client_capabilities is None:
            cls.system_client_capabilities = SshClientCapabilities.discover()

        data = super().from_data(data)

        data.setdefault("cipher", "standard")
        data["cipher"] = SshTransportCipher(data["cipher"])

        data["client_capabilities"] = cls.system_client_capabilities
        if data["cipher"] is SshTransportCipher.DISABLED and not data["client_capabilities"].supports_none_cipher:
            raise ValueError("Local SSH client does not support disabling cipher")

        return SshTransport(**data)

    def __hash__(self):
        return hash((super().__hash__(), self.cipher))

    replication_process = SshReplicationProcess
