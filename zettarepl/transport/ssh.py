# -*- coding=utf-8 -*-
import enum
import logging
import os
import re
import subprocess
import tempfile
import threading

from zettarepl.replication.task.direction import ReplicationDirection
from zettarepl.utils.shlex import implode, pipe

from .async_exec_tee import AsyncExecTee
from .base_ssh import BaseSshTransport
from .interface import *
from .zfscli import *
from .zfscli.exception import ZfsCliExceptionHandler

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


class SshReplicationProcess(ReplicationProcess):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.report_progress = False

        self.private_key_file = None
        self.host_key_file = None

        self.async_exec = None
        self.stop_progress_observer = None

    def run(self):
        self.report_progress = self._zfs_send_can_report_progress()

        self.private_key_file = tempfile.NamedTemporaryFile("w")
        os.chmod(self.private_key_file.name, 0o600)
        self.private_key_file.write(self.transport.private_key)
        self.private_key_file.flush()

        self.host_key_file = tempfile.NamedTemporaryFile("w")
        os.chmod(self.host_key_file.name, 0o600)
        self.host_key_file.write(self.transport.get_host_key_entry())
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
                            self.receive_resume_token,
                            self.dedup,
                            self.large_block,
                            self.embed,
                            self.compressed,
                            self.raw,
                            self.report_progress)

            recv = zfs_recv(self.target_dataset)

            send = ["sh", "-c", "(" + implode(send) + " & PID=$!; echo \"zettarepl: zfs send PID is $PID\" 1>&2; "
                                "wait $PID)"]

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

            if self.report_progress:
                self.stop_progress_observer = threading.Event()

                pid = self.async_exec.head(self._get_zettarepl_pid, 10)

                threading.Thread(daemon=True, name=f"{threading.current_thread().name}.ssh.progress_observer",
                                 target=self._progress_observer, args=(pid,)).start()

        except Exception:
            self.private_key_file.close()
            self.host_key_file.close()
            raise

    def wait(self):
        try:
            with ZfsCliExceptionHandler(self):
                self.async_exec.wait()
        finally:
            self.private_key_file.close()
            self.host_key_file.close()
            if self.stop_progress_observer:
                self.stop_progress_observer.set()

    def stop(self):
        return self.async_exec.stop()

    def _zfs_send_can_report_progress(self):
        send_shell = self._get_send_shell()

        try:
            send_shell.exec(["zfs", "send", "-V"])
        except ExecException as e:
            if "missing snapshot argument" in e.stdout:
                # Option is supported (patched zfs on FreeNAS)
                return True
            else:
                # invalid option 'V'
                return False
        else:
            return False

    def _get_zettarepl_pid(self, line):
        m = re.match("zettarepl: zfs send PID is ([0-9]+)", line.strip())
        if m:
            return int(m.group(1))

    def _progress_observer(self, pid):
        send_shell = self._get_send_shell()

        while True:
            if self.stop_progress_observer.wait(10):
                return

            s = send_shell.exec(["/bin/ps", "-axww", "-o", "command", "-p", str(pid)])
            m = re.search(r"zfs: sending (?P<snapshot>.+) \([0-9]+%: (?P<current>[0-9]+)/(?P<total>[0-9]+)\)", s)
            if m:
                self.notify_progress_observer(int(m.group("current")), int(m.group("total")))
            else:
                logger.debug("Unable to find ZFS send progress in %r", s)

    def _get_send_shell(self):
        if self.direction == ReplicationDirection.PUSH:
            return self.local_shell
        elif self.direction == ReplicationDirection.PULL:
            return self.remote_shell
        else:
            raise ValueError(f"Invalid replication direction: {self.direction!r}")


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
