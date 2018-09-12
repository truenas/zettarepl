# -*- coding=utf-8 -*-
import enum
import fcntl
import json
import logging
import os
import select
import threading

from zettarepl.replication.task.direction import ReplicationDirection

from .base_ssh import BaseSshTransport
from .interface import *
from .utils import put_file

logger = logging.getLogger(__name__)

__all__ = ["SshNetcatTransport"]


class SshNetcatTransportActiveSide(enum.Enum):
    LOCAL = "local"
    REMOTE = "remote"


class SshNetcatReplicationProcess(ReplicationProcess):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.listen_exec = None
        self.listen_result = None
        self.listen_exception = None

        self.connect_exec = None

    def run(self):
        local_helper = put_file("transport/ssh_netcat_helper.py", self.local_shell)
        remote_helper = put_file("transport/ssh_netcat_helper.py", self.remote_shell)

        # Listen

        listen_args = ["--listen", self.remote_shell.transport.active_side_listen_address,
                       "--listen-min-port", str(self.remote_shell.transport.active_side_min_port),
                       "--listen-max-port", str(self.remote_shell.transport.active_side_max_port)]

        send_args = ["send", self.source_dataset]
        if self.recursive:
            send_args.append("--recursive")
        if self.receive_resume_token is None:
            assert self.snapshot is not None

            send_args.extend(["--snapshot", self.snapshot])

            if self.incremental_base:
                send_args.extend(["--incremental-base", self.incremental_base])
        else:
            assert self.snapshot is None
            assert self.incremental_base is None

            send_args.extend(["--receive-resume-token", self.receive_resume_token])

        receive_args = ["receive", self.target_dataset]

        if self.remote_shell.transport.active_side == SshNetcatTransportActiveSide.LOCAL:
            listen_shell = self.local_shell
            listen_args = ["python3", "-u", local_helper] + listen_args

            if self.direction == ReplicationDirection.PUSH:
                listen_args.extend(send_args)
            elif self.direction == ReplicationDirection.PULL:
                listen_args.extend(receive_args)
            else:
                raise ValueError(f"Invalid replication direction: {self.direction!r}")

        elif self.remote_shell.transport.active_side == SshNetcatTransportActiveSide.REMOTE:
            listen_shell = self.remote_shell
            listen_args = ["python3", "-u", remote_helper] + listen_args

            if self.direction == ReplicationDirection.PUSH:
                listen_args.extend(receive_args)
            elif self.direction == ReplicationDirection.PULL:
                listen_args.extend(send_args)
            else:
                raise ValueError(f"Invalid replication direction: {self.direction!r}")

        else:
            raise ValueError(f"Invalid active side: {self.remote_shell.transport.active_side!r}")

        listen = self._listen(listen_shell, listen_args)

        # Connect

        connect_address = self.remote_shell.transport.passive_side_connect_address
        if connect_address is None:
            if self.remote_shell.transport.active_side == SshNetcatTransportActiveSide.LOCAL:
                connect_address = self.remote_shell.exec(["sh", "-c", "echo $SSH_CLIENT"]).split()[0]
                if not connect_address:
                    raise Exception("passive-side-connect-address not specified and $SSH_CLIENT variable is empty")
            elif self.remote_shell.transport.active_side == SshNetcatTransportActiveSide.LOCAL:
                connect_address = self.remote_shell.transport.hostname
            logger.info("Automatically chose connect address %r", connect_address)

        connect_args = ["--connect", connect_address,
                        "--connect-port", str(listen["port"]),
                        "--connect-token", listen["token"]]

        if self.remote_shell.transport.active_side == SshNetcatTransportActiveSide.LOCAL:
            connect_shell = self.remote_shell
            connect_args = ["python3", "-u", remote_helper] + connect_args

            if self.direction == ReplicationDirection.PUSH:
                connect_args.extend(receive_args)
            elif self.direction == ReplicationDirection.PULL:
                connect_args.extend(send_args)
            else:
                raise ValueError(f"Invalid replication direction: {self.direction!r}")

        elif self.remote_shell.transport.active_side == SshNetcatTransportActiveSide.REMOTE:
            connect_shell = self.local_shell
            connect_args = ["python3", "-u", local_helper] + connect_args

            if self.direction == ReplicationDirection.PUSH:
                connect_args.extend(send_args)
            elif self.direction == ReplicationDirection.PULL:
                connect_args.extend(receive_args)
            else:
                raise ValueError(f"Invalid replication direction: {self.direction!r}")

        else:
            raise ValueError(f"Invalid active side: {self.remote_shell.transport.active_side!r}")

        self.connect_exec = connect_shell.exec_async(connect_args)

    def _listen(self, listen_shell, listen_args):
        r, w = os.pipe()
        rh = os.fdopen(r)
        fcntl.fcntl(r, fcntl.F_SETFL, fcntl.fcntl(r, fcntl.F_GETFL) | os.O_NONBLOCK)
        self.listen_exec = listen_shell.exec_async(listen_args, stdout=w)
        self.listen_exec.run()
        threading.Thread(daemon=True, target=self._wait_listen_exec, args=(rh,)).start()
        for i in range(10):
            rs, ws, xs = select.select([r], [], [], 1)
            if rs:
                data = rh.readline()
                logger.debug("Read from listen side: %r", data)
                return json.loads(data)
            if self.listen_exception:
                raise self.listen_exception
            if self.listen_result:
                break
        else:
            self.listen_exec.stop()
            raise Exception("Timeout reading listen data")

    def _wait_listen_exec(self, rh):
        try:
            self.listen_exec.wait()
        except ExecException as e:
            self.listen_exception = e
        else:
            self.listen_result = rh.read()
        finally:
            rh.close()

    def wait(self):
        try:
            return self.connect_exec.wait()
        finally:
            self.stop()

    def stop(self):
        self.listen_exec.stop()
        self.connect_exec.stop()


class SshNetcatTransport(BaseSshTransport):
    def __init__(self, active_side, active_side_listen_address, active_side_min_port, active_side_max_port,
                 passive_side_connect_address, **kwargs):
        super().__init__(**kwargs)
        self.active_side = active_side
        self.active_side_listen_address = active_side_listen_address
        self.active_side_min_port = active_side_min_port
        self.active_side_max_port = active_side_max_port
        self.passive_side_connect_address = passive_side_connect_address

    @classmethod
    def from_data(cls, data):
        data = super().from_data(data)

        data["active_side"] = SshNetcatTransportActiveSide(data.pop("active-side"))
        data["active_side_listen_address"] = data.pop("active-side-listen-address", "0.0.0.0")
        data["active_side_min_port"] = data.pop("active-side-max-port", 1024)
        data["active_side_max_port"] = data.pop("active-side-max-port", 65535)
        data["passive_side_connect_address"] = data.pop("passive-side-connect-address", None)

        return SshNetcatTransport(**data)

    def __hash__(self):
        return hash([hash(super()), self.active_side, self.active_side_min_port, self.active_side_max_port])

    replication_process = SshNetcatReplicationProcess
