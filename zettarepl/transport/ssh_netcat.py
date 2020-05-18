# -*- coding=utf-8 -*-
from collections import namedtuple
import enum
import json
import logging
import threading

from zettarepl.replication.error import ReplicationConfigurationError, RecoverableReplicationError
from zettarepl.replication.task.direction import ReplicationDirection

from .async_exec_tee import AsyncExecTee
from .base_ssh import BaseSshTransport
from .interface import *
from .utils import put_file
from .zfscli.exception import ZfsCliExceptionHandler

logger = logging.getLogger(__name__)

__all__ = ["SshNetcatTransport"]

FirstLineListenEvent = namedtuple("FirstLineListenEvent", ["data"])
CompleteOutputListenEvent = namedtuple("CompleteOutputListenEvent", ["data"])
CompletedListenEvent = namedtuple("CompletedListenEvent", ["returncode"])


class SshNetcatTransportActiveSide(enum.Enum):
    LOCAL = "local"
    REMOTE = "remote"


class SshNetcatExecException(ExecException):
    def __init__(self, connect_exc, listen_exc):
        self.connect_exc = connect_exc
        self.listen_exc = listen_exc

        super().__init__(1, str(self))

    def __str__(self):
        return f"{self.connect_exc}\n{self.listen_exc or 'No error'}"

    def __repr__(self):
        return "SshNetcatExecException(%r, %r)" % (self.connect_exc, self.listen_exc)


class SshNetcatReplicationProcess(ReplicationProcess):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.listen_exec = None
        self.connect_exec = None

        self.listen_exec_error = None
        self.listen_exec_terminated = threading.Event()

    def run(self):
        if self.compression is not None:
            raise ReplicationConfigurationError("compression is not supported for ssh+netcat replication")

        if self.speed_limit is not None:
            raise ReplicationConfigurationError("speed-limit is not supported for ssh+netcat replication")

        local_helper = put_file("transport/ssh_netcat_helper.py", self.local_shell)
        remote_helper = put_file("transport/ssh_netcat_helper.py", self.remote_shell)

        # Listen

        listen_args = ["--listen", self.transport.active_side_listen_address,
                       "--listen-min-port", str(self.transport.active_side_min_port),
                       "--listen-max-port", str(self.transport.active_side_max_port)]

        send_args = ["send", self.source_dataset]
        if self.replicate:
            send_args.append("--replicate")
        else:
            if self.properties:
                send_args.append("--properties")
        if self.dedup:
            send_args.append("--dedup")
        if self.large_block:
            send_args.append("--large-block")
        if self.embed:
            send_args.append("--embed")
        if self.compressed:
            send_args.append("--compressed")
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

        if self.transport.active_side == SshNetcatTransportActiveSide.LOCAL:
            listen_shell = self.local_shell
            listen_args = ["python3", "-u", local_helper] + listen_args

            if self.direction == ReplicationDirection.PUSH:
                listen_args.extend(send_args)
            elif self.direction == ReplicationDirection.PULL:
                listen_args.extend(receive_args)
            else:
                raise ValueError(f"Invalid replication direction: {self.direction!r}")

        elif self.transport.active_side == SshNetcatTransportActiveSide.REMOTE:
            listen_shell = self.remote_shell
            listen_args = ["python3", "-u", remote_helper] + listen_args

            if self.direction == ReplicationDirection.PUSH:
                listen_args.extend(receive_args)
            elif self.direction == ReplicationDirection.PULL:
                listen_args.extend(send_args)
            else:
                raise ValueError(f"Invalid replication direction: {self.direction!r}")

        else:
            raise ValueError(f"Invalid active side: {self.transport.active_side!r}")

        self.listen_exec = AsyncExecTee(listen_shell, listen_args)
        self.listen_exec.run()

        listen = self.listen_exec.head(self._parse_listen_exec, 10)
        threading.Thread(daemon=True, name=f"{threading.current_thread().name}.listen_exec.wait",
                         target=self._wait_listen_exec).start()

        # Connect

        connect_address = self.transport.passive_side_connect_address
        if connect_address is None:
            if self.transport.active_side == SshNetcatTransportActiveSide.LOCAL:
                connect_address = self.remote_shell.exec(["sh", "-c", "echo $SSH_CLIENT"]).split()[0]
                if not connect_address:
                    raise Exception("passive-side-connect-address not specified and $SSH_CLIENT variable is empty")
            elif self.transport.active_side == SshNetcatTransportActiveSide.REMOTE:
                connect_address = self.transport.hostname
            logger.info("Automatically chose connect address %r", connect_address)

        connect_args = ["--connect", connect_address,
                        "--connect-port", str(listen["port"]),
                        "--connect-token", listen["token"]]

        if self.transport.active_side == SshNetcatTransportActiveSide.LOCAL:
            connect_shell = self.remote_shell
            connect_args = ["python3", "-u", remote_helper] + connect_args

            if self.direction == ReplicationDirection.PUSH:
                connect_args.extend(receive_args)
            elif self.direction == ReplicationDirection.PULL:
                connect_args.extend(send_args)
            else:
                raise ValueError(f"Invalid replication direction: {self.direction!r}")

        elif self.transport.active_side == SshNetcatTransportActiveSide.REMOTE:
            connect_shell = self.local_shell
            connect_args = ["python3", "-u", local_helper] + connect_args

            if self.direction == ReplicationDirection.PUSH:
                connect_args.extend(send_args)
            elif self.direction == ReplicationDirection.PULL:
                connect_args.extend(receive_args)
            else:
                raise ValueError(f"Invalid replication direction: {self.direction!r}")

        else:
            raise ValueError(f"Invalid active side: {self.transport.active_side!r}")

        self.connect_exec = connect_shell.exec_async(connect_args)

    def wait(self):
        try:
            with ZfsCliExceptionHandler(self):
                self.connect_exec.wait()
        except ExecException as connect_exec_error:
            if not self.listen_exec_terminated.wait(5):
                self.logger.warning("Listen side has not terminated within 5 seconds after connect side error")

            if isinstance(self.listen_exec_error, RecoverableReplicationError):
                raise self.listen_exec_error from None

            raise SshNetcatExecException(connect_exec_error, self.listen_exec_error) from None
        else:
            if not self.listen_exec_terminated.wait(60):
                self.logger.warning("Listen side has not terminated within 60 seconds after connect side success")

            if isinstance(self.listen_exec_error, RecoverableReplicationError):
                raise self.listen_exec_error from None

            if self.listen_exec_error is not None:
                raise SshNetcatExecException(None, self.listen_exec_error)
        finally:
            self.stop()

    def stop(self):
        self.listen_exec.stop()
        self.connect_exec.stop()

    def _parse_listen_exec(self, data):
        logger.debug("Read from listen side: %r", data)
        return json.loads(data)

    def _wait_listen_exec(self):
        try:
            with ZfsCliExceptionHandler(self):
                self.listen_exec.wait()
        except (ExecException, RecoverableReplicationError) as e:
            self.listen_exec_error = e
        except Exception:
            self.logger.error("listen_exec failed", exc_info=True)
        finally:
            self.listen_exec_terminated.set()


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
        data["active_side_min_port"] = data.pop("active-side-min-port", 1024)
        data["active_side_max_port"] = data.pop("active-side-max-port", 65535)
        data["passive_side_connect_address"] = data.pop("passive-side-connect-address", None)

        return SshNetcatTransport(**data)

    def __hash__(self):
        return hash((super().__hash__(), self.active_side, self.active_side_min_port, self.active_side_max_port))

    replication_process = SshNetcatReplicationProcess
