# -*- coding=utf-8 -*-
from collections import namedtuple
import enum
import json
import logging
import os
import queue
import threading

from zettarepl.replication.error import ReplicationConfigurationError
from zettarepl.replication.task.direction import ReplicationDirection

from .base_ssh import BaseSshTransport
from .interface import *
from .utils import put_file

logger = logging.getLogger(__name__)

__all__ = ["SshNetcatTransport"]

FirstLineListenEvent = namedtuple("FirstLineListenEvent", ["data"])
CompleteOutputListenEvent = namedtuple("CompleteOutputListenEvent", ["data"])
CompletedListenEvent = namedtuple("CompletedListenEvent", ["returncode"])


class SshNetcatTransportActiveSide(enum.Enum):
    LOCAL = "local"
    REMOTE = "remote"


class SshNetcatReplicationProcess(ReplicationProcess):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.listen_exec = None
        self.listen_exec_event_queue = queue.Queue()
        self.listen_exec_returncode = None
        self.listen_exec_output = None
        self.listen_exec_complete_event = threading.Event()

        self.connect_exec = None

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
        if self.recursive:
            send_args.append("--recursive")
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

        listen = self._listen(listen_shell, listen_args)

        # Connect

        connect_address = self.transport.passive_side_connect_address
        if connect_address is None:
            if self.transport.active_side == SshNetcatTransportActiveSide.LOCAL:
                connect_address = self.remote_shell.exec(["sh", "-c", "echo $SSH_CLIENT"]).split()[0]
                if not connect_address:
                    raise Exception("passive-side-connect-address not specified and $SSH_CLIENT variable is empty")
            elif self.transport.active_side == SshNetcatTransportActiveSide.LOCAL:
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

    def _listen(self, listen_shell, listen_args):
        r, w = os.pipe()
        rh = os.fdopen(r)
        self.listen_exec = listen_shell.exec_async(listen_args, stdout=w)
        threading.Thread(daemon=True, name=f"{threading.current_thread().name}.ssh_netcat.read_listen_exec",
                         target=self._read_listen_exec, args=(rh,)).start()
        threading.Thread(daemon=True, name=f"{threading.current_thread().name}.ssh_netcat.wait_listen_exec",
                         target=self._wait_listen_exec).start()

        try:
            event = self.listen_exec_event_queue.get(timeout=10)
        except queue.Empty:
            self.listen_exec.stop()
            raise TimeoutError("Timeout reading listen data")

        if isinstance(event, FirstLineListenEvent):
            logger.debug("Read from listen side: %r", event.data)
            return json.loads(event.data)

        while True:
            try:
                event = self.listen_exec_event_queue.get(timeout=10)
            except queue.Empty:
                self.listen_exec.stop()
                if self.listen_exec_returncode is None:
                    raise TimeoutError("Timeout reading listen output")
                elif self.listen_exec_output is None:
                    raise TimeoutError("Timeout reading listen returncode")
                else:
                    raise RuntimeError()

            if isinstance(event, CompleteOutputListenEvent):
                self.listen_exec_output = event.data
            elif isinstance(event, CompletedListenEvent):
                self.listen_exec_returncode = event.returncode
            else:
                self.listen_exec.stop()
                raise ValueError(f"Unknown listen event: {event!r}")

            if self.listen_exec_output is not None and self.listen_exec_returncode is not None:
                raise ExecException(self.listen_exec_returncode, self.listen_exec_output)

    def _read_listen_exec(self, rh):
        try:
            try:
                first_line = rh.readline()
                self.listen_exec_event_queue.put(FirstLineListenEvent(first_line))
            except Exception:
                self.listen_exec_event_queue.put(CompleteOutputListenEvent(""))
                raise

            try:
                self.listen_exec_event_queue.put(CompleteOutputListenEvent(first_line + "\n" + rh.read()))
            except Exception:
                self.listen_exec_event_queue.put(CompleteOutputListenEvent(""))
                raise
        except Exception:
            logger.error("Unhandled exception in _read_listen_exec", exc_info=True)
        finally:
            rh.close()

    def _wait_listen_exec(self):
        try:
            self.listen_exec.wait()
        except ExecException as e:
            self.listen_exec_event_queue.put(CompletedListenEvent(e.returncode))
        else:
            self.listen_exec_event_queue.put(CompletedListenEvent(0))

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
