# -*- coding=utf-8 -*-
import enum
import json
import logging
import os
import queue
import subprocess
import tempfile
import threading

from zettarepl.utils.shlex import pipe

from .base_ssh import BaseSshTransport
from .interface import Shell, ExecException
from .local import LocalShell
from .utils import put_file
from .zfscli import *

logger = logging.getLogger(__name__)

__all__ = ["SshNetcatTransport"]


class SshNetcatTransportActiveSide(enum.Enum):
    LOCAL = "local"
    REMOTE = "remote"


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

    def push_snapshot(self, shell: Shell, source_dataset: str, target_dataset: str, snapshot: str, recursive: bool,
                      incremental_base: str, receive_resume_token: str):
        src_shell = LocalShell()
        dst_shell = shell

        src_helper = put_file("transport/ssh_netcat_helper.py", src_shell)
        dst_helper = put_file("transport/ssh_netcat_helper.py", dst_shell)

        r, w = os.pipe()
        q = queue.Queue()
        args = ["python3", "-u", src_helper, "--listen", "0.0.0.0", "send", source_dataset, snapshot]
        if recursive:
            args.append("--recursive")
        if incremental_base:
            args.extend(["--incremental-base", incremental_base])
        threading.Thread(target=self._run_helper, args=(src_shell, args, w, q), daemon=True).start()
        with os.fdopen(r) as f:
            data = f.readline()
            logger.debug("Read from helper: %r", data)
            data = json.loads(data)

            try:
                dst_shell.exec(["python3", "-u", dst_helper,
                                "--connect", "192.168.0.180", "--connect-port", str(data["port"]),
                                "--connect-token", data["token"],
                                "receive", target_dataset])
            finally:
                output = f.read()
                exitcode = q.get()
                if exitcode != 0:
                    raise ExecException(exitcode, output)

    def _run_helper(self, shell: Shell, args, pipe, queue: queue.Queue):
        try:
            shell.exec(args, stdout=pipe)
        except ExecException as e:
            queue.put(e.args[0])
        else:
            queue.put(0)
        finally:
            os.close(pipe)
