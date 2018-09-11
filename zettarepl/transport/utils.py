# -*- coding=utf-8 -*-
import hashlib
import logging
import os

from .interface import Shell, ExecException, ReplicationProcess

logger = logging.getLogger(__name__)

__all__ = ["ProcessReplicationProcess", "put_file"]


class ProcessAwareReplicationProcess(ReplicationProcess):
    def __init__(self, *args, **kwargs):
        ReplicationProcess.__init__(*args, **kwargs)


def put_file(name, shell: Shell):
    local_path = os.path.join(os.path.dirname(__file__), "..", name)
    with open(local_path, "rb") as f:
        md5 = hashlib.md5(f.read()).hexdigest()
        f.seek(0)

        remote_path = f"/tmp/zettarepl--{name.replace('/', '--')}--{md5}"
        try:
            shell.exec(["ls", remote_path])
        except ExecException:
            shell.put_file(f, remote_path)

    return remote_path
