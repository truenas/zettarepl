# -*- coding=utf-8 -*-
import logging
import subprocess
import time
from unittest.mock import PropertyMock

logger = logging.getLogger(__name__)

__all__ = ["mock_name", "set_localhost_transport_options", "transports", "wait_replication_tasks_to_complete"]


def mock_name(mock, name):
    type(mock).name = PropertyMock(return_value=name)
    return mock


def set_localhost_transport_options(transport):
    with open("/root/.ssh/id_rsa") as f:
        transport["private-key"] = f.read()

    transport["host-key"] = (
        subprocess.check_output(["ssh-keyscan", "localhost"], encoding="utf8").split("\n")[-2].split(" ", 1)[1]
    )


def transports():
    result = [
        {"type": "local"},
        {"type": "ssh", "hostname": "localhost"},
        {"type": "ssh+netcat", "active-side": "local", "hostname": "localhost"},
        {"type": "ssh+netcat", "active-side": "remote", "hostname": "localhost"},
    ]

    for transport in result[1:]:
        set_localhost_transport_options(transport)

    return result


def wait_replication_tasks_to_complete(zettarepl, timeout=300):
    for i in range(timeout):
        if not zettarepl.running_tasks and not zettarepl.pending_tasks:
            return

        time.sleep(1)

    raise TimeoutError()
