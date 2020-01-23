# -*- coding=utf-8 -*-
import logging
import subprocess
import time
from unittest.mock import Mock, PropertyMock

from zettarepl.definition.definition import Definition
from zettarepl.observer import ReplicationTaskError, ReplicationTaskSuccess
from zettarepl.replication.task.task import ReplicationTask
from zettarepl.transport.local import LocalShell
from zettarepl.utils.itertools import select_by_class
from zettarepl.zettarepl import Zettarepl

logger = logging.getLogger(__name__)

__all__ = ["create_zettarepl", "mock_name", "run_replication_test", "set_localhost_transport_options", "transports",
           "wait_replication_tasks_to_complete"]


def create_zettarepl(definition):
    local_shell = LocalShell()
    zettarepl = Zettarepl(Mock(), local_shell)
    zettarepl._spawn_retention = Mock()
    observer = Mock()
    zettarepl.set_observer(observer)
    zettarepl.set_tasks(definition.tasks)
    return zettarepl


def mock_name(mock, name):
    type(mock).name = PropertyMock(return_value=name)
    return mock


def run_replication_test(definition, success=True):
    definition = Definition.from_data(definition)
    zettarepl = create_zettarepl(definition)
    zettarepl._spawn_replication_tasks(select_by_class(ReplicationTask, definition.tasks))
    wait_replication_tasks_to_complete(zettarepl)

    if success:
        success = zettarepl.observer.call_args_list[-1][0][0]
        assert isinstance(success, ReplicationTaskSuccess), success
    else:
        error = zettarepl.observer.call_args_list[-1][0][0]
        assert isinstance(error, ReplicationTaskError), error
        return error


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
