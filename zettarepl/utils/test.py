# -*- coding=utf-8 -*-
import logging
import subprocess
import tempfile
import time
from unittest.mock import Mock, PropertyMock

from zettarepl.definition.definition import Definition
from zettarepl.observer import *
from zettarepl.replication.task.task import ReplicationTask
from zettarepl.snapshot.task.task import PeriodicSnapshotTask
from zettarepl.transport.local import LocalShell
from zettarepl.utils.itertools import select_by_class
from zettarepl.zettarepl import Zettarepl

logger = logging.getLogger(__name__)

__all__ = ["create_dataset", "create_zettarepl", "mock_name", "run_replication_test", "set_localhost_transport_options",
           "transports", "wait_replication_tasks_to_complete", "wait_retention_to_complete"]


def create_dataset(name, encrypted=False):
    if encrypted:
        with tempfile.NamedTemporaryFile("w+") as f:
            f.write("0" * 32)
            f.flush()

            subprocess.check_call(
                f"zfs create -o encryption=on -o keyformat=raw -o keylocation=file://{f.name} {name}",
                shell=True,
            )
    else:
        subprocess.check_call(f"zfs create {name}", shell=True)


def create_zettarepl(definition, scheduler=None):
    local_shell = LocalShell()
    zettarepl = Zettarepl(scheduler or Mock(), local_shell, definition.max_parallel_replication_tasks)
    zettarepl._spawn_retention = Mock()
    observer = Mock(return_value=None)
    zettarepl.set_observer(observer)
    zettarepl.set_tasks(definition.tasks)
    return zettarepl


def mock_name(mock, name):
    type(mock).name = PropertyMock(return_value=name)
    return mock


def run_periodic_snapshot_test(definition, now, success=True):
    definition = Definition.from_data(definition)
    zettarepl = create_zettarepl(definition)
    zettarepl._run_periodic_snapshot_tasks(now, select_by_class(PeriodicSnapshotTask, definition.tasks), None)
    wait_replication_tasks_to_complete(zettarepl)

    if success:
        for call in zettarepl.observer.call_args_list:
            call = call[0][0]
            assert not isinstance(call, PeriodicSnapshotTaskError), success


def run_replication_test(definition, *, success=True, now=None):
    if now is None:
        now = Mock()

    definition = Definition.from_data(definition)
    zettarepl = create_zettarepl(definition)
    zettarepl._spawn_replication_tasks(now, select_by_class(ReplicationTask, definition.tasks))
    wait_replication_tasks_to_complete(zettarepl)

    if success:
        success = zettarepl.observer.call_args_list[-1][0][0]
        assert isinstance(success, ReplicationTaskSuccess), success
        return success
    else:
        error = zettarepl.observer.call_args_list[-1][0][0]
        assert isinstance(error, ReplicationTaskError), error
        return error


def set_localhost_transport_options(transport):
    with open("/root/.ssh/id_rsa") as f:
        transport["private-key"] = f.read()

    transport["host-key"] = (
        subprocess.check_output(["ssh-keyscan", "127.0.0.1"], encoding="utf8").split("\n")[-2].split(" ", 1)[1]
    )


def transports(netcat=True, unprivileged=False):
    result = [
        {"type": "local"},
        {"type": "ssh", "hostname": "127.0.0.1"},
    ]
    if netcat:
        result += [
            {"type": "ssh+netcat", "active-side": "local", "hostname": "127.0.0.1"},
            {"type": "ssh+netcat", "active-side": "remote", "hostname": "127.0.0.1"},
        ]

    for transport in result[1:]:
        set_localhost_transport_options(transport)

    if unprivileged:
        result = result[1:]
        for transport in result:
            transport["username"] = "user"

    return result


def wait_replication_tasks_to_complete(zettarepl, timeout=300):
    for i in range(timeout):
        if not zettarepl.running_tasks and not zettarepl.pending_tasks:
            return

        time.sleep(1)

    raise TimeoutError()


def wait_retention_to_complete(zettarepl, timeout=300):
    for i in range(timeout):
        if not zettarepl.retention_running:
            return

        time.sleep(1)

    raise TimeoutError()
