# -*- coding=utf-8 -*-
import subprocess
import textwrap
import time
from unittest.mock import Mock

import yaml

from zettarepl.definition.definition import Definition
from zettarepl.observer import ReplicationTaskSuccess
from zettarepl.snapshot.list import list_snapshots
from zettarepl.replication.task.task import ReplicationTask
from zettarepl.transport.local import LocalShell
from zettarepl.utils.itertools import select_by_class
from zettarepl.utils.test import create_zettarepl, set_localhost_transport_options, wait_replication_tasks_to_complete


def test_parallel_replication():
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs receive -A data/dst", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    subprocess.check_call("zfs create data/src", shell=True)

    subprocess.check_call("zfs create data/src/a", shell=True)
    subprocess.check_call("dd if=/dev/urandom of=/mnt/data/src/a/blob bs=1M count=1", shell=True)
    subprocess.check_call("zfs snapshot data/src/a@2018-10-01_01-00", shell=True)

    subprocess.check_call("zfs create data/src/b", shell=True)
    subprocess.check_call("dd if=/dev/urandom of=/mnt/data/src/b/blob bs=1M count=1", shell=True)
    subprocess.check_call("zfs snapshot data/src/b@2018-10-01_01-00", shell=True)

    subprocess.check_call("zfs create data/dst", shell=True)
    subprocess.check_call("zfs create data/dst/a", shell=True)
    subprocess.check_call("zfs create data/dst/b", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        periodic-snapshot-tasks:
          src-a:
            dataset: data/src/a
            recursive: true
            lifetime: PT1H
            naming-schema: "%Y-%m-%d_%H-%M"
            schedule:
              minute: "0"
          src-b:
            dataset: data/src/b
            recursive: true
            lifetime: PT1H
            naming-schema: "%Y-%m-%d_%H-%M"
            schedule:
              minute: "0"

        replication-tasks:
          src-a:
            direction: push
            transport:
              type: ssh
              hostname: localhost
            source-dataset: data/src/a
            target-dataset: data/dst/a
            recursive: true
            periodic-snapshot-tasks:
              - src-a
            auto: true
            retention-policy: none
            speed-limit: 100000
          src-b:
            direction: push
            transport:
              type: ssh
              hostname: localhost
            source-dataset: data/src/b
            target-dataset: data/dst/b
            recursive: true
            periodic-snapshot-tasks:
              - src-b
            auto: true
            retention-policy: none
            speed-limit: 100000
    """))
    set_localhost_transport_options(definition["replication-tasks"]["src-a"]["transport"])
    set_localhost_transport_options(definition["replication-tasks"]["src-b"]["transport"])
    definition = Definition.from_data(definition)

    local_shell = LocalShell()
    zettarepl = create_zettarepl(definition)
    zettarepl._spawn_replication_tasks(select_by_class(ReplicationTask, definition.tasks))

    start = time.monotonic()
    wait_replication_tasks_to_complete(zettarepl)
    end = time.monotonic()
    assert 10 <= end - start <= 15

    zettarepl._spawn_retention.assert_called_once()

    assert sum(1 for m in zettarepl.observer.call_args_list if isinstance(m[0][0], ReplicationTaskSuccess)) == 2

    assert len(list_snapshots(local_shell, "data/dst/a", False)) == 1
    assert len(list_snapshots(local_shell, "data/dst/b", False)) == 1

    subprocess.call("zfs destroy -r data/dst", shell=True)
    subprocess.check_call("zfs create data/dst", shell=True)
    subprocess.check_call("zfs create data/dst/a", shell=True)
    subprocess.check_call("zfs create data/dst/b", shell=True)

    zettarepl._replication_tasks_can_run_in_parallel = Mock(return_value=False)
    zettarepl._spawn_replication_tasks(select_by_class(ReplicationTask, definition.tasks))

    start = time.monotonic()
    wait_replication_tasks_to_complete(zettarepl)
    end = time.monotonic()
    assert 20 <= end - start <= 25

    assert sum(1 for m in zettarepl.observer.call_args_list if isinstance(m[0][0], ReplicationTaskSuccess)) == 4

    assert len(list_snapshots(local_shell, "data/dst/a", False)) == 1
    assert len(list_snapshots(local_shell, "data/dst/b", False)) == 1
