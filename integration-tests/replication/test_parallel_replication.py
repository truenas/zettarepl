# -*- coding=utf-8 -*-
import subprocess
import textwrap
import time
from unittest.mock import Mock

import pytest
import yaml

from zettarepl.definition.definition import Definition
from zettarepl.observer import ReplicationTaskSuccess
from zettarepl.snapshot.list import list_snapshots
from zettarepl.replication.task.task import ReplicationTask
from zettarepl.transport.local import LocalShell
from zettarepl.utils.itertools import select_by_class
from zettarepl.utils.test import create_zettarepl, set_localhost_transport_options, wait_replication_tasks_to_complete


def test_parallel_replication():
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs receive -A tank/dst", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    subprocess.check_call("zfs create tank/src", shell=True)

    subprocess.check_call("zfs create tank/src/a", shell=True)
    subprocess.check_call("dd if=/dev/urandom of=/mnt/tank/src/a/blob bs=1M count=1", shell=True)
    subprocess.check_call("zfs snapshot tank/src/a@2018-10-01_01-00", shell=True)

    subprocess.check_call("zfs create tank/src/b", shell=True)
    subprocess.check_call("dd if=/dev/urandom of=/mnt/tank/src/b/blob bs=1M count=1", shell=True)
    subprocess.check_call("zfs snapshot tank/src/b@2018-10-01_01-00", shell=True)

    subprocess.check_call("zfs create tank/dst", shell=True)
    subprocess.check_call("zfs create tank/dst/a", shell=True)
    subprocess.check_call("zfs create tank/dst/b", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        periodic-snapshot-tasks:
          src-a:
            dataset: tank/src/a
            recursive: true
            lifetime: PT1H
            naming-schema: "%Y-%m-%d_%H-%M"
            schedule:
              minute: "0"
          src-b:
            dataset: tank/src/b
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
              hostname: 127.0.0.1
            source-dataset: tank/src/a
            target-dataset: tank/dst/a
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
              hostname: 127.0.0.1
            source-dataset: tank/src/b
            target-dataset: tank/dst/b
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
    zettarepl._spawn_replication_tasks(Mock(), select_by_class(ReplicationTask, definition.tasks))

    start = time.monotonic()
    wait_replication_tasks_to_complete(zettarepl)
    end = time.monotonic()
    assert 10 <= end - start <= 15

    assert len(zettarepl._spawn_retention.call_args_list) == 3

    assert sum(1 for m in zettarepl.observer.call_args_list if isinstance(m[0][0], ReplicationTaskSuccess)) == 2

    assert len(list_snapshots(local_shell, "tank/dst/a", False)) == 1
    assert len(list_snapshots(local_shell, "tank/dst/b", False)) == 1

    subprocess.call("zfs destroy -r tank/dst", shell=True)
    subprocess.check_call("zfs create tank/dst", shell=True)
    subprocess.check_call("zfs create tank/dst/a", shell=True)
    subprocess.check_call("zfs create tank/dst/b", shell=True)

    zettarepl._replication_tasks_can_run_in_parallel = Mock(return_value=False)
    zettarepl._spawn_replication_tasks(Mock(), select_by_class(ReplicationTask, definition.tasks))

    start = time.monotonic()
    wait_replication_tasks_to_complete(zettarepl)
    end = time.monotonic()
    assert 20 <= end - start <= 25

    assert sum(1 for m in zettarepl.observer.call_args_list if isinstance(m[0][0], ReplicationTaskSuccess)) == 4

    assert len(list_snapshots(local_shell, "tank/dst/a", False)) == 1
    assert len(list_snapshots(local_shell, "tank/dst/b", False)) == 1


@pytest.mark.parametrize("max_parallel_replications", [2, 3])
def test_parallel_replication_3(max_parallel_replications):
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs receive -A tank/dst", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    subprocess.check_call("zfs create tank/src", shell=True)

    subprocess.check_call("zfs create tank/src/a", shell=True)
    subprocess.check_call("dd if=/dev/urandom of=/mnt/tank/src/a/blob bs=1M count=1", shell=True)
    subprocess.check_call("zfs snapshot tank/src/a@2018-10-01_01-00", shell=True)

    subprocess.check_call("zfs create tank/src/b", shell=True)
    subprocess.check_call("dd if=/dev/urandom of=/mnt/tank/src/b/blob bs=1M count=1", shell=True)
    subprocess.check_call("zfs snapshot tank/src/b@2018-10-01_01-00", shell=True)

    subprocess.check_call("zfs create tank/src/c", shell=True)
    subprocess.check_call("dd if=/dev/urandom of=/mnt/tank/src/c/blob bs=1M count=1", shell=True)
    subprocess.check_call("zfs snapshot tank/src/c@2018-10-01_01-00", shell=True)

    subprocess.check_call("zfs create tank/dst", shell=True)
    subprocess.check_call("zfs create tank/dst/a", shell=True)
    subprocess.check_call("zfs create tank/dst/b", shell=True)
    subprocess.check_call("zfs create tank/dst/c", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        periodic-snapshot-tasks:
          src-a:
            dataset: tank/src/a
            recursive: true
            lifetime: PT1H
            naming-schema: "%Y-%m-%d_%H-%M"
            schedule:
              minute: "0"
          src-b:
            dataset: tank/src/b
            recursive: true
            lifetime: PT1H
            naming-schema: "%Y-%m-%d_%H-%M"
            schedule:
              minute: "0"
          src-c:
            dataset: tank/src/c
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
            source-dataset: tank/src/a
            target-dataset: tank/dst/a
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
            source-dataset: tank/src/b
            target-dataset: tank/dst/b
            recursive: true
            periodic-snapshot-tasks:
              - src-b
            auto: true
            retention-policy: none
            speed-limit: 100000
          src-c:
            direction: push
            transport:
              type: ssh
              hostname: localhost
            source-dataset: tank/src/c
            target-dataset: tank/dst/c
            recursive: true
            periodic-snapshot-tasks:
              - src-c
            auto: true
            retention-policy: none
            speed-limit: 100000
    """))
    definition["max-parallel-replication-tasks"] = max_parallel_replications
    set_localhost_transport_options(definition["replication-tasks"]["src-a"]["transport"])
    set_localhost_transport_options(definition["replication-tasks"]["src-b"]["transport"])
    set_localhost_transport_options(definition["replication-tasks"]["src-c"]["transport"])
    definition = Definition.from_data(definition)

    zettarepl = create_zettarepl(definition)
    zettarepl._spawn_replication_tasks(Mock(), select_by_class(ReplicationTask, definition.tasks))

    start = time.monotonic()
    wait_replication_tasks_to_complete(zettarepl)
    end = time.monotonic()

    if max_parallel_replications == 3:
        assert 10 <= end - start <= 15
    else:
        assert 20 <= end - start <= 25
