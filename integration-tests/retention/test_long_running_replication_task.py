# -*- coding=utf-8 -*-
from datetime import datetime
import subprocess
import textwrap
import time
from unittest.mock import Mock, patch

import pytz
import yaml

from zettarepl.definition.definition import Definition
from zettarepl.snapshot.list import list_snapshots
from zettarepl.snapshot.snapshot import Snapshot
from zettarepl.transport.local import LocalShell
from zettarepl.utils.test import (
    set_localhost_transport_options, wait_replication_tasks_to_complete, wait_retention_to_complete,
)
from zettarepl.zettarepl import Zettarepl


def test_long_running_replication_task_does_not_affect_unrelated_local_retention():
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    subprocess.check_call("zfs create data/src", shell=True)
    subprocess.check_call("zfs create data/src/child1", shell=True)
    subprocess.check_call("zfs create data/src/child2", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2023-04-20_00-00", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2023-04-21_00-00", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2023-04-22_00-00", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2023-04-23_00-00", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2023-04-24_00-00", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2023-04-25_00-00", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2023-04-26_00-00", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2023-04-27_00-00", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2023-04-28_00-00", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2023-04-29_00-00", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2023-04-30_00-00", shell=True)

    subprocess.check_call("dd if=/dev/urandom of=/mnt/data/src/child1/blob bs=1M count=1", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        periodic-snapshot-tasks:
          src:
            dataset: data/src
            recursive: true
            lifetime: P7DT1H
            naming-schema: "%Y-%m-%d_%H-%M"
            schedule:
              hour: "0"
              minute: "0"

        replication-tasks:
          src:
            direction: push
            transport:
              type: ssh
              hostname: 127.0.0.1
            source-dataset: data/src/child1
            target-dataset: data/dst
            recursive: true
            periodic-snapshot-tasks:
              - src
            auto: true
            retention-policy: none
            speed-limit: 100000
    """))
    set_localhost_transport_options(definition["replication-tasks"]["src"]["transport"])
    definition = Definition.from_data(definition)

    local_shell = LocalShell()
    zettarepl = Zettarepl(Mock(), local_shell)
    zettarepl.set_tasks(definition.tasks)
    zettarepl.scheduler.schedule.return_value = [
        Mock(datetime=Mock(datetime=datetime(2023, 5, 1, 0, 0),
                           offset_aware_datetime=datetime(2023, 5, 1, 0, 0, tzinfo=pytz.utc),
                           legit_step_back=None),
             tasks=[zettarepl.tasks[0]]),
    ]
    zettarepl.run()
    try:
        time.sleep(5)

        assert list_snapshots(local_shell, "data/src/child1", False) == [
            Snapshot(dataset="data/src/child1", name="2023-04-20_00-00"),
            Snapshot(dataset="data/src/child1", name="2023-04-21_00-00"),
            Snapshot(dataset="data/src/child1", name="2023-04-22_00-00"),
            Snapshot(dataset="data/src/child1", name="2023-04-23_00-00"),
            Snapshot(dataset="data/src/child1", name="2023-04-24_00-00"),
            Snapshot(dataset="data/src/child1", name="2023-04-25_00-00"),
            Snapshot(dataset="data/src/child1", name="2023-04-26_00-00"),
            Snapshot(dataset="data/src/child1", name="2023-04-27_00-00"),
            Snapshot(dataset="data/src/child1", name="2023-04-28_00-00"),
            Snapshot(dataset="data/src/child1", name="2023-04-29_00-00"),
            Snapshot(dataset="data/src/child1", name="2023-04-30_00-00"),
            Snapshot(dataset="data/src/child1", name="2023-05-01_00-00"),
        ]
        assert list_snapshots(local_shell, "data/src/child2", False) == [
            Snapshot(dataset="data/src/child2", name="2023-04-24_00-00"),
            Snapshot(dataset="data/src/child2", name="2023-04-25_00-00"),
            Snapshot(dataset="data/src/child2", name="2023-04-26_00-00"),
            Snapshot(dataset="data/src/child2", name="2023-04-27_00-00"),
            Snapshot(dataset="data/src/child2", name="2023-04-28_00-00"),
            Snapshot(dataset="data/src/child2", name="2023-04-29_00-00"),
            Snapshot(dataset="data/src/child2", name="2023-04-30_00-00"),
            Snapshot(dataset="data/src/child2", name="2023-05-01_00-00"),
        ]
    finally:
        wait_replication_tasks_to_complete(zettarepl)

    wait_retention_to_complete(zettarepl)

    assert list_snapshots(local_shell, "data/src/child1", False) == [
        Snapshot(dataset="data/src/child1", name="2023-04-24_00-00"),
        Snapshot(dataset="data/src/child1", name="2023-04-25_00-00"),
        Snapshot(dataset="data/src/child1", name="2023-04-26_00-00"),
        Snapshot(dataset="data/src/child1", name="2023-04-27_00-00"),
        Snapshot(dataset="data/src/child1", name="2023-04-28_00-00"),
        Snapshot(dataset="data/src/child1", name="2023-04-29_00-00"),
        Snapshot(dataset="data/src/child1", name="2023-04-30_00-00"),
        Snapshot(dataset="data/src/child1", name="2023-05-01_00-00"),
    ]


def test_remote_retention_only_after_long_running_replication_task():
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    subprocess.check_call("zfs create data/src", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2023-04-20_00-00", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2023-04-21_00-00", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2023-04-22_00-00", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2023-04-23_00-00", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2023-04-24_00-00", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2023-04-25_00-00", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2023-04-26_00-00", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2023-04-27_00-00", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2023-04-28_00-00", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2023-04-29_00-00", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2023-04-30_00-00", shell=True)

    subprocess.check_call("zfs send data/src@2023-04-20_00-00 | zfs recv -s -F data/dst", shell=True)
    subprocess.check_call("zfs send -I data/src@2023-04-20_00-00 data/src@2023-04-30_00-00 | zfs recv -s -F data/dst",
                          shell=True)

    subprocess.check_call("dd if=/dev/urandom of=/mnt/data/src/blob bs=1M count=1", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        periodic-snapshot-tasks:
          src:
            dataset: data/src
            recursive: true
            lifetime: P7D
            naming-schema: "%Y-%m-%d_%H-%M"
            schedule:
              hour: "0"
              minute: "0"

        replication-tasks:
          src:
            direction: push
            transport:
              type: ssh
              hostname: 127.0.0.1
            source-dataset: data/src
            target-dataset: data/dst
            recursive: true
            periodic-snapshot-tasks:
              - src
            auto: true
            retention-policy: custom
            lifetime: P5D
            speed-limit: 100000
    """))
    set_localhost_transport_options(definition["replication-tasks"]["src"]["transport"])
    definition = Definition.from_data(definition)

    with patch("zettarepl.replication.run.pre_retention"):
        local_shell = LocalShell()
        zettarepl = Zettarepl(Mock(), local_shell)
        zettarepl.set_tasks(definition.tasks)
        zettarepl.scheduler.schedule.return_value = [
            Mock(datetime=Mock(datetime=datetime(2023, 5, 1, 0, 0),
                               offset_aware_datetime=datetime(2023, 5, 1, 0, 0, tzinfo=pytz.utc),
                               legit_step_back=None),
                 tasks=[zettarepl.tasks[0]]),
        ]
        zettarepl.run()
        try:
            time.sleep(5)

            assert list_snapshots(local_shell, "data/dst", False) == [
                Snapshot(dataset="data/dst", name="2023-04-20_00-00"),
                Snapshot(dataset="data/dst", name="2023-04-21_00-00"),
                Snapshot(dataset="data/dst", name="2023-04-22_00-00"),
                Snapshot(dataset="data/dst", name="2023-04-23_00-00"),
                Snapshot(dataset="data/dst", name="2023-04-24_00-00"),
                Snapshot(dataset="data/dst", name="2023-04-25_00-00"),
                Snapshot(dataset="data/dst", name="2023-04-26_00-00"),
                Snapshot(dataset="data/dst", name="2023-04-27_00-00"),
                Snapshot(dataset="data/dst", name="2023-04-28_00-00"),
                Snapshot(dataset="data/dst", name="2023-04-29_00-00"),
                Snapshot(dataset="data/dst", name="2023-04-30_00-00"),
            ]
        finally:
            wait_replication_tasks_to_complete(zettarepl)

        wait_retention_to_complete(zettarepl)

        assert list_snapshots(local_shell, "data/dst", False) == [
            Snapshot(dataset="data/dst", name="2023-04-26_00-00"),
            Snapshot(dataset="data/dst", name="2023-04-27_00-00"),
            Snapshot(dataset="data/dst", name="2023-04-28_00-00"),
            Snapshot(dataset="data/dst", name="2023-04-29_00-00"),
            Snapshot(dataset="data/dst", name="2023-04-30_00-00"),
            Snapshot(dataset="data/dst", name="2023-05-01_00-00"),
        ]
