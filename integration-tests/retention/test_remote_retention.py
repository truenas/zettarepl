# -*- coding=utf-8 -*-
from datetime import datetime
import pytest
import subprocess
import textwrap
from unittest.mock import Mock

import yaml

from zettarepl.definition.definition import Definition
from zettarepl.snapshot.list import list_snapshots
from zettarepl.snapshot.snapshot import Snapshot
from zettarepl.transport.local import LocalShell
from zettarepl.zettarepl import Zettarepl


@pytest.mark.parametrize("retention_policy,remains", [
    ({"retention-policy": "source"}, [
        Snapshot("data/dst", "2018-10-01_01-00"),
        Snapshot("data/dst", "2018-10-01_02-00"),
        Snapshot("data/dst", "2018-10-01_03-00")
    ]),
    ({"retention-policy": "custom", "lifetime": "PT1H"}, [
        Snapshot("data/dst", "2018-10-01_02-00"),
        Snapshot("data/dst", "2018-10-01_03-00")
    ]),
    ({"retention-policy": "none"}, [
        Snapshot("data/dst", "2018-10-01_00-00"),
        Snapshot("data/dst", "2018-10-01_01-00"),
        Snapshot("data/dst", "2018-10-01_02-00"),
        Snapshot("data/dst", "2018-10-01_03-00")
    ]),
])
def test_push_remote_retention(retention_policy, remains):
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    subprocess.check_call("zfs create data/src", shell=True)
    subprocess.check_call("zfs snapshot data/src@2018-10-01_01-00", shell=True)
    subprocess.check_call("zfs snapshot data/src@2018-10-01_02-00", shell=True)
    subprocess.check_call("zfs snapshot data/src@2018-10-01_03-00", shell=True)

    subprocess.check_call("zfs create data/dst", shell=True)
    subprocess.check_call("zfs snapshot data/dst@2018-10-01_00-00", shell=True)
    subprocess.check_call("zfs snapshot data/dst@2018-10-01_01-00", shell=True)
    subprocess.check_call("zfs snapshot data/dst@2018-10-01_02-00", shell=True)
    subprocess.check_call("zfs snapshot data/dst@2018-10-01_03-00", shell=True)

    data = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        periodic-snapshot-tasks:
          src:
            dataset: data/src
            recursive: true
            lifetime: PT1H
            naming-schema: "%Y-%m-%d_%H-%M"
            schedule:
              minute: "0"

        replication-tasks:
          src:
            direction: push
            transport:
              type: local
            source-dataset: data/src
            target-dataset: data/dst
            recursive: true
            periodic-snapshot-tasks:
              - src
            auto: true
    """))
    data["replication-tasks"]["src"].update(**retention_policy)
    definition = Definition.from_data(data)

    local_shell = LocalShell()
    zettarepl = Zettarepl(Mock(), local_shell)
    zettarepl.set_tasks(definition.tasks)
    zettarepl._run_remote_retention(datetime(2018, 10, 1, 3, 0), [])

    assert list_snapshots(local_shell, "data/dst", False) == remains


def test_source_retention_multiple_sources():
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    subprocess.check_call("zfs create data/src", shell=True)
    subprocess.check_call("zfs create data/src/a", shell=True)
    subprocess.check_call("zfs create data/src/b", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2018-10-01_02-00", shell=True)

    subprocess.check_call("zfs create data/dst", shell=True)
    subprocess.check_call("zfs create data/dst/a", shell=True)
    subprocess.check_call("zfs create data/dst/b", shell=True)
    subprocess.check_call("zfs snapshot -r data/dst@2018-10-01_00-00", shell=True)
    subprocess.check_call("zfs snapshot -r data/dst@2018-10-01_01-00", shell=True)
    subprocess.check_call("zfs snapshot -r data/dst@2018-10-01_02-00", shell=True)

    definition = Definition.from_data(yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        periodic-snapshot-tasks:
          src:
            dataset: data/src
            recursive: true
            lifetime: PT1H
            naming-schema: "%Y-%m-%d_%H-%M"
            schedule:
              minute: "0"

        replication-tasks:
          src:
            direction: push
            transport:
              type: local
            source-dataset: [data/src/a, data/src/b]
            target-dataset: data/dst
            recursive: false
            periodic-snapshot-tasks:
              - src
            auto: true
            retention-policy: source
            hold-pending-snapshots: true
    """)))

    local_shell = LocalShell()
    zettarepl = Zettarepl(Mock(), local_shell)
    zettarepl.set_tasks(definition.tasks)
    zettarepl._run_remote_retention(datetime(2018, 10, 1, 3, 0), [])

    assert list_snapshots(local_shell, "data/dst/a", False) == [Snapshot("data/dst/a", "2018-10-01_02-00")]
    assert list_snapshots(local_shell, "data/dst/b", False) == [Snapshot("data/dst/b", "2018-10-01_02-00")]
