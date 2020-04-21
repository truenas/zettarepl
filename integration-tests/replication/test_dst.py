# -*- coding=utf-8 -*-
from datetime import datetime
from itertools import permutations
import subprocess
import textwrap
from unittest.mock import Mock

import pytest
import pytz
import yaml

from zettarepl.definition.definition import Definition
from zettarepl.observer import PeriodicSnapshotTaskError
from zettarepl.snapshot.list import list_snapshots
from zettarepl.snapshot.snapshot import Snapshot
from zettarepl.replication.task.task import ReplicationTask
from zettarepl.transport.local import LocalShell
from zettarepl.utils.itertools import select_by_class
from zettarepl.utils.test import run_periodic_snapshot_test, run_replication_test
from zettarepl.zettarepl import Zettarepl


@pytest.mark.parametrize("naming_schemas", list(permutations([
    "auto-%Y-%m-%d-%H-%M",
    "auto-%Y-%m-%d-%H-%M%z",
])))
def test_dst(naming_schemas):
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs receive -A data/dst", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    subprocess.check_call("zfs create data/src", shell=True)

    definition = yaml.safe_load(textwrap.dedent(f"""\
        timezone: "Europe/Moscow"

        periodic-snapshot-tasks:
          task1:
            dataset: data/src
            recursive: true
            naming-schema: "{naming_schemas[0]}"
            schedule:
              minute: "*"
              hour: "*"
              day-of-month: "*"
              month: "*"
              day-of-week: "*"
          task2:
            dataset: data/src
            recursive: true
            naming-schema: "{naming_schemas[1]}"
            schedule:
              minute: "*"
              hour: "*"
              day-of-month: "*"
              month: "*"
              day-of-week: "*"
    """))

    run_periodic_snapshot_test(
        definition,
        datetime(2010, 10, 30, 22, 0, 0, tzinfo=pytz.UTC).astimezone(pytz.timezone("Europe/Moscow"))
    )

    local_shell = LocalShell()
    assert list_snapshots(local_shell, "data/src", False) == [
        Snapshot("data/src", "auto-2010-10-31-02-00"),
        Snapshot("data/src", "auto-2010-10-31-02-00:0400"),
    ]

    run_periodic_snapshot_test(
        definition,
        datetime(2010, 10, 30, 23, 0, 0, tzinfo=pytz.UTC).astimezone(pytz.timezone("Europe/Moscow")),
        False,
    )

    assert list_snapshots(local_shell, "data/src", False) == [
        Snapshot("data/src", "auto-2010-10-31-02-00"),
        Snapshot("data/src", "auto-2010-10-31-02-00:0300"),
        Snapshot("data/src", "auto-2010-10-31-02-00:0400"),
    ]

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        replication-tasks:
          src:
            direction: push
            transport:
              type: local
            source-dataset: data/src
            target-dataset: data/dst
            recursive: true
            also-include-naming-schema:
            - "auto-%Y-%m-%d-%H-%M"
            - "auto-%Y-%m-%d-%H-%M%z"
            auto: false
            retention-policy: none
            retries: 1
    """))
    run_replication_test(definition)

    assert list_snapshots(local_shell, "data/dst", False) == [
        Snapshot("data/dst", "auto-2010-10-31-02-00"),
        Snapshot("data/dst", "auto-2010-10-31-02-00:0300"),
        Snapshot("data/dst", "auto-2010-10-31-02-00:0400"),
    ]
