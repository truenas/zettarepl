# -*- coding=utf-8 -*-
from datetime import datetime
from itertools import permutations
import subprocess
import textwrap

import pytest
import pytz
import yaml

from zettarepl.snapshot.list import list_snapshots
from zettarepl.snapshot.snapshot import Snapshot
from zettarepl.transport.local import LocalShell
from zettarepl.utils.test import run_periodic_snapshot_test, run_replication_test


@pytest.mark.parametrize("naming_schemas", list(permutations([
    "auto-%Y-%m-%d-%H-%M",
    "auto-%Y-%m-%d-%H-%M%z",
])))
def test_dst(naming_schemas):
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs receive -A tank/dst", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    subprocess.check_call("zfs create tank/src", shell=True)

    definition = yaml.safe_load(textwrap.dedent(f"""\
        timezone: "Europe/Moscow"

        periodic-snapshot-tasks:
          task1:
            dataset: tank/src
            recursive: true
            naming-schema: "{naming_schemas[0]}"
            schedule:
              minute: "*"
              hour: "*"
              day-of-month: "*"
              month: "*"
              day-of-week: "*"
          task2:
            dataset: tank/src
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
    assert list_snapshots(local_shell, "tank/src", False) == [
        Snapshot("tank/src", "auto-2010-10-31-02-00"),
        Snapshot("tank/src", "auto-2010-10-31-02-00:0400"),
    ]

    run_periodic_snapshot_test(
        definition,
        datetime(2010, 10, 30, 23, 0, 0, tzinfo=pytz.UTC).astimezone(pytz.timezone("Europe/Moscow")),
        False,
    )

    assert list_snapshots(local_shell, "tank/src", False) == [
        Snapshot("tank/src", "auto-2010-10-31-02-00"),
        Snapshot("tank/src", "auto-2010-10-31-02-00:0300"),
        Snapshot("tank/src", "auto-2010-10-31-02-00:0400"),
    ]

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        replication-tasks:
          src:
            direction: push
            transport:
              type: local
            source-dataset: tank/src
            target-dataset: tank/dst
            recursive: true
            also-include-naming-schema:
            - "auto-%Y-%m-%d-%H-%M"
            - "auto-%Y-%m-%d-%H-%M%z"
            auto: false
            retention-policy: none
            retries: 1
    """))
    run_replication_test(definition)

    assert list_snapshots(local_shell, "tank/dst", False) == [
        Snapshot("tank/dst", "auto-2010-10-31-02-00"),
        Snapshot("tank/dst", "auto-2010-10-31-02-00:0300"),
        Snapshot("tank/dst", "auto-2010-10-31-02-00:0400"),
    ]
