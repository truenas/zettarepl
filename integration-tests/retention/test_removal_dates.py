# -*- coding=utf-8 -*-
from datetime import datetime
import subprocess
import textwrap
from unittest.mock import Mock, patch

import pytest
import yaml

from zettarepl.definition.definition import Definition
from zettarepl.snapshot.list import list_snapshots
from zettarepl.snapshot.snapshot import Snapshot
from zettarepl.transport.local import LocalShell
from zettarepl.zettarepl import Zettarepl


@pytest.mark.parametrize("snapshots__removal_dates__result", [
    # Does not remove snapshot that is scheduled to be removed later
    (
        [Snapshot("data/src", "2021-02-19-00-00"), Snapshot("data/src", "2021-04-19-00-00")],
        {"data/src@2021-02-19-00-00": datetime(2021, 5, 1, 0, 0)},
        [0, 1],
    ),
    # Does not remove snapshot that was scheduled to be removed later but is kept by someone else
    (
        [Snapshot("data/src", "2021-04-12-00-00"), Snapshot("data/src", "2021-04-19-00-00")],
        {"data/src@2021-02-19-00-00": datetime(2021, 4, 15, 0, 0)},
        [0, 1],
    ),
    # Removes snapshot
    (
        [Snapshot("data/src", "2021-02-19-00-00"), Snapshot("data/src", "2021-04-19-00-00")],
        {"data/src@2021-02-19-00-00": datetime(2021, 4, 1, 0, 0)},
        [1],
    ),
    # Works even for unknown schemas
    (
        [Snapshot("data/src", "2021-02-19--00-00"), Snapshot("data/src", "2021-04-19-00-00")],
        {"data/src@2021-02-19--00-00": datetime(2021, 4, 1, 0, 0)},
        [1],
    ),
    # Does not touch irrelevant snapshots
    (
        [Snapshot("data/src", "2021-04-01-00-00"), Snapshot("data/src/child", "2021-04-01-00-00"),
         Snapshot("data/src", "2021-04-19-00-00")],
        {"data/src/child@2021-04-01-00-00": datetime(2021, 4, 15, 0, 0)},
        [0, 2],
    ),
    # Works even for unknown dataset
    (
        [Snapshot("data/src2", "2021-02-19-00-00")],
        {"data/src2@2021-02-19-00-00": datetime(2021, 4, 1, 0, 0)},
        [],
    ),
    # Keeps even for unknown dataset
    (
        [Snapshot("data/src2", "2021-02-19-00-00")],
        {"data/src2@2021-02-19-00-00": datetime(2021, 5, 1, 0, 0)},
        [0],
    ),
    # Multiple unknown snapshots
    (
        [Snapshot("data/src2", "2021-02-19-00-00"), Snapshot("data/src2", "snap")],
        {"data/src2@2021-02-19-00-00": datetime(2021, 4, 1, 0, 0)},
        [1],
    ),
])
def test_does_not_remove_the_last_snapshot_left(snapshots__removal_dates__result):
    snapshots, removal_dates, result = snapshots__removal_dates__result

    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs destroy -r data/src2", shell=True)

    subprocess.check_call("zfs create data/src", shell=True)
    subprocess.check_call("zfs create data/src/child", shell=True)
    subprocess.check_call("zfs create data/src2", shell=True)
    for snapshot in snapshots:
        subprocess.check_call(f"zfs snapshot {snapshot}", shell=True)

    data = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        periodic-snapshot-tasks:
          src:
            dataset: data/src
            recursive: false
            naming-schema: "%Y-%m-%d-%H-%M"
            schedule:
              minute: "*"
              hour: "*"
              day-of-month: "*"
              month: "*"
              day-of-week: "*"
            lifetime: P30D
    """))
    definition = Definition.from_data(data)

    local_shell = LocalShell()
    zettarepl = Zettarepl(Mock(), local_shell, use_removal_dates=True)
    zettarepl.set_tasks(definition.tasks)
    with patch("zettarepl.zettarepl.get_removal_dates", Mock(return_value=removal_dates)):
        zettarepl._run_local_retention(datetime(2021, 4, 19, 17, 0), [])

    assert list_snapshots(local_shell, "data/src", False) + list_snapshots(local_shell, "data/src2", False) == [
        snapshots[i] for i in result
    ]
