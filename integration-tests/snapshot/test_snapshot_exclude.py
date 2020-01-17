# -*- coding=utf-8 -*-
from datetime import datetime
import subprocess
import textwrap
from unittest.mock import Mock

import yaml

from zettarepl.definition.definition import Definition
from zettarepl.snapshot.list import list_snapshots
from zettarepl.snapshot.task.task import PeriodicSnapshotTask
from zettarepl.transport.local import LocalShell
from zettarepl.utils.itertools import select_by_class
from zettarepl.zettarepl import Zettarepl


def test_snapshot_exclude():
    subprocess.call("zfs destroy -r data/src", shell=True)

    subprocess.check_call("zfs create data/src", shell=True)
    for dataset in ["DISK1", "DISK1/Apps", "DISK1/ISO", "waggnas"]:
        subprocess.check_call(f"zfs create data/src/{dataset}", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        periodic-snapshot-tasks:
          internal:
            dataset: data/src
            recursive: true
            exclude:
            - data/src/waggnas
            lifetime: "P7W"
            naming-schema: "auto-%Y%m%d.%H%M%S-2w"
            schedule:
              minute: "0"
              hour: "6"
              day-of-month: "*"
              month: "*"
              day-of-week: "*"
              begin: "06:00"
              end: "18:00"
    """))
    definition = Definition.from_data(definition)

    local_shell = LocalShell()
    zettarepl = Zettarepl(Mock(), local_shell)
    zettarepl._spawn_retention = Mock()
    zettarepl.set_tasks(definition.tasks)
    zettarepl._run_periodic_snapshot_tasks(
        datetime(2020, 1, 17, 6, 0),
        select_by_class(PeriodicSnapshotTask, definition.tasks),
    )

    assert len(list_snapshots(local_shell, "data/src", False)) == 1
    assert len(list_snapshots(local_shell, "data/src/DISK1/Apps", False)) == 1
    assert len(list_snapshots(local_shell, "data/src/DISK1/ISO", False)) == 1
    assert len(list_snapshots(local_shell, "data/src/waggnas", False)) == 0
