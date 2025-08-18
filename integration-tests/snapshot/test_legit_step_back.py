# -*- coding=utf-8 -*-
from datetime import datetime
import subprocess
import textwrap
from unittest.mock import Mock

import yaml

from zettarepl.definition.definition import Definition
from zettarepl.observer import PeriodicSnapshotTaskStart, PeriodicSnapshotTaskSuccess
from zettarepl.scheduler.scheduler import Scheduler
from zettarepl.scheduler.tz_clock import TzClock
from zettarepl.snapshot.list import list_snapshots
from zettarepl.transport.local import LocalShell
from zettarepl.utils.test import create_zettarepl


def test_snapshot_exclude():
    subprocess.call("zfs destroy -r tank/src", shell=True)

    subprocess.check_call("zfs create tank/src", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "Europe/Moscow"

        periodic-snapshot-tasks:
          src:
            dataset: tank/src
            recursive: true
            naming-schema: "%Y-%m-%d-%H-%M-%S"
            schedule:
              minute: "0"
              hour: "*"
              day-of-month: "*"
              month: "*"
              day-of-week: "*"
    """))

    definition = Definition.from_data(definition)
    clock = Mock()
    clock.tick.side_effect = [
        datetime(2010, 10, 30, 22, 0, 0),
        datetime(2010, 10, 30, 22, 1, 0),
        datetime(2010, 10, 30, 23, 0, 0),
        None,
    ]
    tz_clock = TzClock(definition.timezone, datetime(2010, 10, 30, 21, 59, 59))
    zettarepl = create_zettarepl(definition, Scheduler(clock, tz_clock))
    zettarepl.run()

    assert isinstance(zettarepl.observer.call_args_list[0][0][0], PeriodicSnapshotTaskStart)
    assert isinstance(zettarepl.observer.call_args_list[1][0][0], PeriodicSnapshotTaskSuccess)
    assert isinstance(zettarepl.observer.call_args_list[2][0][0], PeriodicSnapshotTaskStart)
    assert isinstance(zettarepl.observer.call_args_list[3][0][0], PeriodicSnapshotTaskSuccess)

    local_shell = LocalShell()
    assert len(list_snapshots(local_shell, "tank/src", False)) == 1
