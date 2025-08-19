# -*- coding=utf-8 -*-
from datetime import datetime
import subprocess
import textwrap

import yaml

from zettarepl.snapshot.list import list_snapshots
from zettarepl.transport.local import LocalShell
from zettarepl.utils.test import run_periodic_snapshot_test


def test_snapshot_exclude():
    subprocess.call("zfs destroy -r tank/src", shell=True)

    subprocess.check_call("zfs create tank/src", shell=True)
    for dataset in ["DISK1", "DISK1/Apps", "DISK1/ISO", "waggnas", "DISK2", "DISK2/Apps", "DISK2/ISO"]:
        subprocess.check_call(f"zfs create tank/src/{dataset}", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        periodic-snapshot-tasks:
          internal:
            dataset: tank/src
            recursive: true
            exclude:
            - tank/src/waggnas
            - tank/src/*/ISO
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

    run_periodic_snapshot_test(definition, datetime(2020, 1, 17, 6, 0))

    local_shell = LocalShell()
    assert len(list_snapshots(local_shell, "tank/src", False)) == 1
    assert len(list_snapshots(local_shell, "tank/src/DISK1/Apps", False)) == 1
    assert len(list_snapshots(local_shell, "tank/src/DISK1/ISO", False)) == 0
    assert len(list_snapshots(local_shell, "tank/src/DISK2/Apps", False)) == 1
    assert len(list_snapshots(local_shell, "tank/src/DISK2/ISO", False)) == 0
    assert len(list_snapshots(local_shell, "tank/src/waggnas", False)) == 0
