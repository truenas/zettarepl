# -*- coding=utf-8 -*-
from datetime import datetime
import subprocess
import textwrap

import pytest
import yaml

from zettarepl.snapshot.list import list_snapshots
from zettarepl.transport.local import LocalShell
from zettarepl.utils.test import run_periodic_snapshot_test


@pytest.mark.parametrize("allow_empty", [True, False])
@pytest.mark.parametrize("is_empty", [True, False])
def test_allow_empty(allow_empty, is_empty):
    subprocess.call("zfs destroy -r data/src", shell=True)

    subprocess.check_call("zfs create data/src", shell=True)
    subprocess.check_call("dd if=/dev/urandom of=/mnt/data/src/file_1 bs=1M count=1", shell=True)
    subprocess.check_call("zfs snapshot data/src@snap-1", shell=True)

    if not is_empty:
        subprocess.check_call("dd if=/dev/urandom of=/mnt/data/src/file_2 bs=1M count=1", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        periodic-snapshot-tasks:
          internal:
            dataset: data/src
            recursive: false
            naming-schema: "%Y-%m-%d_%H-%M"
            schedule:
              minute: "*"
              hour: "*"
              day-of-month: "*"
              month: "*"
              day-of-week: "*"
    """))
    definition["periodic-snapshot-tasks"]["internal"]["allow-empty"] = allow_empty

    run_periodic_snapshot_test(definition, datetime(2020, 3, 11, 19, 36))

    local_shell = LocalShell()
    assert len(list_snapshots(local_shell, "data/src", False)) == (
        1 if is_empty and not allow_empty else 2
    )


def test_subsequent_snapshots():
    subprocess.call("zfs destroy -r data/src", shell=True)

    subprocess.check_call("zfs create data/src", shell=True)
    subprocess.check_call("dd if=/dev/urandom of=/mnt/data/src/file_1 bs=1M count=1", shell=True)
    subprocess.check_call("zfs snapshot data/src@snap-1", shell=True)

    subprocess.check_call("dd if=/dev/urandom of=/mnt/data/src/file_2 bs=1M count=1", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        periodic-snapshot-tasks:
          one-week:
            dataset: data/src
            recursive: false
            naming-schema: "%Y-%m-%d_%H-%M-1w"
            lifetime: P7D
            allow-empty: false
            schedule:
              minute: "*"
              hour: "*"
              day-of-month: "*"
              month: "*"
              day-of-week: "*"
          two-weeks:
            dataset: data/src
            recursive: false
            naming-schema: "%Y-%m-%d_%H-%M-2w"
            lifetime: P14D
            allow-empty: false
            schedule:
              minute: "*"
              hour: "*"
              day-of-month: "*"
              month: "*"
              day-of-week: "*"
    """))

    run_periodic_snapshot_test(definition, datetime(2020, 3, 11, 19, 36))

    local_shell = LocalShell()
    assert len(list_snapshots(local_shell, "data/src", False)) == 3
