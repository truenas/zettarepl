from datetime import datetime
import subprocess
import textwrap

import pytest
import yaml

from zettarepl.observer import PeriodicSnapshotTaskError
from zettarepl.snapshot.create import create_snapshot, CreateSnapshotError
from zettarepl.snapshot.snapshot import Snapshot
from zettarepl.transport.local import LocalShell
from zettarepl.utils.test import run_periodic_snapshot_test


def test_rewrites_error_message():
    subprocess.call("zfs destroy -r tank/src", shell=True)

    subprocess.call("zfs create tank/src", shell=True)
    subprocess.call("zfs create tank/src/child1", shell=True)
    subprocess.call("zfs create tank/src/child2", shell=True)
    subprocess.call("zfs snapshot -r tank/src@snap-1", shell=True)

    with pytest.raises(CreateSnapshotError) as ve:
        create_snapshot(LocalShell(), Snapshot("tank/src", "snap-1"), True, [], {})

    assert ve.value.error == "no snapshots were created"
    assert sorted(ve.value.snapshots_errors) == [
        (Snapshot("tank/src", "snap-1"), "snapshot already exists"),
        (Snapshot("tank/src/child1", "snap-1"), "snapshot already exists"),
        (Snapshot("tank/src/child2", "snap-1"), "snapshot already exists"),
    ]


def test_rewrites_error_message_with_exclude():
    subprocess.call("zfs destroy -r tank/src", shell=True)

    subprocess.call("zfs create tank/src", shell=True)
    subprocess.call("zfs create tank/src/child1", shell=True)
    subprocess.call("zfs create tank/src/child2", shell=True)
    subprocess.call("zfs snapshot -r tank/src@snap-1", shell=True)

    with pytest.raises(CreateSnapshotError) as ve:
        create_snapshot(LocalShell(), Snapshot("tank/src", "snap-1"), True, ["tank/src/child2"], {})

    assert ve.value.error == "no snapshots were created"
    assert sorted(ve.value.snapshots_errors) == [
        (Snapshot("tank/src", "snap-1"), "snapshot already exists"),
        (Snapshot("tank/src/child1", "snap-1"), "snapshot already exists"),
    ]


def test_rewrites_error_message_for_task():
    subprocess.call("zfs destroy -r tank/src", shell=True)

    subprocess.check_call("zfs create tank/src", shell=True)
    subprocess.check_call("zfs create tank/src/child1", shell=True)
    subprocess.check_call("zfs create tank/src/child2", shell=True)
    subprocess.check_call("zfs create tank/src/child3", shell=True)
    subprocess.check_call("zfs create tank/src/child4", shell=True)
    subprocess.check_call("zfs create tank/src/child5", shell=True)
    subprocess.check_call("zfs snapshot -r tank/src@2026-03-17_00-00", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        periodic-snapshot-tasks:
          test:
            dataset: tank/src
            recursive: true
            naming-schema: "%Y-%m-%d_%H-%M"
            schedule:
              minute: "*"
              hour: "*"
              day-of-month: "*"
              month: "*"
              day-of-week: "*"
    """))

    error = run_periodic_snapshot_test(definition, datetime(2026, 3, 17, 0, 0), False)
    assert error.error == textwrap.dedent("""\
        Failed to create following snapshots:\n
        'tank/src@2026-03-17_00-00' (and 5 nested datasets): snapshot already exists
    """)


def test_rewrites_error_message_for_complex_task():
    subprocess.call("zfs destroy -r tank/src", shell=True)

    subprocess.check_call("zfs create tank/src", shell=True)
    subprocess.check_call("zfs create tank/src/child1", shell=True)
    subprocess.check_call("zfs create tank/src/child1/nestedchild", shell=True)
    subprocess.check_call("zfs create tank/src/child2", shell=True)
    subprocess.check_call("zfs snapshot -r tank/src@2026-03-17_00-00", shell=True)
    subprocess.check_call("zfs destroy tank/src@2026-03-17_00-00", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        periodic-snapshot-tasks:
          test:
            dataset: tank/src
            recursive: true
            naming-schema: "%Y-%m-%d_%H-%M"
            schedule:
              minute: "*"
              hour: "*"
              day-of-month: "*"
              month: "*"
              day-of-week: "*"
    """))

    error = run_periodic_snapshot_test(definition, datetime(2026, 3, 17, 0, 0), False)
    assert error.error == textwrap.dedent("""\
        Failed to create following snapshots:\n
        'tank/src/child1@2026-03-17_00-00' (and 1 nested datasets): snapshot already exists
        'tank/src/child2@2026-03-17_00-00': snapshot already exists
    """)
