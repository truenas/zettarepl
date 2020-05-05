# -*- coding=utf-8 -*-
import pytest
import subprocess

from zettarepl.snapshot.destroy import destroy_snapshots
from zettarepl.snapshot.list import list_snapshots
from zettarepl.snapshot.snapshot import Snapshot
from zettarepl.transport.local import LocalShell

snapshots = [
    Snapshot("data/dst", "2018-10-01_00-00"),
    Snapshot("data/dst", "2018-10-01_01-00"),
    Snapshot("data/dst", "2018-10-01_02-00"),
    Snapshot("data/dst", "2018-10-01_03-00"),
]


@pytest.mark.parametrize("hold", [
    [],
    [1],
    [1, 2],
    [1, 3],
    [0, 1, 2, 3],
])
def test_zfs_hold(hold):
    subprocess.call("zfs destroy -r data/src", shell=True)
    for snapshot in snapshots:
        subprocess.call(f"zfs release keep {snapshot.dataset}@{snapshot.name}", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    subprocess.check_call("zfs create data/dst", shell=True)
    for snapshot in snapshots:
        subprocess.check_call(f"zfs snapshot {snapshot.dataset}@{snapshot.name}", shell=True)
    for i in hold:
        snapshot = snapshots[i]
        subprocess.check_call(f"zfs hold keep {snapshot.dataset}@{snapshot.name}", shell=True)

    local_shell = LocalShell()
    destroy_snapshots(local_shell, snapshots)

    assert list_snapshots(local_shell, "data/dst", False) == [snapshots[i] for i in hold]
