# -*- coding=utf-8 -*-
import subprocess

from zettarepl.snapshot.destroy import destroy_snapshots
from zettarepl.snapshot.list import list_snapshots
from zettarepl.snapshot.snapshot import Snapshot
from zettarepl.transport.local import LocalShell

snapshots = [
    Snapshot("tank/dst", "2018-10-01_00-00"),
    Snapshot("tank/dst", "2018-10-01_01-00"),
    Snapshot("tank/dst", "2018-10-01_02-00"),
]


def test_zfs_clone():
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    subprocess.check_call("zfs create tank/dst", shell=True)
    for snapshot in snapshots:
        subprocess.check_call(f"zfs snapshot {snapshot.dataset}@{snapshot.name}", shell=True)
    subprocess.check_call(f"zfs clone {snapshots[1].dataset}@{snapshots[1].name} tank/src", shell=True)

    local_shell = LocalShell()
    destroy_snapshots(local_shell, snapshots)

    assert list_snapshots(local_shell, "tank/dst", False) == [snapshots[1]]
