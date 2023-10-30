# -*- coding=utf-8 -*-
import subprocess

from zettarepl.snapshot.destroy import destroy_snapshots
from zettarepl.snapshot.list import list_snapshots
from zettarepl.snapshot.snapshot import Snapshot
from zettarepl.transport.local import LocalShell


def test_ignores_nonexisting_snapshots():
    local_shell = LocalShell()

    destroy_snapshots(local_shell, [Snapshot("data", "nonexistent")])


def test_ignores_nonexisting_snapshots_but_destroys_existing():
    local_shell = LocalShell()

    subprocess.call("zfs destroy -r data/src", shell=True)

    subprocess.check_call("zfs create data/src", shell=True)
    subprocess.check_call("zfs snapshot data/src@snap-1", shell=True)

    destroy_snapshots(local_shell, [Snapshot("data", "nonexistent"), Snapshot("data/src", "snap-1")])

    assert list_snapshots(local_shell, "data/src", False) == []
