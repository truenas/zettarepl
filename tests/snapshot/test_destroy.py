# -*- coding=utf-8 -*-
from mock import call, Mock

from zettarepl.snapshot import Snapshot
from zettarepl.snapshot.destroy import destroy_snapshots


def test__destroy_snapshots__works():
    shell = Mock()

    destroy_snapshots(shell, [Snapshot("data", "snap-1"), Snapshot("data/work", "snap-1"), Snapshot("data", "snap-2")])

    assert shell.exec.call_count == 2
    shell.exec.assert_has_calls([
        call(["zfs", "destroy", "data@snap-1%snap-2"]),
        call(["zfs", "destroy", "data/work@snap-1"])
    ], True)
