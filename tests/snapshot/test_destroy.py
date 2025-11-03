# -*- coding=utf-8 -*-
from unittest.mock import ANY, call, Mock, patch

from zettarepl.snapshot.destroy import destroy_snapshots
from zettarepl.snapshot.snapshot import Snapshot


def test__destroy_snapshots__works():
    shell = Mock()

    destroy_snapshots(shell, [Snapshot("data", "snap-1"), Snapshot("data/work", "snap-1"), Snapshot("data", "snap-2")])

    assert shell.exec.call_count == 2
    shell.exec.assert_has_calls([
        call(["zfs", "destroy", "data@snap-1,snap-2"], timeout=ANY),
        call(["zfs", "destroy", "data/work@snap-1"], timeout=ANY)
    ], True)


def test__destroy_snapshots__arg_max():
    shell = Mock()

    with patch("zettarepl.snapshot.destroy.ARG_MAX", 20):
        destroy_snapshots(shell, [Snapshot("data", "snap-1"),
                                  Snapshot("data", "snap-2"),
                                  Snapshot("data", "snap-3")])

    assert shell.exec.call_count == 2
    shell.exec.assert_has_calls([
        call(["zfs", "destroy", "data@snap-1,snap-2"], timeout=ANY),
        call(["zfs", "destroy", "data@snap-3"], timeout=ANY)
    ], True)
