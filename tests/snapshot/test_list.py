# -*- coding=utf-8 -*-
from mock import Mock

from zettarepl.snapshot.list import list_snapshots


def test__list_snapshots__empty():
    shell = Mock()
    shell.exec.return_value = ""

    assert list_snapshots(shell, "/mnt/data") == []


def test__list_snapshots__non_empty():
    shell = Mock()
    shell.exec.return_value = "snap1\nsnap2\n"

    assert list_snapshots(shell, "/mnt/data") == ["snap1", "snap2"]
