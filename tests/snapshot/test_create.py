# -*- coding=utf-8 -*-
import pytest
import textwrap
from unittest.mock import ANY, Mock

from zettarepl.snapshot.create import *
from zettarepl.snapshot.snapshot import Snapshot
from zettarepl.transport.interface import ExecException


def test__create_snapshot__zcp_ok():
    shell = Mock()
    shell.exec.return_value = "Channel program fully executed with no return value."

    create_snapshot(shell, Snapshot("data/src", "snap-1"), True, ["data/src/garbage", "data/src/temp"])

    shell.exec.assert_called_once_with(["zfs", "program", "data", ANY, "data/src", "snap-1",
                                        "data/src/garbage", "data/src/temp"])


def test__create_snapshot__zcp_errors():
    shell = Mock()
    shell.exec.side_effect = ExecException(1, textwrap.dedent("""\
        Channel program execution failed:
        [string "channel program"]:44: snapshot=data/src/home@snap-1 error=17, snapshot=data/src/work@snap-1 error=17
        stack traceback:
            [C]: in function 'error'
            [string "channel program"]:44: in main chunk
    """))

    with pytest.raises(CreateSnapshotError) as e:
        create_snapshot(shell, Snapshot("data/src", "snap-1"), True, ["data/src/garbage"])

    assert e.value.args[0] == [
        ("data/src/home@snap-1", "File exists"),
        ("data/src/work@snap-1", "File exists"),
    ]
