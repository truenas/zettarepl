# -*- coding=utf-8 -*-
import pytest

from zettarepl.transport.local import LocalShell
from zettarepl.transport.timeout import ShellTimeoutContext


def test__timeout():
    with pytest.raises(TimeoutError):
        with ShellTimeoutContext(5):
            LocalShell().exec(["sleep", "10"])
