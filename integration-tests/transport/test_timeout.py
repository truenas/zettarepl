# -*- coding=utf-8 -*-
import pytest

from zettarepl.transport.local import LocalShell


def test__timeout():
    with pytest.raises(TimeoutError):
        LocalShell().exec(["sleep", "10"], timeout=5)
