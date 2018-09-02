# -*- coding=utf-8 -*-
from zettarepl.utils.shlex import pipe


def test__pipe():
    assert (
        pipe(["zfs", "send", "my dataset@snap"], ["zfs", "recv", "my'dataset"]) ==
        ["sh", "-c", "zfs send 'my dataset@snap' | zfs recv 'my'\"'\"'dataset'"]
    )
