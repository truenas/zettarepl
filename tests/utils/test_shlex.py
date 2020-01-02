# -*- coding=utf-8 -*-
import subprocess

import pytest

from zettarepl.utils.shlex import pipe


@pytest.mark.parametrize("bad_command", [None] + list(range(4)))
def test__pipe(bad_command):
    commands = [
        "echo a",
        "sed 's/a/)/'",
        "sed 's/)/\"/'",
        "sed 's/\"/d/'",
    ]
    if bad_command is not None:
        commands[bad_command] += f"; echo ERROR 1>&2; exit {bad_command * 10 + 1}"

    piped = pipe(*[["sh", "-c", cmd] for cmd in commands])
    print(piped[2])

    cp = subprocess.run(piped, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")

    assert cp.returncode == (0 if bad_command is None else bad_command * 10 + 1)
    assert cp.stdout == "d\n"
    if bad_command is not None:
        assert cp.stderr == "ERROR\n"
