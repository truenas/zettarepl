# -*- coding=utf-8 -*-
import subprocess
import time

from zettarepl.transport.local import LocalShell
from zettarepl.utils.shlex import pipe


def test__local_async_exec_stop():
    def assert_marker_count(count):
        assert int(subprocess.check_output(
            "ps axw | grep ZETTAREPL_TEST_MARKER_1 | grep -v grep | wc -l", shell=True, encoding="utf-8",
        ).strip()) == count

    subprocess.run(
        "kill -9 $(ps axw | grep ZETTAREPL_TEST_MARKER_1 | grep -v grep | awk '{print $1}')", shell=True,
    )

    assert_marker_count(0)

    local_shell = LocalShell()
    exec = local_shell.exec_async(pipe(["python", "-c", "'ZETTAREPL_TEST_MARKER_1'; import time; time.sleep(60)"],
                                       ["python", "-c", "'ZETTAREPL_TEST_MARKER_1'; import time; time.sleep(60)"]))

    time.sleep(2)
    assert_marker_count(6)

    exec.stop()

    time.sleep(1)
    assert_marker_count(0)
