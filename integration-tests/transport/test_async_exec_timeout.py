# -*- coding=utf-8 -*-
import copy
import subprocess
import time

import pytest

from zettarepl.transport.create import create_transport
from zettarepl.utils.test import transports


@pytest.mark.parametrize("transport", transports(False))
@pytest.mark.parametrize("stdout", [True, False])
def test__async_exec_timeout(transport, stdout):
    if transport["type"] == "local":
        expected_timeout = 5
    else:
        expected_timeout = 10

    transport_inst = create_transport(copy.deepcopy(transport))
    shell = transport_inst.shell(transport_inst)

    start = time.monotonic()
    with pytest.raises(TimeoutError):
        shell.exec(["python", "-c", "'ZETTAREPL_TEST_MARKER_1'; import time; time.sleep(15)"], timeout=5)
    end = time.monotonic()
    assert expected_timeout * 0.9 < end - start < expected_timeout * 1.1

    if transport["type"] == "local":
        assert int(subprocess.check_output(
            "ps axw | grep ZETTAREPL_TEST_MARKER_1 | grep -v grep | wc -l", shell=True, encoding="utf-8",
        ).strip()) == 0
