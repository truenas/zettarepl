# -*- coding=utf-8 -*-
from queue import Queue
import time

import pytest

from zettarepl.transport.ssh import SshTransport
from zettarepl.utils.test import set_localhost_transport_options


@pytest.mark.parametrize("bufsize", [1, 9000])
@pytest.mark.parametrize("stdout_to_queue", [False, True])
def test__ssh_read_stdout(bufsize, stdout_to_queue):
    data = dict(hostname="127.0.0.1", port=22, username="root")
    set_localhost_transport_options(data)
    transport = SshTransport.from_data(data)
    f1 = "0" * bufsize
    f2 = "1" * bufsize
    f3 = "2" * bufsize
    q = None
    if stdout_to_queue:
        q = Queue()
    start = time.monotonic()
    result = transport.shell(transport).exec(["sh", "-c", f"echo {f1}; sleep 15; echo {f2}; sleep 15; echo {f3}"],
                                             stdout=q)
    if stdout_to_queue:
        result = []

        while True:
            data = q.get()
            if data is None:
                break

            result.append(data)

        assert result == [f"{f1}\n", f"{f2}\n", f"{f3}\n"]
    else:
        assert result == f"{f1}\n{f2}\n{f3}\n"

    assert time.monotonic() - start <= (15 + 15) * 1.1
