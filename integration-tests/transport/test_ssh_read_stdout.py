# -*- coding=utf-8 -*-
import pytest

from zettarepl.transport.ssh import SshTransport
from zettarepl.utils.test import set_localhost_transport_options


@pytest.mark.parametrize("bufsize", [1, 9000])
def test__ssh_read_stdout(bufsize):
    data = dict(hostname="127.0.0.1", port=22, username="root")
    set_localhost_transport_options(data)
    transport = SshTransport.from_data(data)
    f1 = "0" * bufsize
    f2 = "1" * bufsize
    f3 = "2" * bufsize
    result = transport.shell(transport).exec(["sh", "-c", f"echo {f1}; sleep 15; echo {f2}; sleep 15; echo {f3}"])
    assert result == f"{f1}\n{f2}\n{f3}\n"
