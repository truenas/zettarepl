# -*- coding=utf-8 -*-
import pytest
import textwrap
from unittest.mock import ANY, Mock, call

from zettarepl.snapshot.create import *
from zettarepl.snapshot.snapshot import Snapshot
from zettarepl.transport.interface import ExecException


def test__create_snapshot__zfscli_no_properties():
    shell = Mock()

    create_snapshot(shell, Snapshot("data/src", "snap-1"), True, [], {})

    shell.exec.assert_called_once_with(["zfs", "snapshot", "-r", "data/src@snap-1"])


def test__create_snapshot__zfscli_properties():
    shell = Mock()

    create_snapshot(shell, Snapshot("data/src", "snap-1"), True, [], {"freenas:vmsynced": "Y"})

    shell.exec.assert_called_once_with(["zfs", "snapshot", "-r", "-o", "freenas:vmsynced=Y", "data/src@snap-1"])


def test__create_snapshot__zcp_ok():
    shell = Mock()
    shell.exec.return_value = "Channel program fully executed with no return value."

    create_snapshot(shell, Snapshot("data/src", "snap-1"), True, ["data/src/garbage", "data/src/temp"], {})

    shell.exec.assert_has_calls([call(["zfs", "list", "-t", "filesystem,volume", "-H", "-o", "name", "-s", "name", "-r", "data/src"]), call(["zfs", "program", "data", ANY])])


def test__create_snapshot__zcp_errors():
    shell = Mock()

    list_dataset_shell_call = textwrap.dedent("""\
       boot-pool	2.80G	15.6G	96K	none
        boot-pool/ROOT	2.79G	15.6G	96K	none
        boot-pool/ROOT/22.02.3	2.79G	15.6G	2.77G	legacy
        boot-pool/ROOT/Initial-Install	8K	15.6G	2.66G	/
        boot-pool/grub	8.18M	15.6G	8.18M	legacy
        data	13.8M	2.61G	104K	/mnt/data
        data/.system	9.95M	2.61G	104K	legacy
        data/.system/configs-cd93307f360c4818ad53abf4dac4059c	96K	2.61G	96K	legacy
        data/.system/cores	96K	1024M	96K	legacy
        data/.system/ctdb_shared_vol	96K	2.61G	96K	legacy
        data/.system/glusterd	104K	2.61G	104K	legacy
        data/.system/rrd-cd93307f360c4818ad53abf4dac4059c	8.68M	2.61G	8.68M	legacy
        data/.system/samba4	148K	2.61G	148K	legacy
        data/.system/services	96K	2.61G	96K	legacy
        data/.system/syslog-cd93307f360c4818ad53abf4dac4059c	464K	2.61G	464K	legacy
        data/.system/webui	96K	2.61G	96K	legacy
        data/src	792K	2.61G	112K	/mnt/data/src
        data/src/DISK1	296K	2.61G	104K	/mnt/data/src/DISK1
        data/src/DISK1/Apps	96K	2.61G	96K	/mnt/data/src/DISK1/Apps
        data/src/DISK1/ISO	96K	2.61G	96K	/mnt/data/src/DISK1/ISO
        data/src/DISK2	288K	2.61G	96K	/mnt/data/src/DISK2
        data/src/DISK2/Apps	96K	2.61G	96K	/mnt/data/src/DISK2/Apps
        data/src/DISK2/ISO	96K	2.61G	96K	/mnt/data/src/DISK2/ISO
        data/src/waggnas	96K	2.61G	96K	/mnt/data/src/waggnas
    """)

    zcp_program_shell_call = ExecException(1, textwrap.dedent("""\
        Channel program execution failed:
        [string "channel program"]:44: snapshot=data/src/home@snap-1 error=17, snapshot=data/src/work@snap-1 error=17
        stack traceback:
            [C]: in function 'error'
            [string "channel program"]:44: in main chunk
    """))

    shell.exec.side_effect = [list_dataset_shell_call, zcp_program_shell_call]

    with pytest.raises(CreateSnapshotError) as e:
        create_snapshot(shell, Snapshot("data/src", "snap-1"), True, ["data/src/garbage"], {})

    assert e.value.args[0] == (
        "Failed to create following snapshots:\n"
        "'data/src/home@snap-1': File exists\n"
        "'data/src/work@snap-1': File exists"
    )
