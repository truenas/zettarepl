# -*- coding=utf-8 -*-
import logging
import subprocess
import textwrap

import pytest
import yaml

from zettarepl.snapshot.list import list_snapshots
from zettarepl.transport.local import LocalShell
from zettarepl.utils.test import run_replication_test, transports


@pytest.mark.parametrize("transport", transports())
@pytest.mark.parametrize("dedup", [False, True])
def test_replication_resume(caplog, transport, dedup):
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs receive -A data/dst", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    subprocess.check_call("zfs create data/src", shell=True)
    subprocess.check_call("dd if=/dev/zero of=/mnt/data/src/blob bs=1M count=1", shell=True)
    subprocess.check_call("zfs snapshot data/src@2018-10-01_01-00", shell=True)

    subprocess.check_call("zfs create data/dst", shell=True)
    subprocess.check_call("(zfs send data/src@2018-10-01_01-00 | throttle -b 102400 | zfs recv -s -F data/dst) & "
                          "sleep 1; killall zfs", shell=True)

    assert "receive_resume_token\t1-" in subprocess.check_output("zfs get -H receive_resume_token data/dst",
                                                                 shell=True, encoding="utf-8")

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        periodic-snapshot-tasks:
          src:
            dataset: data/src
            recursive: true
            lifetime: PT1H
            naming-schema: "%Y-%m-%d_%H-%M"
            schedule:
              minute: "0"

        replication-tasks:
          src:
            direction: push
            source-dataset: data/src
            target-dataset: data/dst
            recursive: true
            periodic-snapshot-tasks:
              - src
            auto: true
            retention-policy: none
    """))
    definition["replication-tasks"]["src"]["transport"] = transport
    definition["replication-tasks"]["src"]["dedup"] = dedup

    caplog.set_level(logging.INFO)
    run_replication_test(definition)

    assert any(
        "Resuming replication for destination dataset" in record.message
        for record in caplog.get_records("call")
    )

    local_shell = LocalShell()
    assert len(list_snapshots(local_shell, "data/dst", False)) == 1
