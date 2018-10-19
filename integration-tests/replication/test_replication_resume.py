# -*- coding=utf-8 -*-
import subprocess
import textwrap
from unittest.mock import Mock

import yaml

from zettarepl.definition.definition import Definition
from zettarepl.snapshot.list import list_snapshots
from zettarepl.replication.task.task import ReplicationTask
from zettarepl.transport.local import LocalShell
from zettarepl.utils.itertools import select_by_class
from zettarepl.zettarepl import Zettarepl


def test_replication_resume(caplog):
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    subprocess.check_call("zfs create data/src", shell=True)
    subprocess.check_call("dd if=/dev/zero of=/mnt/data/src/blob bs=1M count=1", shell=True)
    subprocess.check_call("zfs snapshot data/src@2018-10-01_01-00", shell=True)

    subprocess.check_call("zfs create data/dst", shell=True)
    subprocess.check_call("(zfs send data/src@2018-10-01_01-00 | throttle -b 102400 | zfs recv -s -F data/dst) & "
                          "sleep 1; killall zfs", shell=True)

    assert "receive_resume_token\t1-" in subprocess.check_output("zfs get -H receive_resume_token data/dst",
                                                                 shell=True, encoding="utf-8")

    definition = Definition.from_data(yaml.load(textwrap.dedent("""\
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
            transport:
              type: local
            source-dataset: data/src
            target-dataset: data/dst
            recursive: true
            periodic-snapshot-tasks:
              - src
            auto: true
            retention-policy: none
    """)))

    local_shell = LocalShell()
    zettarepl = Zettarepl(Mock(), local_shell)
    zettarepl.set_tasks(definition.tasks)
    zettarepl._run_replication_tasks(select_by_class(ReplicationTask, definition.tasks))

    assert any(
        "Resuming replication for dst_dataset" in record.message
        for record in caplog.get_records("call")
    )

    assert len(list_snapshots(local_shell, "data/dst", False)) == 1
