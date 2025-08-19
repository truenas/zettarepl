# -*- coding=utf-8 -*-
import logging
import subprocess
import textwrap
import time
from unittest.mock import Mock

import pytest
import yaml

from zettarepl.definition.definition import Definition
from zettarepl.observer import ReplicationTaskSuccess
from zettarepl.snapshot.list import list_snapshots
from zettarepl.replication.task.task import ReplicationTask
from zettarepl.transport.local import LocalShell
from zettarepl.utils.itertools import select_by_class
from zettarepl.utils.test import create_zettarepl, set_localhost_transport_options, wait_replication_tasks_to_complete


@pytest.mark.parametrize("direction", ["push", "pull"])
def test_replication_retry(caplog, direction):
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs receive -A tank/dst", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    subprocess.check_call("zfs create tank/src", shell=True)
    subprocess.check_call("dd if=/dev/urandom of=/mnt/tank/src/blob bs=1M count=1", shell=True)
    subprocess.check_call("zfs snapshot tank/src@2018-10-01_01-00", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        periodic-snapshot-tasks:
          src:
            dataset: tank/src
            recursive: true
            lifetime: PT1H
            naming-schema: "%Y-%m-%d_%H-%M"
            schedule:
              minute: "0"

        replication-tasks:
          src:
            transport:
              type: ssh
              hostname: 127.0.0.1
            source-dataset: tank/src
            target-dataset: tank/dst
            recursive: true
            auto: false
            retention-policy: none
            speed-limit: 200000
            retries: 2
    """))
    definition["replication-tasks"]["src"]["direction"] = direction
    if direction == "push":
        definition["replication-tasks"]["src"]["periodic-snapshot-tasks"] = ["src"]
    else:
        definition["replication-tasks"]["src"]["naming-schema"] = ["%Y-%m-%d_%H-%M"]
    set_localhost_transport_options(definition["replication-tasks"]["src"]["transport"])
    definition = Definition.from_data(definition)

    caplog.set_level(logging.INFO)
    zettarepl = create_zettarepl(definition)
    zettarepl._spawn_replication_tasks(Mock(), select_by_class(ReplicationTask, definition.tasks))

    time.sleep(2)
    if direction == "push":
        subprocess.check_output("kill $(pgrep -f '^zfs recv')", shell=True)
    else:
        subprocess.check_output("kill $(pgrep -f '^(zfs send|zfs: sending)')", shell=True)

    wait_replication_tasks_to_complete(zettarepl)

    assert any(
        " recoverable replication error" in record.message
        for record in caplog.get_records("call")
    )
    assert any(
        "Resuming replication for destination dataset" in record.message
        for record in caplog.get_records("call")
    )

    success = zettarepl.observer.call_args_list[-1][0][0]
    assert isinstance(success, ReplicationTaskSuccess), success

    local_shell = LocalShell()
    assert len(list_snapshots(local_shell, "tank/dst", False)) == 1
