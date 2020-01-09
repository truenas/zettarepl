# -*- coding=utf-8 -*-
import subprocess
import textwrap
from unittest.mock import Mock

import pytest
import yaml

from zettarepl.definition.definition import Definition
from zettarepl.observer import ReplicationTaskSuccess
from zettarepl.snapshot.list import list_snapshots
from zettarepl.replication.task.task import ReplicationTask
from zettarepl.transport.local import LocalShell
from zettarepl.utils.itertools import select_by_class
from zettarepl.utils.test import transports, wait_replication_tasks_to_complete
from zettarepl.zettarepl import Zettarepl


@pytest.mark.parametrize("dst_parent_is_readonly", [True, False])
@pytest.mark.parametrize("dst_exists", [True, False])
@pytest.mark.parametrize("transport", transports())
@pytest.mark.parametrize("properties", [True, False])
@pytest.mark.parametrize("compression", [None, "pigz", "plzip", "lz4", "xz"])
def test_push_replication(dst_parent_is_readonly, dst_exists, transport, properties, compression):
    if transport["type"] != "ssh" and compression:
        return

    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs destroy -r data/dst_parent", shell=True)

    subprocess.check_call("zfs create data/src", shell=True)
    subprocess.check_call("zfs set test:property=test-value data/src", shell=True)
    subprocess.check_call("zfs snapshot data/src@2018-10-01_01-00", shell=True)
    subprocess.check_call("zfs snapshot data/src@2018-10-01_02-00", shell=True)

    subprocess.check_call("zfs create data/dst_parent", shell=True)
    if dst_exists:
        subprocess.check_call("zfs create data/dst_parent/dst", shell=True)
    if dst_parent_is_readonly:
        subprocess.check_call("zfs set readonly=on data/dst_parent", shell=True)

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
            target-dataset: data/dst_parent/dst
            recursive: true
            periodic-snapshot-tasks:
              - src
            auto: true
            retention-policy: none
            retries: 1
    """))
    definition["replication-tasks"]["src"]["transport"] = transport
    definition["replication-tasks"]["src"]["properties"] = properties
    if compression:
        definition["replication-tasks"]["src"]["compression"] = compression
    definition = Definition.from_data(definition)

    local_shell = LocalShell()
    zettarepl = Zettarepl(Mock(), local_shell)
    zettarepl._spawn_retention = Mock()
    observer = Mock()
    zettarepl.set_observer(observer)
    zettarepl.set_tasks(definition.tasks)
    zettarepl._spawn_replication_tasks(select_by_class(ReplicationTask, definition.tasks))
    wait_replication_tasks_to_complete(zettarepl)

    error = observer.call_args_list[-1][0][0]
    assert isinstance(error, ReplicationTaskSuccess), error

    assert len(list_snapshots(local_shell, "data/dst_parent/dst", False)) == 2

    assert (
        ("test-value" in subprocess.check_output("zfs get test:property data/dst_parent/dst",
                                                 shell=True, encoding="utf-8")) ==
        properties
    )

    subprocess.check_call("zfs snapshot data/src@2018-10-01_03-00", shell=True)

    zettarepl._spawn_replication_tasks(select_by_class(ReplicationTask, definition.tasks))
    wait_replication_tasks_to_complete(zettarepl)

    assert len(list_snapshots(local_shell, "data/dst_parent/dst", False)) == 3
