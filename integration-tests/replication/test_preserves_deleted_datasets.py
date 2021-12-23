# -*- coding=utf-8 -*-
import subprocess
import textwrap

import yaml

from zettarepl.definition.definition import Definition
from zettarepl.observer import ReplicationTaskSuccess
from zettarepl.snapshot.list import list_snapshots
from zettarepl.replication.task.task import ReplicationTask
from zettarepl.transport.local import LocalShell
from zettarepl.utils.itertools import select_by_class
from zettarepl.utils.test import create_zettarepl, wait_replication_tasks_to_complete


def test_push_replication():
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    subprocess.check_call("zfs create data/src", shell=True)
    subprocess.check_call("zfs create data/src/child", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2018-10-01_01-00", shell=True)

    subprocess.check_call("zfs create data/dst", shell=True)

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
            transport:
              type: local
            source-dataset: data/src
            target-dataset: data/dst
            recursive: true
            periodic-snapshot-tasks:
              - src
            auto: true
            retention-policy: none
    """))

    definition = Definition.from_data(definition)
    zettarepl = create_zettarepl(definition)
    zettarepl._spawn_replication_tasks(None, select_by_class(ReplicationTask, definition.tasks))
    wait_replication_tasks_to_complete(zettarepl)

    assert sum(1 for m in zettarepl.observer.call_args_list if isinstance(m[0][0], ReplicationTaskSuccess)) == 1

    subprocess.check_call("zfs destroy -r data/src/child", shell=True)
    subprocess.check_call("zfs snapshot data/src@2018-10-01_02-00", shell=True)

    zettarepl._spawn_replication_tasks(None, select_by_class(ReplicationTask, definition.tasks))
    wait_replication_tasks_to_complete(zettarepl)

    assert sum(1 for m in zettarepl.observer.call_args_list if isinstance(m[0][0], ReplicationTaskSuccess)) == 2

    local_shell = LocalShell()
    assert len(list_snapshots(local_shell, "data/dst/child", False)) == 1
