# -*- coding=utf-8 -*-
import subprocess
import textwrap
from unittest.mock import Mock, patch

import pytest
import yaml

from zettarepl.definition.definition import Definition
from zettarepl.observer import ReplicationTaskSuccess
from zettarepl.snapshot.list import list_snapshots
from zettarepl.replication.run import resume_replications
from zettarepl.replication.task.task import ReplicationTask
from zettarepl.transport.local import LocalShell
from zettarepl.utils.itertools import select_by_class
from zettarepl.utils.test import transports, wait_replication_tasks_to_complete
from zettarepl.zettarepl import Zettarepl


@pytest.mark.parametrize("transport", transports())
def test_snapshot_gone(transport):
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    subprocess.check_call("zfs create data/src", shell=True)
    subprocess.check_call("zfs snapshot data/src@2018-10-01_01-00", shell=True)
    subprocess.check_call("zfs snapshot data/src@2018-10-01_02-00", shell=True)

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
            retries: 2
    """))
    definition["replication-tasks"]["src"]["transport"] = transport
    definition = Definition.from_data(definition)

    local_shell = LocalShell()
    zettarepl = Zettarepl(Mock(), local_shell)
    zettarepl._spawn_retention = Mock()
    observer = Mock()
    zettarepl.set_observer(observer)
    zettarepl.set_tasks(definition.tasks)

    deleted = False
    def resume_replications_mock(*args, **kwargs):
        nonlocal deleted
        if not deleted:
            # Snapshots are already listed, and now we remove one of them to simulate PULL replication
            # from remote system that has `allow_empty_snapshots: false`. Only do this once.
            subprocess.check_call("zfs destroy data/src@2018-10-01_01-00", shell=True)
            deleted = True

        return resume_replications(*args, **kwargs)

    with patch("zettarepl.replication.run.resume_replications", resume_replications_mock):
        zettarepl._spawn_replication_tasks(select_by_class(ReplicationTask, definition.tasks))
        wait_replication_tasks_to_complete(zettarepl)

    error = observer.call_args_list[-1][0][0]
    assert isinstance(error, ReplicationTaskSuccess), error

    assert len(list_snapshots(local_shell, "data/dst", False)) == 1
