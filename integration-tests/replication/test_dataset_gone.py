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
def test_dataset_gone(transport):
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    subprocess.check_call("zfs create tank/src", shell=True)
    subprocess.check_call("zfs create tank/src/a", shell=True)
    subprocess.check_call("zfs create tank/src/b", shell=True)
    subprocess.check_call("zfs snapshot -r tank/src@2018-10-01_01-00", shell=True)
    subprocess.check_call("zfs snapshot -r tank/src@2018-10-01_02-00", shell=True)

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
            direction: push
            source-dataset: tank/src
            target-dataset: tank/dst
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
            # Datasets are already listed, and now we remove one of them to simulate removing a dataset during
            # the replication. Only do this once.
            subprocess.check_call("zfs destroy -r tank/src/b", shell=True)
            deleted = True

        return resume_replications(*args, **kwargs)

    with patch("zettarepl.replication.run.resume_replications", resume_replications_mock):
        zettarepl._spawn_replication_tasks(Mock(), select_by_class(ReplicationTask, definition.tasks))
        wait_replication_tasks_to_complete(zettarepl)

    error = observer.call_args_list[-1][0][0]
    assert isinstance(error, ReplicationTaskSuccess), error

    assert len(list_snapshots(local_shell, "tank/dst/a", False)) == 2
