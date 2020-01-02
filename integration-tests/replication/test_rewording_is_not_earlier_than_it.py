# -*- coding=utf-8 -*-
import subprocess
import textwrap
from unittest.mock import Mock

import pytest
import yaml

from zettarepl.definition.definition import Definition
from zettarepl.observer import ReplicationTaskError
from zettarepl.replication.task.task import ReplicationTask
from zettarepl.transport.local import LocalShell
from zettarepl.utils.itertools import select_by_class
from zettarepl.utils.test import transports, wait_replication_tasks_to_complete
from zettarepl.zettarepl import Zettarepl


@pytest.mark.parametrize("transport", transports())
@pytest.mark.parametrize("direction", ["push", "pull"])
def test_rewording_is_not_earlier_than_it(transport, direction):
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs receive -A data/dst", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    subprocess.check_call("zfs create data/src", shell=True)
    subprocess.check_call("zfs snapshot data/src@2018-10-01_01-00", shell=True)
    subprocess.check_call("zfs snapshot data/src@2018-10-01_03-00", shell=True)
    subprocess.check_call("zfs snapshot data/src@2018-10-01_02-00", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        replication-tasks:
          src:
            source-dataset: data/src
            target-dataset: data/dst
            recursive: true
            auto: false
            retention-policy: none
            retries: 1
    """))
    definition["replication-tasks"]["src"]["direction"] = direction
    definition["replication-tasks"]["src"]["transport"] = transport
    if direction == "push":
        definition["replication-tasks"]["src"]["also-include-naming-schema"] = "%Y-%m-%d_%H-%M"
    else:
        definition["replication-tasks"]["src"]["naming-schema"] = "%Y-%m-%d_%H-%M"
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
    assert isinstance(error, ReplicationTaskError)
    assert (
        "is newer than" in error.error and
        "but has an older date"  in error.error
    )
