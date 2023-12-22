# -*- coding=utf-8 -*-
import subprocess
import textwrap
from unittest.mock import Mock

import pytest
import yaml

from zettarepl.definition.definition import Definition
from zettarepl.observer import ReplicationTaskSuccess, ReplicationTaskError
from zettarepl.snapshot.list import list_snapshots
from zettarepl.replication.task.task import ReplicationTask
from zettarepl.transport.local import LocalShell
from zettarepl.utils.itertools import select_by_class
from zettarepl.utils.test import transports, create_dataset, wait_replication_tasks_to_complete
from zettarepl.zettarepl import Zettarepl


@pytest.mark.parametrize("dst_parent_is_readonly", [True, False])
@pytest.mark.parametrize("dst_exists", [True, False])
@pytest.mark.parametrize("transport", transports())
@pytest.mark.parametrize("replicate", [True, False])
@pytest.mark.parametrize("properties", [True, False])
@pytest.mark.parametrize("encrypted", [True, False])
@pytest.mark.parametrize("has_encrypted_child", [True, False])
@pytest.mark.parametrize("dst_parent_encrypted", [True, False])
def test_push_replication(dst_parent_is_readonly, dst_exists, transport, replicate, properties, encrypted,
                          has_encrypted_child, dst_parent_encrypted):
    if replicate and not properties:
        return
    if encrypted and has_encrypted_child:
        # If parent is encrypted, child is also encrypted
        return

    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs destroy -r data/dst_parent", shell=True)

    create_dataset("data/src", encrypted)
    subprocess.check_call("zfs set test:property=test-value data/src", shell=True)
    if has_encrypted_child:
        create_dataset("data/src/child", True)
    subprocess.check_call("zfs snapshot -r data/src@2018-10-01_01-00", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2018-10-01_02-00", shell=True)

    create_dataset("data/dst_parent", dst_parent_encrypted)
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
    definition["replication-tasks"]["src"]["replicate"] = replicate
    definition["replication-tasks"]["src"]["properties"] = properties
    definition = Definition.from_data(definition)

    local_shell = LocalShell()
    zettarepl = Zettarepl(Mock(), local_shell)
    zettarepl._spawn_retention = Mock()
    observer = Mock()
    zettarepl.set_observer(observer)
    zettarepl.set_tasks(definition.tasks)
    zettarepl._spawn_replication_tasks(Mock(), select_by_class(ReplicationTask, definition.tasks))
    wait_replication_tasks_to_complete(zettarepl)

    if dst_exists and properties and encrypted and not dst_parent_encrypted:
        error = observer.call_args_list[-1][0][0]
        assert isinstance(error, ReplicationTaskError), error
        assert error.error == ("Unable to send encrypted dataset 'data/src' to existing unencrypted or unrelated "
                               "dataset 'data/dst_parent/dst'")
        return

    error = observer.call_args_list[-1][0][0]
    assert isinstance(error, ReplicationTaskSuccess), error

    assert len(list_snapshots(local_shell, "data/dst_parent/dst", False)) == 2

    assert (
        ("test-value" in subprocess.check_output("zfs get test:property data/dst_parent/dst",
                                                 shell=True, encoding="utf-8")) ==
        properties
    )

    subprocess.check_call("zfs snapshot -r data/src@2018-10-01_03-00", shell=True)

    zettarepl._spawn_replication_tasks(Mock(), select_by_class(ReplicationTask, definition.tasks))
    wait_replication_tasks_to_complete(zettarepl)

    error = observer.call_args_list[-1][0][0]
    assert isinstance(error, ReplicationTaskSuccess), error

    assert len(list_snapshots(local_shell, "data/dst_parent/dst", False)) == 3

    warnings = []
    if properties and encrypted:
        pass
    elif dst_parent_is_readonly and not dst_exists:
        warnings.append(
            "cannot mount '/mnt/data/dst_parent/dst': failed to create mountpoint: Read-only file system"
        )
    if has_encrypted_child:
        if properties:
            pass
        elif dst_parent_is_readonly and not dst_exists:
            warnings.append(
                "cannot mount '/mnt/data/dst_parent/dst/child': failed to create mountpoint: Read-only file system"
            )

    assert error.warnings == warnings
