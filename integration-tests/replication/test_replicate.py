# -*- coding=utf-8 -*-
import subprocess
import textwrap
import time
from unittest.mock import Mock

import pytest
import yaml

from zettarepl.definition.definition import Definition
from zettarepl.observer import ReplicationTaskSuccess
from zettarepl.replication.task.task import ReplicationTask
from zettarepl.snapshot.list import list_snapshots
from zettarepl.snapshot.snapshot import Snapshot
from zettarepl.transport.local import LocalShell
from zettarepl.utils.itertools import select_by_class
from zettarepl.utils.test import (
    create_zettarepl, run_replication_test, set_localhost_transport_options, wait_replication_tasks_to_complete,
)


@pytest.mark.parametrize("snapshot_match_options", [
    {"also-include-naming-schema": ["%Y-%m-%d_%H-%M"]},
    {"name-regex": ".+"},
])
def test_retry(snapshot_match_options):
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs receive -A tank/dst", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    subprocess.check_call("zfs create tank/src", shell=True)
    subprocess.check_call("zfs create tank/src/child1", shell=True)
    subprocess.check_call("zfs create tank/src/child2", shell=True)
    subprocess.check_call("zfs snapshot -r tank/src@2021-08-23_19-25", shell=True)
    subprocess.check_call("zfs send -R tank/src@2021-08-23_19-25 | zfs recv tank/dst", shell=True)

    subprocess.check_call("dd if=/dev/urandom of=/mnt/tank/src/child1/blob bs=1M count=1", shell=True)
    subprocess.check_call("zfs snapshot -r tank/src@2021-08-23_19-30", shell=True)

    definition = yaml.safe_load(textwrap.dedent(f"""\
        timezone: "UTC"

        replication-tasks:
          src:
            direction: push
            transport:
              type: ssh
              hostname: 127.0.0.1
            source-dataset: tank/src
            target-dataset: tank/dst
            recursive: true
            replicate: true
            auto: false
            retention-policy: source
            speed-limit: 200000
            retries: 2
    """))
    definition["replication-tasks"]["src"].update(snapshot_match_options)
    set_localhost_transport_options(definition["replication-tasks"]["src"]["transport"])
    definition = Definition.from_data(definition)

    zettarepl = create_zettarepl(definition)
    zettarepl._spawn_replication_tasks(Mock(), select_by_class(ReplicationTask, definition.tasks))

    time.sleep(2)
    subprocess.check_output("kill $(pgrep -f '^zfs recv')", shell=True)

    wait_replication_tasks_to_complete(zettarepl)

    success = zettarepl.observer.call_args_list[-1][0][0]
    assert isinstance(success, ReplicationTaskSuccess), success

    local_shell = LocalShell()
    assert list_snapshots(local_shell, "tank/dst", True) == [
        Snapshot(dataset='tank/dst/child1', name='2021-08-23_19-25'),
        Snapshot(dataset='tank/dst/child1', name='2021-08-23_19-30'),
        Snapshot(dataset='tank/dst/child2', name='2021-08-23_19-25'),
        Snapshot(dataset='tank/dst/child2', name='2021-08-23_19-30'),
        Snapshot(dataset='tank/dst', name='2021-08-23_19-25'),
        Snapshot(dataset='tank/dst', name='2021-08-23_19-30'),
    ]


@pytest.mark.parametrize("snapshot_to_destroy,error_text", [
    # Fake incomplete `zfs send -R` by removing one of the children's most recent snapshots.
    (
        "tank/dst/child2@2021-08-23_19-30",
        (
            f"Last full ZFS replication failed to transfer all the children of the snapshot tank/src@2021-08-23_19-30. "
            "The snapshot tank/dst/child2@2021-08-23_19-30 was not transferred. Please run "
            "`zfs destroy -r tank/dst@2021-08-23_19-30` on the target system and run replication again."
        ),
    ),
    # Older child snapshots might have been removed by retention or manually, we should not care about them.
    ("tank/dst/child2@2021-08-23_19-25", None),
])
@pytest.mark.parametrize("snapshot_match_options", [
    {"also-include-naming-schema": ["%Y-%m-%d_%H-%M"]},
    {"name-regex": ".+"},
])
@pytest.mark.parametrize("take_new_snapshot", [True, False])
def test_replicate(snapshot_to_destroy, error_text, snapshot_match_options, take_new_snapshot):
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs receive -A tank/dst", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    subprocess.check_call("zfs create tank/src", shell=True)
    subprocess.check_call("zfs create tank/src/child1", shell=True)
    subprocess.check_call("zfs create tank/src/child2", shell=True)
    subprocess.check_call("zfs snapshot -r tank/src@2021-08-23_19-25", shell=True)
    subprocess.check_call("zfs snapshot -r tank/src@2021-08-23_19-30", shell=True)
    subprocess.check_call("zfs send -R tank/src@2021-08-23_19-25 | zfs recv tank/dst", shell=True)
    subprocess.check_call("zfs send -R -i tank/src@2021-08-23_19-25 tank/src@2021-08-23_19-30 | "
                          "zfs recv tank/dst", shell=True)
    subprocess.check_call(f"zfs destroy {snapshot_to_destroy}", shell=True)

    if take_new_snapshot:
        subprocess.check_call(f"zfs snapshot -r tank/src@2021-08-23_19-35", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        replication-tasks:
          src:
            direction: push
            transport:
              type: local
            source-dataset: tank/src
            target-dataset: tank/dst
            recursive: true
            replicate: true
            auto: false
            retention-policy: source
            retries: 1
    """))
    definition["replication-tasks"]["src"].update(snapshot_match_options)

    error = run_replication_test(definition, success=error_text is None)
    if error_text is not None:
        assert error.error == error_text
