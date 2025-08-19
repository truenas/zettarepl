# -*- coding=utf-8 -*-
import subprocess
import textwrap
from unittest.mock import Mock

import pytest
import yaml

from zettarepl.definition.definition import Definition
from zettarepl.replication.task.task import ReplicationTask
from zettarepl.observer import (ReplicationTaskStart, ReplicationTaskSnapshotStart, ReplicationTaskSnapshotProgress,
                                ReplicationTaskSnapshotSuccess, ReplicationTaskDataProgress, ReplicationTaskSuccess)
from zettarepl.utils.itertools import select_by_class
from zettarepl.utils.test import transports, create_zettarepl, wait_replication_tasks_to_complete


@pytest.mark.parametrize("transport", transports())
def test_replication_progress(transport):
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    subprocess.check_call("zfs create tank/src", shell=True)

    subprocess.check_call("zfs create tank/src/src1", shell=True)
    subprocess.check_call("zfs snapshot tank/src/src1@2018-10-01_01-00", shell=True)
    subprocess.check_call("dd if=/dev/urandom of=/mnt/tank/src/src1/blob bs=1M count=1", shell=True)
    subprocess.check_call("zfs snapshot tank/src/src1@2018-10-01_02-00", shell=True)
    subprocess.check_call("rm /mnt/tank/src/src1/blob", shell=True)
    subprocess.check_call("zfs snapshot tank/src/src1@2018-10-01_03-00", shell=True)

    subprocess.check_call("zfs create tank/src/src2", shell=True)
    subprocess.check_call("zfs snapshot tank/src/src2@2018-10-01_01-00", shell=True)
    subprocess.check_call("zfs snapshot tank/src/src2@2018-10-01_02-00", shell=True)
    subprocess.check_call("zfs snapshot tank/src/src2@2018-10-01_03-00", shell=True)
    subprocess.check_call("zfs snapshot tank/src/src2@2018-10-01_04-00", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        replication-tasks:
          src:
            direction: push
            source-dataset:
            - tank/src/src1
            - tank/src/src2
            target-dataset: tank/dst
            recursive: true
            also-include-naming-schema:
            - "%Y-%m-%d_%H-%M"
            auto: false
            retention-policy: none
            retries: 1
    """))
    definition["replication-tasks"]["src"]["transport"] = transport
    if transport["type"] == "ssh":
        definition["replication-tasks"]["src"]["speed-limit"] = 10240 * 9

    definition = Definition.from_data(definition)
    zettarepl = create_zettarepl(definition)
    zettarepl._spawn_replication_tasks(Mock(), select_by_class(ReplicationTask, definition.tasks))
    wait_replication_tasks_to_complete(zettarepl)

    calls = [call for call in zettarepl.observer.call_args_list
             if call[0][0].__class__ != ReplicationTaskDataProgress]

    result = [
        ReplicationTaskStart("src"),
        ReplicationTaskSnapshotStart("src",     "tank/src/src1", "2018-10-01_01-00", 0, 3),
        ReplicationTaskSnapshotSuccess("src",   "tank/src/src1", "2018-10-01_01-00", 1, 3),
        ReplicationTaskSnapshotStart("src",     "tank/src/src1", "2018-10-01_02-00", 1, 3),
        ReplicationTaskSnapshotSuccess("src",   "tank/src/src1", "2018-10-01_02-00", 2, 3),
        ReplicationTaskSnapshotStart("src",     "tank/src/src1", "2018-10-01_03-00", 2, 3),
        ReplicationTaskSnapshotSuccess("src",   "tank/src/src1", "2018-10-01_03-00", 3, 3),
        ReplicationTaskSnapshotStart("src",     "tank/src/src2", "2018-10-01_01-00", 3, 7),
        ReplicationTaskSnapshotSuccess("src",   "tank/src/src2", "2018-10-01_01-00", 4, 7),
        ReplicationTaskSnapshotStart("src",     "tank/src/src2", "2018-10-01_02-00", 4, 7),
        ReplicationTaskSnapshotSuccess("src",   "tank/src/src2", "2018-10-01_02-00", 5, 7),
        ReplicationTaskSnapshotStart("src",     "tank/src/src2", "2018-10-01_03-00", 5, 7),
        ReplicationTaskSnapshotSuccess("src",   "tank/src/src2", "2018-10-01_03-00", 6, 7),
        ReplicationTaskSnapshotStart("src",     "tank/src/src2", "2018-10-01_04-00", 6, 7),
        ReplicationTaskSnapshotSuccess("src",   "tank/src/src2", "2018-10-01_04-00", 7, 7),
        ReplicationTaskSuccess("src", []),
    ]

    if transport["type"] == "ssh":
        result.insert(4, ReplicationTaskSnapshotProgress("src", "tank/src/src1", "2018-10-01_02-00", 1, 3,
                                                         10240 * 9 * 10,    # We poll for progress every 10 seconds so
                                                                            # we would have transferred 10x speed limit
                                                         1024 * 1024,
        ))

    for i, message in enumerate(result):
        call = calls[i]

        assert call[0][0].__class__ == message.__class__, calls

        d1 = call[0][0].__dict__
        d2 = message.__dict__

        if isinstance(message, ReplicationTaskSnapshotProgress):
            bytes_sent_1 = d1.pop("bytes_sent")
            bytes_total_1 = d1.pop("bytes_total")
            bytes_sent_2 = d2.pop("bytes_sent")
            bytes_total_2 = d2.pop("bytes_total")

            assert 0.8 <= bytes_sent_1 / bytes_sent_2 <= 1.2
            assert 0.8 <= bytes_total_1 / bytes_total_2 <= 1.2

        assert d1 == d2


def test_replication_progress_resume():
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    subprocess.check_call("zfs create tank/src", shell=True)
    subprocess.check_call("zfs snapshot tank/src@2018-10-01_01-00", shell=True)
    subprocess.check_call("dd if=/dev/urandom of=/mnt/tank/src/blob bs=1M count=1", shell=True)
    subprocess.check_call("zfs snapshot tank/src@2018-10-01_02-00", shell=True)
    subprocess.check_call("dd if=/dev/urandom of=/mnt/tank/src/blob bs=1M count=1", shell=True)
    subprocess.check_call("zfs snapshot tank/src@2018-10-01_03-00", shell=True)
    subprocess.check_call("dd if=/dev/urandom of=/mnt/tank/src/blob bs=1M count=1", shell=True)
    subprocess.check_call("zfs snapshot tank/src@2018-10-01_04-00", shell=True)

    subprocess.check_call("zfs create tank/dst", shell=True)
    subprocess.check_call("zfs send tank/src@2018-10-01_01-00 | zfs recv -s -F tank/dst", shell=True)
    subprocess.check_call("(zfs send -i tank/src@2018-10-01_01-00 tank/src@2018-10-01_02-00 | "
                          " throttle -b 102400 | zfs recv -s -F tank/dst) & "
                          "sleep 1; killall zfs", shell=True)

    assert "receive_resume_token\t1-" in subprocess.check_output("zfs get -H receive_resume_token tank/dst",
                                                                 shell=True, encoding="utf-8")

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
            also-include-naming-schema:
            - "%Y-%m-%d_%H-%M"
            auto: false
            retention-policy: none
            retries: 1
    """))

    definition = Definition.from_data(definition)
    zettarepl = create_zettarepl(definition)
    zettarepl._spawn_replication_tasks(Mock(), select_by_class(ReplicationTask, definition.tasks))
    wait_replication_tasks_to_complete(zettarepl)

    calls = [call for call in zettarepl.observer.call_args_list
             if call[0][0].__class__ != ReplicationTaskDataProgress]

    result = [
        ReplicationTaskStart("src"),
        ReplicationTaskSnapshotStart("src",     "tank/src", "2018-10-01_02-00", 0, 3),
        ReplicationTaskSnapshotSuccess("src",   "tank/src", "2018-10-01_02-00", 1, 3),
        ReplicationTaskSnapshotStart("src",     "tank/src", "2018-10-01_03-00", 1, 3),
        ReplicationTaskSnapshotSuccess("src",   "tank/src", "2018-10-01_03-00", 2, 3),
        ReplicationTaskSnapshotStart("src",     "tank/src", "2018-10-01_04-00", 2, 3),
        ReplicationTaskSnapshotSuccess("src",   "tank/src", "2018-10-01_04-00", 3, 3),
        ReplicationTaskSuccess("src", []),
    ]

    for i, message in enumerate(result):
        call = calls[i]

        assert call[0][0].__class__ == message.__class__

        d1 = call[0][0].__dict__
        d2 = message.__dict__

        assert d1 == d2


def test_replication_progress_pre_calculate():
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    subprocess.check_call("zfs create tank/src", shell=True)
    subprocess.check_call("zfs create tank/src/alice", shell=True)
    subprocess.check_call("zfs create tank/src/bob", shell=True)
    subprocess.check_call("zfs create tank/src/charlie", shell=True)
    subprocess.check_call("zfs snapshot -r tank/src@2018-10-01_01-00", shell=True)

    subprocess.check_call("zfs create tank/dst", shell=True)
    subprocess.check_call("zfs send -R tank/src@2018-10-01_01-00 | zfs recv -s -F tank/dst", shell=True)

    subprocess.check_call("zfs create tank/src/dave", shell=True)
    subprocess.check_call("zfs snapshot -r tank/src@2018-10-01_02-00", shell=True)

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
            also-include-naming-schema:
            - "%Y-%m-%d_%H-%M"
            auto: false
            retention-policy: none
            retries: 1
    """))

    definition = Definition.from_data(definition)
    zettarepl = create_zettarepl(definition)
    zettarepl._spawn_replication_tasks(Mock(), select_by_class(ReplicationTask, definition.tasks))
    wait_replication_tasks_to_complete(zettarepl)

    calls = [call for call in zettarepl.observer.call_args_list
             if call[0][0].__class__ != ReplicationTaskDataProgress]

    result = [
        ReplicationTaskStart("src"),
        ReplicationTaskSnapshotStart("src",     "tank/src",         "2018-10-01_02-00", 0, 5),
        ReplicationTaskSnapshotSuccess("src",   "tank/src",         "2018-10-01_02-00", 1, 5),
        ReplicationTaskSnapshotStart("src",     "tank/src/alice",   "2018-10-01_02-00", 1, 5),
        ReplicationTaskSnapshotSuccess("src",   "tank/src/alice",   "2018-10-01_02-00", 2, 5),
        ReplicationTaskSnapshotStart("src",     "tank/src/bob",     "2018-10-01_02-00", 2, 5),
        ReplicationTaskSnapshotSuccess("src",   "tank/src/bob",     "2018-10-01_02-00", 3, 5),
        ReplicationTaskSnapshotStart("src",     "tank/src/charlie", "2018-10-01_02-00", 3, 5),
        ReplicationTaskSnapshotSuccess("src",   "tank/src/charlie", "2018-10-01_02-00", 4, 5),
        ReplicationTaskSnapshotStart("src",     "tank/src/dave",    "2018-10-01_02-00", 4, 5),
        ReplicationTaskSnapshotSuccess("src",   "tank/src/dave",    "2018-10-01_02-00", 5, 5),
        ReplicationTaskSuccess("src", []),
    ]

    for i, message in enumerate(result):
        call = calls[i]

        assert call[0][0].__class__ == message.__class__

        d1 = call[0][0].__dict__
        d2 = message.__dict__

        assert d1 == d2
