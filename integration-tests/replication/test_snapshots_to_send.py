# -*- coding=utf-8 -*-
import subprocess
import textwrap
from unittest.mock import Mock

import yaml

from zettarepl.observer import ReplicationTaskSnapshotSuccess
from zettarepl.utils.test import run_replication_test


def test_only_from_scratch():
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs receive -A tank/dst", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    subprocess.check_call("zfs create tank/src", shell=True)
    subprocess.check_call("zfs snapshot tank/src@2018-10-01_01-00", shell=True)
    subprocess.check_call("zfs snapshot tank/src@2100-10-01_01-00", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "Europe/Moscow"

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
            only-from-scratch: true
            retention-policy: custom
            lifetime: P7D
            retries: 1
    """))
    observer = Mock(return_value=None)
    run_replication_test(definition, observer=observer)
    """
    [call(<zettarepl.observer.ReplicationTaskStart object at 0x7fda183cd940>),
     call(<zettarepl.observer.ReplicationTaskSnapshotStart object at 0x7fda183ce3c0>),
     call(<zettarepl.observer.ReplicationTaskSnapshotSuccess object at 0x7fda183ceba0>),
     call(<zettarepl.observer.ReplicationTaskSuccess object at 0x7fda183ce7b0>)]
    So it sends only one snapshot, not two
    """
    assert len(observer.call_args_list) == 4
    assert isinstance(observer.call_args_list[2][0][0], ReplicationTaskSnapshotSuccess)
    assert observer.call_args_list[2][0][0].snapshot == "2100-10-01_01-00"
