# -*- coding=utf-8 -*-
import subprocess
import textwrap
from unittest.mock import patch

import yaml

from zettarepl.definition.definition import Definition
from zettarepl.replication.task.task import ReplicationTask
from zettarepl.observer import ReplicationTaskDataProgress
from zettarepl.utils.itertools import select_by_class
from zettarepl.utils.test import set_localhost_transport_options, create_zettarepl, wait_replication_tasks_to_complete


def test_replication_data_progress():
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    subprocess.check_call("zfs create data/src", shell=True)

    subprocess.check_call("dd if=/dev/urandom of=/mnt/data/src/blob bs=1M count=1", shell=True)
    subprocess.check_call("zfs snapshot data/src@2018-10-01_01-00", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        replication-tasks:
          src:
            direction: push
            transport:
              type: ssh
              hostname: 127.0.0.1
            source-dataset:
            - data/src
            target-dataset: data/dst
            recursive: true
            also-include-naming-schema:
            - "%Y-%m-%d_%H-%M"
            auto: false
            retention-policy: none
            retries: 1
    """))
    set_localhost_transport_options(definition["replication-tasks"]["src"]["transport"])
    definition["replication-tasks"]["src"]["speed-limit"] = 10240 * 9

    with patch("zettarepl.replication.run.DatasetSizeObserver.INTERVAL", 5):
        definition = Definition.from_data(definition)
        zettarepl = create_zettarepl(definition)
        zettarepl._spawn_replication_tasks(select_by_class(ReplicationTask, definition.tasks))
        wait_replication_tasks_to_complete(zettarepl)

    calls = [call for call in zettarepl.observer.call_args_list
             if call[0][0].__class__ == ReplicationTaskDataProgress]

    assert len(calls) == 2

    assert 1024 * 1024 * 0.8 <= calls[0][0][0].src_size <= 1024 * 1024 * 1.2
    assert 0 <= calls[0][0][0].dst_size <= 10240 * 1.2

    assert 1024 * 1024 * 0.8 <= calls[1][0][0].src_size <= 1024 * 1024 * 1.2
    assert 10240 * 6 * 0.8 <= calls[1][0][0].dst_size <= 10240 * 6 * 1.2
