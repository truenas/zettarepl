# -*- coding=utf-8 -*-
import os
import subprocess
import textwrap

import yaml

from zettarepl.snapshot.list import list_snapshots
from zettarepl.transport.local import LocalShell
from zettarepl.utils.test import run_replication_test


def test_keeps_mount_structure():
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs receive -A data/dst", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    subprocess.check_call("zfs create data/src", shell=True)
    subprocess.check_call("zfs create data/src/child", shell=True)
    subprocess.check_call("zfs create data/src/child/grandchild", shell=True)
    with open("/mnt/data/src/child/grandchild/file", "w") as f:
        pass
    subprocess.check_call("zfs snapshot -r data/src@2018-10-01_01-00", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        replication-tasks:
          src:
            direction: push
            transport:
              type: local
            source-dataset: data/src
            target-dataset: data/dst
            recursive: true
            also-include-naming-schema:
              - "%Y-%m-%d_%H-%M"
            auto: false
            retention-policy: none
            retries: 1
    """))
    run_replication_test(definition)

    subprocess.check_call("zfs snapshot -r data/src@2018-10-01_02-00", shell=True)
    run_replication_test(definition)

    assert os.path.exists("/mnt/data/dst/child/grandchild/file")
