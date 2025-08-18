# -*- coding=utf-8 -*-
import os
import subprocess
import textwrap

import yaml

from zettarepl.utils.test import run_replication_test


def test_keeps_mount_structure():
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs receive -A tank/dst", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    subprocess.check_call("zfs create tank/src", shell=True)
    subprocess.check_call("zfs create tank/src/child", shell=True)
    subprocess.check_call("zfs create tank/src/child/grandchild", shell=True)
    with open("/mnt/tank/src/child/grandchild/file", "w") as f:
        pass
    subprocess.check_call("zfs snapshot -r tank/src@2018-10-01_01-00", shell=True)

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
    run_replication_test(definition)

    subprocess.check_call("zfs snapshot -r tank/src@2018-10-01_02-00", shell=True)
    run_replication_test(definition)

    assert os.path.exists("/mnt/tank/dst/child/grandchild/file")
