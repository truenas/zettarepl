# -*- coding=utf-8 -*-
import subprocess
import textwrap

import yaml

from zettarepl.snapshot.list import list_snapshots
from zettarepl.transport.local import LocalShell
from zettarepl.utils.test import run_replication_test


def test_multiple_source_datasets():
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs receive -A tank/dst", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    subprocess.check_call("zfs create tank/src", shell=True)
    subprocess.check_call("zfs create tank/src/internal", shell=True)
    subprocess.check_call("zfs create tank/src/internal/DISK1", shell=True)
    subprocess.check_call("zfs create tank/src/internal/DISK1/Apps", shell=True)
    subprocess.check_call("zfs create tank/src/internal/DISK1/ISO", shell=True)
    subprocess.check_call("zfs snapshot -r tank/src@2018-10-01_01-00", shell=True)

    subprocess.check_call("zfs create tank/dst", shell=True)
    subprocess.check_call("zfs create tank/dst/core", shell=True)
    subprocess.check_call("zfs send -R tank/src/internal/DISK1@2018-10-01_01-00 | "
                          "zfs recv tank/dst/core/tsaukpaetra", shell=True)

    subprocess.check_call("zfs snapshot -r tank/src@2018-10-01_02-00", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        replication-tasks:
          src:
            direction: push
            transport:
              type: local
            source-dataset:
              - tank/src/internal/DISK1/Apps
              - tank/src/internal/DISK1/ISO
            target-dataset: tank/dst/core/tsaukpaetra
            recursive: false
            also-include-naming-schema:
              - "%Y-%m-%d_%H-%M"
            auto: false
            retention-policy: none
            retries: 1
    """))

    run_replication_test(definition)

    local_shell = LocalShell()
    assert len(list_snapshots(local_shell, "tank/dst/core/tsaukpaetra/Apps", False)) == 2
    assert len(list_snapshots(local_shell, "tank/dst/core/tsaukpaetra/ISO", False)) == 2
