# -*- coding=utf-8 -*-
import subprocess
import textwrap

import yaml

from zettarepl.snapshot.list import list_snapshots
from zettarepl.transport.local import LocalShell
from zettarepl.utils.test import run_replication_test


def test_multiple_source_datasets():
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs receive -A data/dst", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    subprocess.check_call("zfs create data/src", shell=True)
    subprocess.check_call("zfs create data/src/internal", shell=True)
    subprocess.check_call("zfs create data/src/internal/DISK1", shell=True)
    subprocess.check_call("zfs create data/src/internal/DISK1/Apps", shell=True)
    subprocess.check_call("zfs create data/src/internal/DISK1/ISO", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2018-10-01_01-00", shell=True)

    subprocess.check_call("zfs create data/dst", shell=True)
    subprocess.check_call("zfs create data/dst/core", shell=True)
    subprocess.check_call("zfs send -R data/src/internal/DISK1@2018-10-01_01-00 | "
                          "zfs recv data/dst/core/tsaukpaetra", shell=True)

    subprocess.check_call("zfs snapshot -r data/src@2018-10-01_02-00", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        replication-tasks:
          src:
            direction: push
            transport:
              type: local
            source-dataset:
              - data/src/internal/DISK1/Apps
              - data/src/internal/DISK1/ISO
            target-dataset: data/dst/core/tsaukpaetra
            recursive: false
            also-include-naming-schema:
              - "%Y-%m-%d_%H-%M"
            auto: false
            retention-policy: none
            retries: 1
    """))

    run_replication_test(definition)

    local_shell = LocalShell()
    assert len(list_snapshots(local_shell, "data/dst/core/tsaukpaetra/Apps", False)) == 2
    assert len(list_snapshots(local_shell, "data/dst/core/tsaukpaetra/ISO", False)) == 2
