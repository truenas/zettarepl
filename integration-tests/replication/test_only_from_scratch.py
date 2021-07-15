# -*- coding=utf-8 -*-
import subprocess
import textwrap

import pytest
import yaml

from zettarepl.snapshot.list import list_snapshots
from zettarepl.snapshot.snapshot import Snapshot
from zettarepl.transport.local import LocalShell
from zettarepl.utils.test import run_replication_test


@pytest.mark.parametrize("has_dst", [0, 1, 2])
def test_only_from_scratch(has_dst):
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs receive -A data/dst", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    subprocess.check_call("zfs create data/src", shell=True)
    subprocess.check_call("zfs snapshot data/src@2018-10-01_01-00", shell=True)

    if has_dst:
        subprocess.check_call("zfs create data/dst", shell=True)
        if has_dst == 2:
            subprocess.check_call("zfs snapshot data/dst@2018-10-01_01-00", shell=True)

    definition = yaml.safe_load(textwrap.dedent(f"""\
        timezone: "Europe/Moscow"

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
            only-from-scratch: true
            retention-policy: none
            retries: 1
    """))
    if has_dst:
        error = run_replication_test(definition, success=False)
        assert error.error == "Target dataset 'data/dst' already exists"
    else:
        run_replication_test(definition)

        local_shell = LocalShell()
        assert list_snapshots(local_shell, "data/dst", False) == [
            Snapshot("data/dst", "2018-10-01_01-00"),
        ]
