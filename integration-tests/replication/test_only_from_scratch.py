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
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs receive -A tank/dst", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    subprocess.check_call("zfs create tank/src", shell=True)
    subprocess.check_call("zfs snapshot tank/src@2018-10-01_01-00", shell=True)

    if has_dst:
        subprocess.check_call("zfs create tank/dst", shell=True)
        if has_dst == 2:
            subprocess.check_call("zfs snapshot tank/dst@2018-10-01_01-00", shell=True)

    definition = yaml.safe_load(textwrap.dedent(f"""\
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
            retention-policy: none
            retries: 1
    """))
    if has_dst:
        error = run_replication_test(definition, success=False)
        assert error.error == "Target dataset 'tank/dst' already exists"
    else:
        run_replication_test(definition)

        local_shell = LocalShell()
        assert list_snapshots(local_shell, "tank/dst", False) == [
            Snapshot("tank/dst", "2018-10-01_01-00"),
        ]
