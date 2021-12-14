# -*- coding=utf-8 -*-
import subprocess
import textwrap

import pytest
import yaml

from zettarepl.snapshot.list import list_snapshots
from zettarepl.transport.local import LocalShell
from zettarepl.utils.test import transports, run_replication_test


@pytest.mark.parametrize("transport", transports())
def test_pull_replication(transport):
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs receive -A data/dst", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    subprocess.check_call("zfs create data/src", shell=True)
    subprocess.check_call("zfs snapshot data/src@2018-10-01_01-00", shell=True)
    subprocess.check_call("zfs snapshot data/src@2018-10-01_02-00", shell=True)

    subprocess.check_call("zfs create data/dst", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        replication-tasks:
          src:
            direction: pull
            source-dataset: data/src
            target-dataset: data/dst
            recursive: true
            naming-schema:
              - "%Y-%m-%d_%H-%M"
            auto: false
            retention-policy: none
    """))
    definition["replication-tasks"]["src"]["transport"] = transport

    run_replication_test(definition)

    local_shell = LocalShell()
    assert len(list_snapshots(local_shell, "data/dst", False)) == 2
