# -*- coding=utf-8 -*-
import subprocess
import textwrap

import yaml

from zettarepl.snapshot.list import list_snapshots
from zettarepl.transport.local import LocalShell
from zettarepl.utils.test import run_replication_test


def test_creates_intermediate_datasets():
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs receive -A data/deeply", shell=True)
    subprocess.call("zfs destroy -r data/deeply", shell=True)

    subprocess.check_call("zfs create -V 1M data/src", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2018-10-01_01-00", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2018-10-01_02-00", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        replication-tasks:
          src:
            direction: push
            transport:
              type: local
            source-dataset: data/src
            target-dataset: data/deeply/nested/dst
            recursive: true
            also-include-naming-schema:
              - "%Y-%m-%d_%H-%M"
            auto: false
            retention-policy: none
            retries: 1
    """))
    run_replication_test(definition)

    local_shell = LocalShell()
    assert len(list_snapshots(local_shell, "data/deeply/nested/dst", False)) == 2
