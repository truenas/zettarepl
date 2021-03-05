# -*- coding=utf-8 -*-
import subprocess
import textwrap

import yaml

from zettarepl.utils.test import run_replication_test


def test_nothing_to_replicate():
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs receive -A data/dst", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    subprocess.check_call("zfs create data/src", shell=True)
    subprocess.check_call("zfs snapshot data/src@manual-snap", shell=True)

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
    """))

    assert run_replication_test(definition, success=False).error == (
        "Dataset 'data/src' does not have any matching snapshots to replicate"
    )
