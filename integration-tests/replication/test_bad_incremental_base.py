# -*- coding=utf-8 -*-
import subprocess
import textwrap

import pytest
import yaml

from zettarepl.utils.test import transports, create_dataset, run_replication_test


@pytest.mark.parametrize("transport", transports())
def test_bad_incremental_base(transport):
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    create_dataset("data/src")
    subprocess.check_call("zfs snapshot -r data/src@2018-10-01_01-00", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2018-10-01_02-00", shell=True)

    create_dataset("data/dst")
    subprocess.check_call("zfs snapshot -r data/dst@2018-10-01_01-00", shell=True)

    definition = yaml.safe_load(textwrap.dedent("""\
        timezone: "UTC"

        periodic-snapshot-tasks:
          src:
            dataset: data/src
            recursive: true
            lifetime: PT1H
            naming-schema: "%Y-%m-%d_%H-%M"
            schedule:
              minute: "0"

        replication-tasks:
          src:
            direction: push
            source-dataset: data/src
            target-dataset: data/dst
            recursive: true
            periodic-snapshot-tasks:
              - src
            auto: true
            retention-policy: none
            retries: 1
    """))
    definition["replication-tasks"]["src"]["transport"] = transport

    assert (
        "does not match incremental source" in run_replication_test(definition, success=False).error.replace("\n", " ")
    )
