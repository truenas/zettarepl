# -*- coding=utf-8 -*-
import subprocess
import textwrap

import pytest
import yaml

from zettarepl.utils.test import set_localhost_transport_options, create_dataset, run_replication_test


@pytest.mark.parametrize("compression", ["pigz", "plzip", "lz4", "xz"])
def test_push_replication(compression):
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    create_dataset("data/src")
    subprocess.check_call("zfs snapshot -r data/src@2018-10-01_01-00", shell=True)

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
            transport:
              type: ssh
              hostname: 127.0.0.1
            source-dataset: data/src
            target-dataset: data/dst
            recursive: true
            periodic-snapshot-tasks:
              - src
            auto: true
            retention-policy: none
            retries: 1
    """))
    set_localhost_transport_options(definition["replication-tasks"]["src"]["transport"])
    definition["replication-tasks"]["src"]["compression"] = compression

    run_replication_test(definition)
