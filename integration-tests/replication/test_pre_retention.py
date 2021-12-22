# -*- coding=utf-8 -*-
from datetime import datetime
import subprocess
import textwrap

import pytest
import yaml

from zettarepl.utils.test import create_dataset, run_replication_test


@pytest.mark.parametrize("direction", ["push", "pull"])
@pytest.mark.parametrize("recursive", [True, False])
@pytest.mark.parametrize("retention_policy", ["source", "custom"])
def test_pre_retention(direction, recursive, retention_policy):
    subprocess.call("zfs destroy -r data/src", shell=True)
    subprocess.call("zfs destroy -r data/dst", shell=True)

    create_dataset("data/src")
    create_dataset("data/src/a")
    create_dataset("data/src/b")
    subprocess.check_call("zfs set quota=200M data/src", shell=True)
    subprocess.check_call("dd if=/dev/urandom of=/mnt/data/src/a/blob bs=1K count=90000", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2018-10-01_01-00", shell=True)
    subprocess.check_call("dd if=/dev/urandom of=/mnt/data/src/a/blob bs=1K count=90000", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2018-10-01_02-00", shell=True)
    subprocess.check_call("zfs send -R data/src@2018-10-01_01-00 | zfs recv -s -F data/dst", shell=True)
    subprocess.check_call(
        "zfs send -R -i data/src@2018-10-01_01-00 data/src@2018-10-01_02-00 | zfs recv -s -F data/dst", shell=True,
    )
    subprocess.check_call("zfs destroy -r data/src@2018-10-01_01-00", shell=True)
    subprocess.check_call("dd if=/dev/urandom of=/mnt/data/src/a/blob bs=1K count=90000", shell=True)
    subprocess.check_call("zfs snapshot -r data/src@2018-10-01_03-00", shell=True)

    definition = yaml.safe_load(textwrap.dedent(f"""\
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
            transport:
              type: local
            target-dataset: data/dst
            properties: true
            auto: false
            retention-policy: source
            retries: 1
    """))

    if direction == "push":
        definition["replication-tasks"]["src"]["direction"] = "push"
        definition["replication-tasks"]["src"]["periodic-snapshot-tasks"] = ["src"]
    else:
        definition["replication-tasks"]["src"]["direction"] = "pull"
        definition["replication-tasks"]["src"]["naming-schema"] = "%Y-%m-%d_%H-%M"

    if recursive:
        definition["replication-tasks"]["src"]["source-dataset"] = "data/src"
        definition["replication-tasks"]["src"]["recursive"] = True
    else:
        definition["replication-tasks"]["src"]["source-dataset"] = ["data/src/a", "data/src/b"]
        definition["replication-tasks"]["src"]["recursive"] = False

    if retention_policy == "source":
        definition["replication-tasks"]["src"]["retention-policy"] = "source"
    else:
        definition["replication-tasks"]["src"]["retention-policy"] = "custom"
        definition["replication-tasks"]["src"]["lifetime"] = "PT1H"

    run_replication_test(definition, now=datetime(2018, 10, 2))
