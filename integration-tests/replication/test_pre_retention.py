# -*- coding=utf-8 -*-
from datetime import datetime
import subprocess
import textwrap

import pytest
import yaml

from zettarepl.snapshot.list import list_snapshots
from zettarepl.transport.local import LocalShell
from zettarepl.utils.test import create_dataset, run_replication_test


@pytest.mark.parametrize("direction", ["push", "pull"])
@pytest.mark.parametrize("recursive", [True, False])
@pytest.mark.parametrize("retention_policy", ["source", "custom"])
def test_pre_retention(direction, recursive, retention_policy):
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    create_dataset("tank/src")
    create_dataset("tank/src/a")
    create_dataset("tank/src/b")
    subprocess.check_call("zfs set quota=200M tank/src", shell=True)
    subprocess.check_call("dd if=/dev/urandom of=/mnt/tank/src/a/blob bs=1K count=90000", shell=True)
    subprocess.check_call("zfs snapshot -r tank/src@2018-10-01_01-00", shell=True)
    subprocess.check_call("dd if=/dev/urandom of=/mnt/tank/src/a/blob bs=1K count=90000", shell=True)
    subprocess.check_call("zfs snapshot -r tank/src@2018-10-01_02-00", shell=True)
    subprocess.check_call("zfs send -R tank/src@2018-10-01_01-00 | zfs recv -s -F tank/dst", shell=True)
    subprocess.check_call(
        "zfs send -R -i tank/src@2018-10-01_01-00 tank/src@2018-10-01_02-00 | zfs recv -s -F tank/dst", shell=True,
    )
    subprocess.check_call("zfs destroy -r tank/src@2018-10-01_01-00", shell=True)
    subprocess.check_call("dd if=/dev/urandom of=/mnt/tank/src/a/blob bs=1K count=90000", shell=True)
    subprocess.check_call("zfs snapshot -r tank/src@2018-10-01_03-00", shell=True)

    definition = yaml.safe_load(textwrap.dedent(f"""\
        timezone: "UTC"

        periodic-snapshot-tasks:
          src:
            dataset: tank/src
            recursive: true
            lifetime: PT1H
            naming-schema: "%Y-%m-%d_%H-%M"
            schedule:
              minute: "0"

        replication-tasks:
          src:
            transport:
              type: local
            target-dataset: tank/dst
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
        definition["replication-tasks"]["src"]["source-dataset"] = "tank/src"
        definition["replication-tasks"]["src"]["recursive"] = True
    else:
        definition["replication-tasks"]["src"]["source-dataset"] = ["tank/src/a", "tank/src/b"]
        definition["replication-tasks"]["src"]["recursive"] = False

    if retention_policy == "source":
        definition["replication-tasks"]["src"]["retention-policy"] = "source"
    else:
        definition["replication-tasks"]["src"]["retention-policy"] = "custom"
        definition["replication-tasks"]["src"]["lifetime"] = "PT1H"

    run_replication_test(definition, now=datetime(2018, 10, 2))


def test_pre_retention_multiple_source_datasets():
    subprocess.call("zfs destroy -r tank/src", shell=True)
    subprocess.call("zfs destroy -r tank/dst", shell=True)

    create_dataset("tank/src")
    create_dataset("tank/src/Family_Media")
    subprocess.check_call("zfs snapshot -r tank/src@2022-05-16_00-00", shell=True)
    subprocess.check_call("zfs snapshot -r tank/src@2022-05-17_00-00", shell=True)
    subprocess.check_call("zfs send -R tank/src@2022-05-17_00-00 | zfs recv -s -F tank/dst", shell=True)

    subprocess.check_call("zfs snapshot -r tank/src@2022-05-18_00-00", shell=True)

    definition = yaml.safe_load(textwrap.dedent(f"""\
        timezone: "UTC"

        replication-tasks:
          src:
            direction: push
            transport:
              type: local
            source-dataset: [tank/src, tank/src/Family_Media]
            target-dataset: tank/dst
            recursive: false
            properties: true
            also-include-naming-schema:
              - "%Y-%m-%d_%H-%M"
            auto: false
            retention-policy: source
            retries: 1
    """))

    run_replication_test(definition, now=datetime(2022, 5, 18))

    local_shell = LocalShell()
    assert len(list_snapshots(local_shell, "tank/dst/Family_Media", False)) == 3
